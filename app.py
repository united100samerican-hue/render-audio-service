import asyncio
import os

from fastapi import FastAPI, Header, HTTPException, Request, Body
from telethon import TelegramClient
from telethon.sessions import StringSession

from player import VoicePlayer
from tiktok_service import TikTokService
import recording_service as rec

app = FastAPI(title="Render Audio Service", version="2.1")

player = VoicePlayer()
tiktok_service = TikTokService()

SECRET = os.getenv("KEEPALIVE_SECRET", "").strip()

def guard(v: str | None):
    if SECRET and (v or "").strip() != SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

async def _body(req: Request):
    try:
        return await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")

def _record_expected_secrets() -> set[str]:
    return {s for s in [getattr(rec, "RECORDING_SECRET", ""), SECRET] if str(s).strip()}

def _record_guard(x_recording_secret: str | None = None, x_keepalive_secret: str | None = None):
    provided = (x_recording_secret or x_keepalive_secret or "").strip()
    expected = _record_expected_secrets()
    if expected and provided not in expected:
        raise HTTPException(status_code=403, detail="forbidden")

async def _init_recording():
    if rec.manager is not None:
        return
    if not getattr(rec, "SESSION_STRING", "").strip() or not getattr(rec, "API_ID", 0) or not getattr(rec, "API_HASH", "").strip():
        return
    rec.client = TelegramClient(StringSession(rec.SESSION_STRING), int(rec.API_ID), rec.API_HASH)
    await rec.client.start()
    rec.manager = rec.RecorderManager(rec.client)

async def _shutdown_recording():
    try:
        if rec.manager is not None:
            for session in list(rec.manager.sessions.values()):
                try:
                    if session.stop_task and not session.stop_task.done():
                        session.stop_task.cancel()
                except Exception:
                    pass
                try:
                    if session.call:
                        stop = getattr(session.call, "stop", None)
                        if callable(stop):
                            maybe = stop()
                            if asyncio.iscoroutine(maybe):
                                await maybe
                except Exception:
                    pass
                try:
                    if session.file_handle:
                        session.file_handle.close()
                except Exception:
                    pass
    finally:
        try:
            if rec.client is not None:
                await rec.client.disconnect()
        except Exception:
            pass
        rec.manager = None
        rec.client = None

@app.on_event("startup")
async def _startup():
    await _init_recording()

@app.on_event("shutdown")
async def _shutdown():
    await _shutdown_recording()

@app.get("/")
async def root():
    return {"ok": True, "service": "render-audio-service"}

@app.get("/ping")
async def ping(x_keepalive_secret: str | None = Header(default=None)):
    guard(x_keepalive_secret)
    return {"ok": True}

@app.get("/healthz")
async def healthz():
    return {"ok": True, "ready": True}

@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "render-audio-service",
        "recording_ready": rec.manager is not None,
        "recording_app": getattr(rec, "APP_NAME", "voice-recorder-service"),
    }

@app.post("/meta")
async def meta(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.meta(
            body["chatId"],
            body["source_type"],
            body["source_id"],
            body.get("title", ""),
            body.get("duration", 0),
        )
        return {"ok": True, "action": "meta", "state": state.get("state", {})}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/start")
async def start(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.start(
            body["chatId"],
            body["source_type"],
            body["source_id"],
            body.get("title", ""),
            body.get("duration", 0),
        )
        return {"ok": True, "action": "start", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/pause")
async def pause(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.pause(body["chatId"])
        return {"ok": True, "action": "pause", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/resume")
async def resume(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.resume(body["chatId"])
        return {"ok": True, "action": "resume", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/stop")
async def stop(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.stop(body["chatId"])
        return {"ok": True, "action": "stop", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/seek")
async def seek(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.seek(body["chatId"], body.get("delta", 0))
        return {"ok": True, "action": "seek", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/record/start")
async def record_start(
    payload: rec.StartRequest,
    x_recording_secret: str | None = Header(default=None),
    x_keepalive_secret: str | None = Header(default=None),
):
    _record_guard(x_recording_secret, x_keepalive_secret)
    if rec.manager is None:
        await _init_recording()
    if rec.manager is None:
        raise HTTPException(status_code=503, detail="service_not_ready")
    try:
        return await rec.manager.start(payload.chat_id, payload.started_by, payload.group_title, payload.title)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

@app.post("/record/stop")
async def record_stop(
    payload: rec.StopRequest,
    x_recording_secret: str | None = Header(default=None),
    x_keepalive_secret: str | None = Header(default=None),
):
    _record_guard(x_recording_secret, x_keepalive_secret)
    if rec.manager is None:
        await _init_recording()
    if rec.manager is None:
        raise HTTPException(status_code=503, detail="service_not_ready")
    try:
        return await rec.manager.stop(
            payload.chat_id,
            stopped_by=payload.stopped_by,
            group_title=payload.group_title,
            caption=payload.caption,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

@app.post("/record/status")
async def record_status(
    payload: rec.StatusRequest,
    x_recording_secret: str | None = Header(default=None),
    x_keepalive_secret: str | None = Header(default=None),
):
    _record_guard(x_recording_secret, x_keepalive_secret)
    if rec.manager is None:
        await _init_recording()
    if rec.manager is None:
        raise HTTPException(status_code=503, detail="service_not_ready")
    return await rec.manager.status(payload.chat_id)

# ===================== TikTok Routes (مستقلة) =====================

@app.post("/tiktok/start")
async def tiktok_start(body: dict = Body(...)):
    chat_id = int(body.get("chatId"))
    url = body.get("source_url")
    video = body.get("video", True)
    result = await tiktok_service.start(chat_id, url, video)
    return result

@app.post("/tiktok/stop")
async def tiktok_stop(body: dict = Body(...)):
    chat_id = int(body.get("chatId"))
    result = await tiktok_service.stop(chat_id)
    return result

@app.post("/tiktok/state")
async def tiktok_state(body: dict = Body(...)):
    chat_id = int(body.get("chatId"))
    result = await tiktok_service.get_state(chat_id)
    return {"ok": True, "state": result}
# ================================================================