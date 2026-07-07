import asyncio
import logging
import os
from typing import Any

from fastapi import Body, FastAPI, Header, HTTPException, Request

from player import VoicePlayer
from tiktok_service import TikTokService

logger = logging.getLogger("render-audio-service")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())

app = FastAPI(title="Render Audio Service", version="2.1")

player = VoicePlayer()
tiktok_service = TikTokService()

SECRET = os.getenv("KEEPALIVE_SECRET", "").strip()
RECORDING_SECRET = os.getenv("RECORDING_SECRET", "").strip()

try:
    import recording_service as rec
except Exception as exc:  # pragma: no cover
    rec = None
    logger.warning("recording_service import failed: %s", exc)


def guard(expected: str, provided: str | None) -> None:
    if expected and (provided or "").strip() != expected:
        raise HTTPException(status_code=403, detail="forbidden")


async def _body(req: Request) -> dict[str, Any]:
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="invalid_json")
    return body


def _chat_id(body: dict[str, Any]) -> str:
    value = body.get("chatId", body.get("chat_id", ""))
    return str(value).strip()


async def _record_manager():
    if rec is None:
        raise HTTPException(status_code=503, detail="recording_service_unavailable")
    await rec.ensure_manager()
    if rec.manager is None:
        reason = rec.RECORDING_BACKEND_ERROR or "missing_env"
        raise HTTPException(status_code=503, detail=f"service_not_ready: {reason}")
    return rec.manager


@app.get("/")
async def root():
    return {"ok": True, "service": "render-audio-service"}


@app.get("/ping")
async def ping(x_keepalive_secret: str | None = Header(default=None)):
    guard(SECRET, x_keepalive_secret)
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "ready": True}


@app.post("/meta")
async def meta(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(SECRET, x_keepalive_secret)
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
    guard(SECRET, x_keepalive_secret)
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
    guard(SECRET, x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.pause(body["chatId"])
        return {"ok": True, "action": "pause", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/resume")
async def resume(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(SECRET, x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.resume(body["chatId"])
        return {"ok": True, "action": "resume", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/stop")
async def stop(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(SECRET, x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.stop(body["chatId"])
        return {"ok": True, "action": "stop", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/seek")
async def seek(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    guard(SECRET, x_keepalive_secret)
    body = await _body(req)
    try:
        state = await player.seek(body["chatId"], body.get("delta", 0))
        return {"ok": True, "action": "seek", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/record/start")
async def record_start(req: Request, x_recording_secret: str | None = Header(default=None)):
    guard(RECORDING_SECRET, x_recording_secret)
    body = await _body(req)
    manager = await _record_manager()
    return await manager.start(
        _chat_id(body),
        str(body.get("started_by", body.get("startedBy", ""))),
        str(body.get("group_title", body.get("groupTitle", ""))),
        str(body.get("title", "")),
    )


@app.post("/record/stop")
async def record_stop(req: Request, x_recording_secret: str | None = Header(default=None)):
    guard(RECORDING_SECRET, x_recording_secret)
    body = await _body(req)
    manager = await _record_manager()
    return await manager.stop(
        _chat_id(body),
        auto=bool(body.get("auto", False)),
        stopped_by=str(body.get("stopped_by", body.get("stoppedBy", ""))),
        group_title=str(body.get("group_title", body.get("groupTitle", ""))),
        caption=str(body.get("caption", "")),
    )


@app.post("/record/status")
async def record_status(req: Request, x_recording_secret: str | None = Header(default=None)):
    guard(RECORDING_SECRET, x_recording_secret)
    body = await _body(req)
    manager = await _record_manager()
    return await manager.status(_chat_id(body))


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


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)