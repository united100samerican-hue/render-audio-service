import os
from fastapi import FastAPI, Header, HTTPException, Request
from player import VoicePlayer
from tiktok_service import TikTokService

app = FastAPI(title="Render Audio Service", version="2.0")
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