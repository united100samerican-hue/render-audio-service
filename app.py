from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import Body, FastAPI, Header, HTTPException, Request

from player import VoicePlayer
from tiktok_service import TikTokService
import recording_service as rec

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("app")

app = FastAPI(title="Render Audio Service", version="2.3")

player = VoicePlayer()
tiktok_service = TikTokService()
SECRET = os.getenv("KEEPALIVE_SECRET", "").strip()


def _guard(secret: str | None) -> None:
    if SECRET and (secret or "").strip() != SECRET:
        raise HTTPException(status_code=403, detail="forbidden")


async def _json(req: Request) -> dict[str, Any]:
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="invalid_json")
    return body


def _record_secrets() -> set[str]:
    values: set[str] = set()
    if SECRET:
        values.add(SECRET)
    secret = str(getattr(rec, "RECORDING_SECRET", "") or "").strip()
    if secret:
        values.add(secret)
    return values


def _record_guard(
    x_recording_secret: str | None = None,
    x_keepalive_secret: str | None = None,
) -> None:
    expected = _record_secrets()
    if not expected:
        return
    provided = (x_recording_secret or x_keepalive_secret or "").strip()
    if provided not in expected:
        raise HTTPException(status_code=403, detail="forbidden")


def _field_names(model_cls: Any) -> set[str]:
    fields = getattr(model_cls, "model_fields", None)
    if isinstance(fields, dict) and fields:
        return set(fields.keys())
    ann = getattr(model_cls, "__annotations__", None)
    if isinstance(ann, dict) and ann:
        return set(ann.keys())
    return set()


def _build_model(model_cls: Any, data: dict[str, Any]) -> Any:
    allowed = _field_names(model_cls)
    if allowed:
        data = {k: v for k, v in data.items() if k in allowed}
    return model_cls(**data)


def _record_env_snapshot() -> dict[str, Any]:
    return {
        "SESSION_STRING": bool(getattr(rec, "SESSION_STRING", "")),
        "API_ID": bool(getattr(rec, "API_ID", 0)),
        "API_HASH": bool(getattr(rec, "API_HASH", "")),
        "BOT_TOKEN": bool(getattr(rec, "BOT_TOKEN", "")),
        "RECORDING_SECRET": bool(getattr(rec, "RECORDING_SECRET", "")),
        "backend_available": bool(getattr(rec, "RECORDING_BACKEND_AVAILABLE", False)),
        "backend_error": str(getattr(rec, "RECORDING_BACKEND_ERROR", "") or ""),
    }


async def _get_recording_manager():
    manager = getattr(rec, "manager", None)
    if manager is not None:
        return manager

    ensure = getattr(rec, "ensure_manager", None)
    if callable(ensure):
        try:
            manager = await ensure()
        except Exception:
            logger.exception("recording ensure_manager failed")
            manager = None
        if manager is not None:
            return manager

    return getattr(rec, "manager", None)


async def _init_recording() -> None:
    await _get_recording_manager()


@app.on_event("startup")
async def _startup() -> None:
    await _init_recording()


@app.get("/")
async def root():
    return {"ok": True, "service": "render-audio-service"}


@app.get("/ping")
async def ping(x_keepalive_secret: str | None = Header(default=None)):
    _guard(x_keepalive_secret)
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "ready": True}


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": "render-audio-service",
        "recording_ready": getattr(rec, "manager", None) is not None,
        "recording_backend": bool(getattr(rec, "RECORDING_BACKEND_AVAILABLE", False)),
        "recording_import_error": str(getattr(rec, "RECORDING_BACKEND_ERROR", "")),
    }


@app.post("/meta")
async def meta(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    _guard(x_keepalive_secret)
    body = await _json(req)
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
    _guard(x_keepalive_secret)
    body = await _json(req)
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
    _guard(x_keepalive_secret)
    body = await _json(req)
    try:
        state = await player.pause(body["chatId"])
        return {"ok": True, "action": "pause", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/resume")
async def resume(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    _guard(x_keepalive_secret)
    body = await _json(req)
    try:
        state = await player.resume(body["chatId"])
        return {"ok": True, "action": "resume", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/stop")
async def stop(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    _guard(x_keepalive_secret)
    body = await _json(req)
    try:
        state = await player.stop(body["chatId"])
        return {"ok": True, "action": "stop", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/seek")
async def seek(req: Request, x_keepalive_secret: str | None = Header(default=None)):
    _guard(x_keepalive_secret)
    body = await _json(req)
    try:
        state = await player.seek(body["chatId"], body.get("delta", 0))
        return {"ok": True, "action": "seek", "state": state}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/record/start")
async def record_start(
    req: Request,
    x_recording_secret: str | None = Header(default=None),
    x_keepalive_secret: str | None = Header(default=None),
):
    _record_guard(x_recording_secret, x_keepalive_secret)
    body = await _json(req)

    payload = _build_model(
        rec.StartRequest,
        {
            "chat_id": str(body.get("chat_id", body.get("chatId", ""))).strip(),
            "deliver_to": str(body.get("deliver_to", body.get("deliverTo", ""))).strip(),
            "started_by": str(body.get("started_by", body.get("startedBy", ""))).strip(),
            "group_title": str(body.get("group_title", body.get("groupTitle", ""))).strip(),
            "title": str(body.get("title", "")).strip(),
        },
    )

    chat_id = str(getattr(payload, "chat_id", "")).strip()
    deliver_to = str(getattr(payload, "deliver_to", "")).strip() or str(body.get("deliver_to", body.get("deliverTo", ""))).strip()
    started_by = str(getattr(payload, "started_by", "")).strip()
    group_title = str(getattr(payload, "group_title", "")).strip()
    title = str(getattr(payload, "title", "")).strip()

    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id_required")
    if not deliver_to:
        raise HTTPException(status_code=400, detail="deliver_to_required")

    logger.info("[REC][ENV]", extra={"env": _record_env_snapshot()})

    manager = await _get_recording_manager()
    if manager is None:
        detail = str(getattr(rec, "RECORDING_BACKEND_ERROR", "") or "missing_env")
        raise HTTPException(status_code=503, detail=f"service_not_ready: {detail}")

    try:
        return await manager.start(chat_id, deliver_to, started_by, group_title, title)
    except Exception as exc:
        logger.exception("record_start_failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/record/stop")
async def record_stop(
    req: Request,
    x_recording_secret: str | None = Header(default=None),
    x_keepalive_secret: str | None = Header(default=None),
):
    _record_guard(x_recording_secret, x_keepalive_secret)
    body = await _json(req)

    payload = _build_model(
        rec.StopRequest,
        {
            "chat_id": str(body.get("chat_id", body.get("chatId", ""))).strip(),
            "deliver_to": str(body.get("deliver_to", body.get("deliverTo", ""))).strip(),
            "stopped_by": str(body.get("stopped_by", body.get("stoppedBy", ""))).strip(),
            "group_title": str(body.get("group_title", body.get("groupTitle", ""))).strip(),
            "caption": str(body.get("caption", "")).strip(),
            "auto": bool(body.get("auto", False)),
        },
    )

    chat_id = str(getattr(payload, "chat_id", "")).strip()
    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id_required")

    manager = await _get_recording_manager()
    if manager is None:
        detail = str(getattr(rec, "RECORDING_BACKEND_ERROR", "") or "missing_env")
        raise HTTPException(status_code=503, detail=f"service_not_ready: {detail}")

    try:
        return await manager.stop(
            chat_id,
            auto=bool(getattr(payload, "auto", False)),
            deliver_to=str(getattr(payload, "deliver_to", "")).strip(),
            stopped_by=str(getattr(payload, "stopped_by", "")).strip(),
            group_title=str(getattr(payload, "group_title", "")).strip(),
            caption=str(getattr(payload, "caption", "")).strip(),
        )
    except Exception as exc:
        logger.exception("record_stop_failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/record/status")
async def record_status(
    req: Request,
    x_recording_secret: str | None = Header(default=None),
    x_keepalive_secret: str | None = Header(default=None),
):
    _record_guard(x_recording_secret, x_keepalive_secret)
    body = await _json(req)

    chat_id = str(body.get("chat_id", body.get("chatId", ""))).strip()
    if not chat_id:
        raise HTTPException(status_code=400, detail="chat_id_required")

    manager = await _get_recording_manager()
    if manager is None:
        return {
            "ok": True,
            "recording": False,
            "service_ready": False,
            "reason": str(getattr(rec, "RECORDING_BACKEND_ERROR", "") or "missing_env"),
        }

    return await manager.status(chat_id)


@app.post("/tiktok/start")
async def tiktok_start(body: dict = Body(...)):
    return await tiktok_service.start(int(body.get("chatId")), body.get("source_url"), body.get("video", True))


@app.post("/tiktok/stop")
async def tiktok_stop(body: dict = Body(...)):
    return await tiktok_service.stop(int(body.get("chatId")))


@app.post("/tiktok/state")
async def tiktok_state(body: dict = Body(...)):
    state = await tiktok_service.get_state(int(body.get("chatId")))
    return {"ok": True, "state": state}


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)