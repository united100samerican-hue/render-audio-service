import asyncio
import logging
import os
import subprocess
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from telethon import TelegramClient
from telethon.sessions import StringSession

try:
    from pytgcalls import GroupCallFactory
except Exception as exc:  # pragma: no cover
    raise RuntimeError("pytgcalls import failed") from exc

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("recording_service")

APP_NAME = "voice-recorder-service"
ROOT = Path(os.getenv("RECORDINGS_ROOT", "/tmp/voice-recorder")).resolve()
ROOT.mkdir(parents=True, exist_ok=True)
RECORDINGS_DIR = ROOT / "recordings"
RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
SILENCE_WAV = ROOT / "silence.wav"

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "180"))
MAX_RECORDING_MINUTES = int(os.getenv("MAX_RECORDING_MINUTES", "180"))

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
RECORDING_SECRET = os.getenv("RECORDING_SECRET", "").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "").strip()


def ensure_silence() -> None:
    if SILENCE_WAV.exists() and SILENCE_WAV.stat().st_size > 0:
        return
    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=48000:cl=mono",
            "-t",
            "1",
            "-c:a",
            "pcm_s16le",
            str(SILENCE_WAV),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@dataclass
class RecorderSession:
    chat_id: str
    deliver_to: str = ""
    status: str = "idle"
    started_at: float = 0.0
    output_path: Path = field(default_factory=Path)
    mode: str = "file"
    call: Any = None
    file_handle: Any = None
    title: str = ""
    started_by: str = ""
    group_title: str = ""
    stop_task: Optional[asyncio.Task] = None
    last_error: str = ""


class StartRequest(BaseModel):
    chat_id: str = Field(..., min_length=1)
    deliver_to: str = Field(..., min_length=1)
    started_by: str = ""
    group_title: str = ""
    title: str = ""


class StopRequest(BaseModel):
    chat_id: str = Field(..., min_length=1)
    deliver_to: str = ""
    group_title: str = ""
    stopped_by: str = ""
    caption: str = ""


class StatusRequest(BaseModel):
    chat_id: str = Field(..., min_length=1)


class RecorderManager:
    def __init__(self, client: TelegramClient) -> None:
        self.client = client
        self.factory = GroupCallFactory(
            self.client,
            mtproto_backend=GroupCallFactory.MTPROTO_CLIENT_TYPE.TELETHON,
        )
        self.sessions: dict[str, RecorderSession] = {}
        self.lock = asyncio.Lock()

    def _new_output_path(self, chat_id: str) -> Path:
        return RECORDINGS_DIR / f"recording{chat_id}{int(time.time())}.ogg"

    async def _upload_to_telegram(self, deliver_to: str, path: Path, caption: str) -> tuple[bool, str]:
        if not BOT_TOKEN:
            return False, "BOT_TOKEN missing"
        if not deliver_to:
            return False, "deliver_to missing"
        if not path.exists() or path.stat().st_size == 0:
            return False, "recording file missing"

        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            for endpoint, field_name in (("sendAudio", "audio"), ("sendDocument", "document")):
                try:
                    with path.open("rb") as fh:
                        files = {field_name: (path.name, fh, "application/octet-stream")}
                        data = {"chat_id": deliver_to, "caption": caption or ""}
                        if endpoint == "sendAudio":
                            data["supports_streaming"] = "true"
                        r = await client.post(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}",
                            data=data,
                            files=files,
                        )
                    try:
                        j = r.json()
                    except Exception:
                        j = None
                    if r.status_code < 400 and isinstance(j, dict) and j.get("ok"):
                        return True, endpoint
                    logger.error(
                        "telegram upload failed endpoint=%s status=%s body=%s",
                        endpoint,
                        r.status_code,
                        getattr(r, "text", ""),
                    )
                except Exception:
                    logger.exception("telegram upload exception endpoint=%s", endpoint)
        return False, "telegram_send_failed"

    async def _auto_stop_after(self, chat_id: str, minutes: int) -> None:
        try:
            await asyncio.sleep(max(1, minutes) * 60)
            await self.stop(chat_id, auto=True)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("auto_stop_failed chat_id=%s", chat_id)

    async def _start_file_recorder(self, session: RecorderSession) -> None:
        ensure_silence()
        call = self.factory.get_file_group_call(
            input_filename=str(SILENCE_WAV),
            output_filename=str(session.output_path),
            play_on_repeat=True,
        )
        await call.start(int(session.chat_id), enable_action=False)
        session.call = call
        session.mode = "file"

    async def _start_raw_recorder(self, session: RecorderSession) -> None:
        buffer = session.output_path.open("wb")

        def on_recorded_data(*args):
            chunk = None
            for item in args:
                if isinstance(item, (bytes, bytearray)):
                    chunk = bytes(item)
                    break
            if chunk:
                buffer.write(chunk)

        call = self.factory.get_raw_group_call(on_recorded_data=on_recorded_data)
        await call.start(int(session.chat_id), enable_action=False)
        session.call = call
        session.file_handle = buffer
        session.mode = "raw"

    async def start(
        self,
        chat_id: str,
        deliver_to: str,
        started_by: str = "",
        group_title: str = "",
        title: str = "",
    ) -> dict[str, Any]:
        async with self.lock:
            current = self.sessions.get(chat_id)
            if current and current.status == "recording":
                return {
                    "ok": True,
                    "recording": True,
                    "mode": current.mode,
                    "file_name": current.output_path.name,
                }

            session = RecorderSession(
                chat_id=chat_id,
                deliver_to=deliver_to,
                status="recording",
                started_at=time.time(),
                output_path=self._new_output_path(chat_id),
                started_by=started_by,
                group_title=group_title,
                title=title,
            )
            self.sessions[chat_id] = session
            try:
                try:
                    await self._start_file_recorder(session)
                except Exception as exc:
                    session.last_error = str(exc)
                    logger.exception("file recorder failed, switching to raw mode chat_id=%s", chat_id)
                    await self._start_raw_recorder(session)
                session.stop_task = asyncio.create_task(self._auto_stop_after(chat_id, MAX_RECORDING_MINUTES))
                return {
                    "ok": True,
                    "recording": True,
                    "mode": session.mode,
                    "file_name": session.output_path.name,
                }
            except Exception as exc:
                session.status = "error"
                session.last_error = str(exc)
                logger.exception("start failed chat_id=%s", chat_id)
                self.sessions.pop(chat_id, None)
                try:
                    if session.file_handle:
                        session.file_handle.close()
                except Exception:
                    pass
                raise

    async def stop(
        self,
        chat_id: str,
        auto: bool = False,
        deliver_to: str = "",
        stopped_by: str = "",
        group_title: str = "",
        caption: str = "",
    ) -> dict[str, Any]:
        async with self.lock:
            session = self.sessions.get(chat_id)
            if not session:
                return {"ok": False, "error": "not_recording"}

            if session.stop_task and not session.stop_task.done():
                session.stop_task.cancel()

            try:
                if session.call:
                    stop = getattr(session.call, "stop", None)
                    if callable(stop):
                        maybe = stop()
                        if asyncio.iscoroutine(maybe):
                            await maybe
            except Exception as exc:
                session.last_error = str(exc)
                logger.exception("stop call failed chat_id=%s", chat_id)
            finally:
                try:
                    if session.file_handle:
                        session.file_handle.flush()
                        session.file_handle.close()
                except Exception:
                    logger.exception("file handle close failed chat_id=%s", chat_id)

            session.status = "stopped"
            target = (deliver_to or session.deliver_to or "").strip()
            if not target:
                logger.error("missing deliver_to on stop chat_id=%s", chat_id)
                self.sessions.pop(chat_id, None)
                return {
                    "ok": False,
                    "error": "deliver_to_missing",
                    "file_name": session.output_path.name,
                    "mode": session.mode,
                }

            meta = [
                x
                for x in [
                    session.group_title,
                    group_title,
                    caption,
                    f"by:{stopped_by}" if stopped_by else "",
                ]
                if x
            ]
            caption_text = " | ".join(meta).strip() or "voice chat recording"
            sent, result = await self._upload_to_telegram(target, session.output_path, caption_text)
            size = session.output_path.stat().st_size if session.output_path.exists() else 0
            data = {
                "ok": True,
                "recording": False,
                "file_name": session.output_path.name,
                "sent": sent,
                "send_result": result,
                "mode": session.mode,
                "size": size,
                "last_error": session.last_error,
            }
            try:
                if sent:
                    session.output_path.unlink(missing_ok=True)
            except Exception:
                logger.exception("cleanup failed chat_id=%s", chat_id)
            self.sessions.pop(chat_id, None)
            return data

    async def status(self, chat_id: str) -> dict[str, Any]:
        async with self.lock:
            session = self.sessions.get(chat_id)
            if not session:
                return {"ok": True, "recording": False}
            return {
                "ok": True,
                "recording": session.status == "recording",
                "mode": session.mode,
                "file_name": session.output_path.name,
                "started_at": session.started_at,
                "last_error": session.last_error,
                "deliver_to": session.deliver_to,
            }


client: Optional[TelegramClient] = None
manager: Optional[RecorderManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, manager
    if not SESSION_STRING or not API_ID or not API_HASH:
        raise RuntimeError("Missing SESSION_STRING / API_ID / API_HASH")
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()
    manager = RecorderManager(client)
    try:
        yield
    finally:
        try:
            if manager:
                for session in list(manager.sessions.values()):
                    if session.stop_task and not session.stop_task.done():
                        session.stop_task.cancel()
                    try:
                        if session.call:
                            stop = getattr(session.call, "stop", None)
                            if callable(stop):
                                maybe = stop()
                                if asyncio.iscoroutine(maybe):
                                    await maybe
                    except Exception:
                        logger.exception("lifespan stop failed chat_id=%s", session.chat_id)
                    try:
                        if session.file_handle:
                            session.file_handle.close()
                    except Exception:
                        logger.exception("lifespan file close failed chat_id=%s", session.chat_id)
        finally:
            if client:
                await client.disconnect()


app = FastAPI(title=APP_NAME, lifespan=lifespan)


def _check_secret(request: Request) -> None:
    if not RECORDING_SECRET:
        return
    secret = request.headers.get("x-recording-secret", "") or request.headers.get("x-keepalive-secret", "")
    if secret != RECORDING_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")


@app.get("/health")
async def health():
    return {"ok": True, "service": APP_NAME}


@app.post("/record/start")
async def record_start(payload: StartRequest, request: Request):
    _check_secret(request)
    if manager is None:
        raise HTTPException(status_code=503, detail="service_not_ready")
    try:
        return await manager.start(
            payload.chat_id,
            payload.deliver_to,
            payload.started_by,
            payload.group_title,
            payload.title,
        )
    except Exception as exc:
        logger.exception("record_start error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/record/stop")
async def record_stop(payload: StopRequest, request: Request):
    _check_secret(request)
    if manager is None:
        raise HTTPException(status_code=503, detail="service_not_ready")
    try:
        return await manager.stop(
            payload.chat_id,
            deliver_to=payload.deliver_to,
            stopped_by=payload.stopped_by,
            group_title=payload.group_title,
            caption=payload.caption,
        )
    except Exception as exc:
        logger.exception("record_stop error")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/record/status")
async def record_status(payload: StatusRequest, request: Request):
    _check_secret(request)
    if manager is None:
        raise HTTPException(status_code=503, detail="service_not_ready")
    return await manager.status(payload.chat_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("recording_service:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)