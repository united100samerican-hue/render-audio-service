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

logger = logging.getLogger("recording_service")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())

APP_NAME = "voice-recorder-service"
ROOT = Path("/tmp/voice-recorder")
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

RECORDING_BACKEND_AVAILABLE = True
RECORDING_BACKEND_ERROR = ""

try:
    from pytgcalls.group_call_factory import GroupCallFactory
    from pytgcalls.mtproto_client_type import MTProtoClientType
except Exception as exc:
    GroupCallFactory = None
    MTProtoClientType = None
    RECORDING_BACKEND_AVAILABLE = False
    RECORDING_BACKEND_ERROR = f"{type(exc).__name__}: {exc}"
    logger.warning("pytgcalls import failed: %s", RECORDING_BACKEND_ERROR)

client: Optional[TelegramClient] = None
manager: Optional["RecorderManager"] = None
_boot_lock = asyncio.Lock()


def _ensure_silence() -> None:
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
    started_by: str = ""
    group_title: str = ""
    title: str = ""


class StopRequest(BaseModel):
    chat_id: str = Field(..., min_length=1)
    group_title: str = ""
    stopped_by: str = ""
    caption: str = ""
    auto: bool = False


class StatusRequest(BaseModel):
    chat_id: str = Field(..., min_length=1)


class RecorderManager:
    def __init__(self, client: TelegramClient) -> None:
        if GroupCallFactory is None or MTProtoClientType is None:
            raise RuntimeError(RECORDING_BACKEND_ERROR or "pytgcalls unavailable")
        self.client = client
        self.factory = GroupCallFactory(
            self.client,
            mtproto_backend=MTProtoClientType.TELETHON,
        )
        self.sessions: dict[str, RecorderSession] = {}
        self.lock = asyncio.Lock()

    def _new_output_path(self, chat_id: str) -> Path:
        return RECORDINGS_DIR / f"recording_{chat_id}_{int(time.time())}.ogg"

    async def _upload_to_telegram(self, chat_id: str, path: Path, caption: str) -> tuple[bool, str]:
        if not BOT_TOKEN:
            return False, "BOT_TOKEN missing"
        if not path.exists() or path.stat().st_size == 0:
            return False, "recording file missing"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client_http:
            for endpoint, field_name in (("sendAudio", "audio"), ("sendDocument", "document")):
                with path.open("rb") as fh:
                    files = {field_name: (path.name, fh, "application/octet-stream")}
                    data = {"chat_id": chat_id, "caption": caption or ""}
                    if endpoint == "sendAudio":
                        data["supports_streaming"] = "true"
                    response = await client_http.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/{endpoint}",
                        data=data,
                        files=files,
                    )
                try:
                    payload = response.json()
                except Exception:
                    payload = None
                if response.status_code < 400 and isinstance(payload, dict) and payload.get("ok"):
                    return True, endpoint
        return False, "telegram_send_failed"

    async def _auto_stop_after(self, chat_id: str, minutes: int) -> None:
        try:
            await asyncio.sleep(max(1, minutes) * 60)
            await self.stop(chat_id, auto=True)
        except asyncio.CancelledError:
            return
        except Exception:
            return

    async def _start_file_recorder(self, session: RecorderSession) -> None:
        _ensure_silence()
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
        def on_recorded_data(*args: Any) -> None:
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

    async def start(self, chat_id: str, started_by: str = "", group_title: str = "", title: str = "") -> dict[str, Any]:
        async with self.lock:
            current = self.sessions.get(chat_id)
            if current and current.status == "recording":
                return {"ok": True, "recording": True, "mode": current.mode, "file_name": current.output_path.name}
            session = RecorderSession(
                chat_id=chat_id,
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
                    await self._start_raw_recorder(session)
                session.stop_task = asyncio.create_task(self._auto_stop_after(chat_id, MAX_RECORDING_MINUTES))
                return {"ok": True, "recording": True, "mode": session.mode, "file_name": session.output_path.name}
            except Exception as exc:
                session.status = "error"
                session.last_error = str(exc)
                self.sessions.pop(chat_id, None)
                try:
                    if session.file_handle:
                        session.file_handle.close()
                except Exception:
                    pass
                raise

    async def stop(self, chat_id: str, auto: bool = False, stopped_by: str = "", group_title: str = "", caption: str = "") -> dict[str, Any]:
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
            finally:
                try:
                    if session.file_handle:
                        session.file_handle.flush()
                        session.file_handle.close()
                except Exception:
                    pass
            session.status = "stopped"
            meta = [x for x in [session.group_title, caption, f"by:{stopped_by}" if stopped_by else ""] if x]
            caption_text = " | ".join(meta).strip() or "voice chat recording"
            sent, result = await self._upload_to_telegram(chat_id, session.output_path, caption_text)
            data = {
                "ok": True,
                "recording": False,
                "file_name": session.output_path.name,
                "sent": sent,
                "send_result": result,
                "mode": session.mode,
                "size": session.output_path.stat().st_size if session.output_path.exists() else 0,
                "auto": auto,
            }
            try:
                if sent:
                    session.output_path.unlink(missing_ok=True)
            except Exception:
                pass
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
            }

    async def shutdown(self) -> None:
        async with self.lock:
            sessions = list(self.sessions.values())
            self.sessions.clear()
        for session in sessions:
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


async def ensure_manager() -> Optional[RecorderManager]:
    global client, manager
    async with _boot_lock:
        if manager is not None:
            return manager
        if not RECORDING_BACKEND_AVAILABLE:
            logger.warning("recording backend unavailable: %s", RECORDING_BACKEND_ERROR)
            return None
        if not SESSION_STRING or not API_ID or not API_HASH:
            logger.warning("recording env missing")
            return None
        try:
            client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
            await client.start()
            manager = RecorderManager(client)
            return manager
        except Exception as exc:
            logger.warning("recording manager init failed: %s", exc)
            try:
                if client is not None:
                    await client.disconnect()
            except Exception:
                pass
            client = None
            manager = None
            return None


def _check_secret(request: Request) -> None:
    if not RECORDING_SECRET:
        return
    if request.headers.get("x-recording-secret", "") != RECORDING_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await ensure_manager()
        yield
    finally:
        global client, manager
        try:
            if manager is not None:
                await manager.shutdown()
        finally:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            manager = None
            client = None


app = FastAPI(title=APP_NAME, lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": APP_NAME,
        "recording_backend": RECORDING_BACKEND_AVAILABLE,
        "recording_ready": manager is not None,
    }


@app.post("/record/start")
async def record_start(payload: StartRequest, request: Request):
    _check_secret(request)
    current = await ensure_manager()
    if current is None:
        raise HTTPException(status_code=503, detail=f"service_not_ready: {RECORDING_BACKEND_ERROR or 'missing_env'}")
    try:
        return await current.start(payload.chat_id, payload.started_by, payload.group_title, payload.title)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/record/stop")
async def record_stop(payload: StopRequest, request: Request):
    _check_secret(request)
    current = await ensure_manager()
    if current is None:
        raise HTTPException(status_code=503, detail=f"service_not_ready: {RECORDING_BACKEND_ERROR or 'missing_env'}")
    try:
        return await current.stop(
            payload.chat_id,
            auto=payload.auto,
            stopped_by=payload.stopped_by,
            group_title=payload.group_title,
            caption=payload.caption,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/record/status")
async def record_status(payload: StatusRequest, request: Request):
    _check_secret(request)
    current = await ensure_manager()
    if current is None:
        return {
            "ok": True,
            "recording": False,
            "service_ready": False,
            "reason": RECORDING_BACKEND_ERROR or "missing_env",
        }
    return await current.status(payload.chat_id)


if __name__ == "__main__":  # pragma: no cover
    import uvicorn
    uvicorn.run("recording_service:app", host="0.0.0.0", port=int(os.getenv("PORT", "10000")), reload=False)