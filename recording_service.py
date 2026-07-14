from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx
from pydantic import BaseModel, Field
from telethon import TelegramClient
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("recording_service")

APP_NAME = "recording-service"
ROOT = Path(os.getenv("RECORDINGS_ROOT", "/tmp/recording-service")).resolve()
ROOT.mkdir(parents=True, exist_ok=True)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
RECORDING_SECRET = os.getenv("RECORDING_SECRET", "").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "").strip()

HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "180"))
MAX_RECORDING_MINUTES = int(os.getenv("MAX_RECORDING_MINUTES", "180"))
OUTPUT_SAMPLE_RATE = int(os.getenv("OUTPUT_SAMPLE_RATE", "48000"))
OUTPUT_CHANNELS = int(os.getenv("OUTPUT_CHANNELS", "1"))

RECORDING_BACKEND_AVAILABLE = False
RECORDING_BACKEND_ERROR = ""

_manager_lock: Optional[asyncio.Lock] = None
client: Optional[TelegramClient] = None
manager: Optional["RecorderManager"] = None


def _get_lock() -> asyncio.Lock:
    global _manager_lock
    if _manager_lock is None:
        _manager_lock = asyncio.Lock()
    return _manager_lock


def env_check() -> dict[str, Any]:
    return {
        "SESSION_STRING": bool(SESSION_STRING),
        "API_ID": bool(API_ID),
        "API_HASH": bool(API_HASH),
        "BOT_TOKEN": bool(BOT_TOKEN),
        "RECORDING_SECRET": bool(RECORDING_SECRET),
        "backend_available": bool(RECORDING_BACKEND_AVAILABLE),
        "backend_error": RECORDING_BACKEND_ERROR,
    }


def _required_env_ready() -> tuple[bool, str]:
    missing: list[str] = []
    if not SESSION_STRING:
        missing.append("SESSION_STRING")
    if not API_ID:
        missing.append("API_ID")
    if not API_HASH:
        missing.append("API_HASH")
    if missing:
        return False, f"missing_env: {', '.join(missing)}"
    return True, ""


def _import_pytgcalls_backend() -> tuple[str, Any]:
    last_error: Optional[Exception] = None
    candidates = [
        ("pytgcalls", "GroupCallFactory"),
        ("pytgcalls.group_call_factory", "GroupCallFactory"),
        ("pytgcalls.factory", "GroupCallFactory"),
    ]

    for module_name, attr_name in candidates:
        try:
            module = __import__(module_name, fromlist=[attr_name])
            factory = getattr(module, attr_name)
            return "group_call_factory", factory
        except Exception as exc:
            last_error = exc

    try:
        from pytgcalls import PyTgCalls  # type: ignore

        return "pytgcalls", PyTgCalls
    except Exception as exc:
        last_error = exc

    raise ImportError(
        "No compatible pytgcalls backend was found. "
        "This service needs either GroupCallFactory or a PyTgCalls build that exposes raw group-call support."
    ) from last_error


try:
    PYTGCALLS_KIND, PYTGCALLS_BACKEND = _import_pytgcalls_backend()
    RECORDING_BACKEND_AVAILABLE = True
    RECORDING_BACKEND_ERROR = ""
except Exception as exc:
    PYTGCALLS_KIND = ""
    PYTGCALLS_BACKEND = None
    RECORDING_BACKEND_AVAILABLE = False
    RECORDING_BACKEND_ERROR = f"backend_import_error: {exc}"


@dataclass
class RecorderSession:
    chat_id: str
    deliver_to: str = ""
    started_by: str = ""
    group_title: str = ""
    title: str = ""
    status: str = "idle"
    mode: str = "raw"
    started_at: float = 0.0
    last_error: str = ""
    group_call: Any = None
    pcm_path: Path = field(default_factory=Path)
    final_path: Path = field(default_factory=Path)
    fd: Optional[int] = None
    stop_task: Optional[asyncio.Task] = None


class StartRequest(BaseModel):
    chat_id: str = Field(..., min_length=1)
    deliver_to: str = Field(..., min_length=1)
    started_by: str = ""
    group_title: str = ""
    title: str = ""


class StopRequest(BaseModel):
    chat_id: str = Field(..., min_length=1)
    deliver_to: str = ""
    stopped_by: str = ""
    group_title: str = ""
    caption: str = ""
    auto: bool = False


class StatusRequest(BaseModel):
    chat_id: str = Field(..., min_length=1)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def rec_dir_for(chat_id: str) -> Path:
    p = ROOT / str(chat_id).replace("-", "m")
    ensure_dir(p)
    return p


def now() -> int:
    return int(time.time() * 1000)


def sanitize_caption(text: str) -> str:
    return " ".join(str(text or "").split()).strip()


async def tg_send_audio(deliver_to: str, path: Path, caption: str) -> tuple[bool, str]:
    if not BOT_TOKEN:
        return False, "BOT_TOKEN missing"
    if not deliver_to:
        return False, "deliver_to missing"
    if not path.exists() or path.stat().st_size <= 0:
        return False, "file missing"

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for method, field_name in (("sendAudio", "audio"), ("sendDocument", "document")):
            try:
                with path.open("rb") as fh:
                    files = {field_name: (path.name, fh, "application/octet-stream")}
                    data = {"chat_id": deliver_to, "caption": caption or ""}
                    if method == "sendAudio":
                        data["supports_streaming"] = "true"
                    r = await client.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
                        data=data,
                        files=files,
                    )
                try:
                    j = r.json()
                except Exception:
                    j = None
                if r.status_code < 400 and isinstance(j, dict) and j.get("ok"):
                    return True, method
                logger.error(
                    "upload failed method=%s status=%s body=%s",
                    method,
                    r.status_code,
                    getattr(r, "text", "")[:800],
                )
            except Exception:
                logger.exception("upload exception method=%s", method)

    return False, "telegram_send_failed"


class RecorderManager:
    def __init__(self, client: TelegramClient) -> None:
        if PYTGCALLS_BACKEND is None:
            raise RuntimeError(RECORDING_BACKEND_ERROR or "pytgcalls backend missing")

        self.client = client
        self.sessions: dict[str, RecorderSession] = {}
        self.lock = asyncio.Lock()
        self.write_lock = threading.Lock()

        if PYTGCALLS_KIND == "group_call_factory":
            try:
                self.factory = PYTGCALLS_BACKEND(
                    self.client,
                    mtproto_backend=PYTGCALLS_BACKEND.MTPROTO_CLIENT_TYPE.TELETHON,
                )
            except Exception:
                self.factory = PYTGCALLS_BACKEND(self.client)
        else:
            self.factory = PYTGCALLS_BACKEND(self.client)

        if not hasattr(self.factory, "get_raw_group_call"):
            raise RuntimeError(
                "PyTgCalls is installed, but this build does not expose get_raw_group_call. "
                "This recorder requires a backend that supports raw group calls."
            )

    def _paths(self, chat_id: str) -> tuple[Path, Path]:
        d = rec_dir_for(chat_id)
        t = now()
        pcm_path = d / f"recording_{chat_id}_{t}.pcm"
        final_path = d / f"recording_{chat_id}_{t}.ogg"
        return pcm_path, final_path

    def _on_recorded_data(self, session: RecorderSession, frame: bytes, length: int) -> None:
        if not session.fd or not frame:
            return
        try:
            with self.write_lock:
                os.write(session.fd, frame[:length] if length else frame)
        except Exception:
            logger.exception("write_failed chat_id=%s", session.chat_id)

    async def _auto_stop(self, chat_id: str, minutes: int) -> None:
        try:
            await asyncio.sleep(max(1, minutes) * 60)
            await self.stop(chat_id, auto=True)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("auto_stop_failed chat_id=%s", chat_id)

    async def start(
        self,
        chat_id: str,
        deliver_to: str,
        started_by: str = "",
        group_title: str = "",
        title: str = "",
    ) -> dict[str, Any]:
        async with self.lock:
            old = self.sessions.get(chat_id)
            if old and old.status == "recording":
                return {
                    "ok": True,
                    "recording": True,
                    "chat_id": chat_id,
                    "mode": old.mode,
                    "file_name": old.pcm_path.name,
                }

            pcm_path, final_path = self._paths(chat_id)
            session = RecorderSession(
                chat_id=chat_id,
                deliver_to=deliver_to,
                started_by=started_by,
                group_title=group_title,
                title=title,
                status="recording",
                mode="raw",
                started_at=time.time(),
                pcm_path=pcm_path,
                final_path=final_path,
            )
            self.sessions[chat_id] = session

            try:
                session.fd = os.open(session.pcm_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
                session.group_call = self.factory.get_raw_group_call(
                    on_recorded_data=lambda gc, frame, length: self._on_recorded_data(session, frame, length)
                )
                await session.group_call.start(int(chat_id), enable_action=False)
                session.stop_task = asyncio.create_task(self._auto_stop(chat_id, MAX_RECORDING_MINUTES))
                return {
                    "ok": True,
                    "recording": True,
                    "chat_id": chat_id,
                    "mode": session.mode,
                    "file_name": session.pcm_path.name,
                }
            except Exception as exc:
                session.last_error = str(exc)
                logger.exception("start_failed chat_id=%s", chat_id)
                try:
                    if session.fd:
                        os.close(session.fd)
                        session.fd = None
                except Exception:
                    pass
                self.sessions.pop(chat_id, None)
                raise

    def _convert_pcm_to_ogg(self, pcm_path: Path, ogg_path: Path) -> None:
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "s16le",
            "-ar",
            str(OUTPUT_SAMPLE_RATE),
            "-ac",
            str(OUTPUT_CHANNELS),
            "-i",
            str(pcm_path),
            "-c:a",
            "libopus",
            "-b:a",
            "64k",
            str(ogg_path),
        ]
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

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
                if session.group_call:
                    maybe = session.group_call.stop()
                    if asyncio.iscoroutine(maybe):
                        await maybe
            except Exception as exc:
                session.last_error = str(exc)
                logger.exception("stop_group_call_failed chat_id=%s", chat_id)

            try:
                if session.fd:
                    os.close(session.fd)
                    session.fd = None
            except Exception:
                logger.exception("close_fd_failed chat_id=%s", chat_id)

            session.status = "stopped"
            target = (deliver_to or session.deliver_to or "").strip()
            if not target:
                self.sessions.pop(chat_id, None)
                return {
                    "ok": False,
                    "error": "deliver_to_missing",
                    "file_name": session.pcm_path.name,
                    "last_error": session.last_error,
                }

            try:
                self._convert_pcm_to_ogg(session.pcm_path, session.final_path)
                uploaded_path = session.final_path
                upload_caption = sanitize_caption(
                    caption
                    or " | ".join(
                        x
                        for x in [
                            session.group_title,
                            group_title,
                            f"by:{stopped_by}" if stopped_by else "",
                            "recording",
                        ]
                        if x
                    )
                )
                sent, result = await tg_send_audio(target, uploaded_path, upload_caption)
            except Exception as exc:
                session.last_error = str(exc)
                logger.exception("conversion_or_upload_failed chat_id=%s", chat_id)
                sent, result = False, "convert_or_upload_failed"
                uploaded_path = session.pcm_path

            size = uploaded_path.stat().st_size if uploaded_path.exists() else 0
            try:
                if uploaded_path.exists() and sent:
                    uploaded_path.unlink(missing_ok=True)
                if session.pcm_path.exists():
                    session.pcm_path.unlink(missing_ok=True)
            except Exception:
                logger.exception("cleanup_failed chat_id=%s", chat_id)

            self.sessions.pop(chat_id, None)
            return {
                "ok": True,
                "recording": False,
                "chat_id": chat_id,
                "file_name": uploaded_path.name,
                "sent": sent,
                "send_result": result,
                "size": size,
                "last_error": session.last_error,
                "auto": auto,
            }

    async def status(self, chat_id: str) -> dict[str, Any]:
        async with self.lock:
            session = self.sessions.get(chat_id)
            if not session:
                return {"ok": True, "recording": False}
            return {
                "ok": True,
                "recording": session.status == "recording",
                "chat_id": chat_id,
                "mode": session.mode,
                "file_name": session.pcm_path.name,
                "started_at": session.started_at,
                "last_error": session.last_error,
                "deliver_to": session.deliver_to,
            }


async def ensure_manager() -> Optional[RecorderManager]:
    global client, manager, RECORDING_BACKEND_AVAILABLE, RECORDING_BACKEND_ERROR

    async with _get_lock():
        if manager is not None:
            return manager

        ready, reason = _required_env_ready()
        if not ready:
            RECORDING_BACKEND_AVAILABLE = False
            RECORDING_BACKEND_ERROR = reason
            logger.error("recording env not ready: %s | %s", reason, env_check())
            return None

        if PYTGCALLS_BACKEND is None:
            RECORDING_BACKEND_AVAILABLE = False
            RECORDING_BACKEND_ERROR = RECORDING_BACKEND_ERROR or "backend_import_error"
            logger.error("recording backend unavailable: %s", env_check())
            return None

        try:
            client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
            await client.start()
            manager = RecorderManager(client)
            RECORDING_BACKEND_AVAILABLE = True
            RECORDING_BACKEND_ERROR = ""
            logger.info("recording backend initialized: %s", env_check())
            return manager
        except Exception as exc:
            RECORDING_BACKEND_AVAILABLE = False
            RECORDING_BACKEND_ERROR = str(exc)
            logger.exception("recording init failed")
            try:
                if client:
                    await client.disconnect()
            except Exception:
                pass
            client = None
            manager = None
            return None


async def shutdown_manager() -> None:
    global client, manager

    async with _get_lock():
        try:
            if manager:
                for s in list(manager.sessions.values()):
                    if s.stop_task and not s.stop_task.done():
                        s.stop_task.cancel()
                    try:
                        if s.group_call:
                            maybe = s.group_call.stop()
                            if asyncio.iscoroutine(maybe):
                                await maybe
                    except Exception:
                        logger.exception("shutdown_stop_failed chat_id=%s", s.chat_id)
                    try:
                        if s.fd:
                            os.close(s.fd)
                            s.fd = None
                    except Exception:
                        pass
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
            client = None
            manager = None