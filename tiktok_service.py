import asyncio
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional

import yt_dlp
from telethon import TelegramClient
from telethon.sessions import StringSession
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, DisconnectEvent, RoomUserSeqEvent
from pytgcalls import PyTgCalls

try:
    from pytgcalls import StreamType
except Exception:  # pragma: no cover
    StreamType = None

AudioPiped = None
AudioVideoPiped = None
for _import_path in (
    "pytgcalls.types.input_stream",
    "pytgcalls.types.input_streams",
    "pytgcalls.types",
):
    try:  # pragma: no cover - compatibility shim
        _mod = __import__(_import_path, fromlist=["AudioPiped", "AudioVideoPiped"])
        AudioPiped = getattr(_mod, "AudioPiped", AudioPiped)
        AudioVideoPiped = getattr(_mod, "AudioVideoPiped", AudioVideoPiped)
    except Exception:
        pass

logger = logging.getLogger("tiktok_service")


class TikTokSession:
    def __init__(self, chat_id: int):
        self.chat_id = chat_id
        self.client: Optional[TikTokLiveClient] = None
        self.viewers: int = 0
        self.title: str = ""
        self.username: str = ""
        self.source_url: str = ""
        self.is_active: bool = False
        self.started_at: float = 0.0
        self.last_seen_at: float = 0.0
        self.task: Optional[asyncio.Task] = None
        self.lock = asyncio.Lock()

    def duration_seconds(self) -> int:
        if not self.started_at:
            return 0
        end = self.last_seen_at if (not self.is_active and self.last_seen_at >= self.started_at) else time.time()
        return max(0, int(end - self.started_at))

    def as_state(self) -> Dict[str, Any]:
        return {
            "status": "playing" if self.is_active else "idle",
            "viewers": int(self.viewers or 0),
            "title": self.title or "",
            "username": self.username or "",
            "source_url": self.source_url or "",
            "duration": self.duration_seconds(),
            "elapsed": self.duration_seconds(),
            "started_at": int(self.started_at) if self.started_at else 0,
        }


class TikTokService:
    def __init__(self):
        self.api_id = int(os.getenv("API_ID", "0"))
        self.api_hash = os.getenv("API_HASH", "").strip()
        self.session_string = os.getenv("SESSION_STRING", "").strip()
        if not self.api_id or not self.api_hash or not self.session_string:
            raise RuntimeError("missing_tiktok_env")
        self.client: Optional[TelegramClient] = None
        self.pytgcalls: Optional[PyTgCalls] = None
        self.sessions: Dict[int, TikTokSession] = {}
        self._boot_lock = asyncio.Lock()
        self._ready = False

    def _cookie_file_path(self) -> Optional[str]:
        cookiefile = os.getenv("TIKTOK_COOKIES_FILE", "").strip()
        if cookiefile:
            if Path(cookiefile).exists():
                return cookiefile
            logger.warning("TIKTOK_COOKIES_FILE is set but file does not exist: %s", cookiefile)
        raw_cookies = os.getenv("TIKTOK_COOKIES_TEXT", "").strip() or os.getenv("TIKTOK_COOKIES", "").strip()
        if raw_cookies:
            tmp_path = Path(tempfile.gettempdir()) / "tiktok_cookies.txt"
            try:
                tmp_path.write_text(raw_cookies, encoding="utf-8")
                return str(tmp_path)
            except Exception:
                logger.exception("Failed to write TikTok cookies to temp file")
        return None

    async def boot(self):
        if self._ready:
            return
        async with self._boot_lock:
            if self._ready:
                return
            self.client = TelegramClient(StringSession(self.session_string), self.api_id, self.api_hash)
            if not self.client.is_connected():
                await self.client.start()
            self.pytgcalls = PyTgCalls(self.client)
            await self.pytgcalls.start()
            self._ready = True
            logger.info("TikTokService booted successfully")

    def _extract_unique_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        txt = str(url).strip()
        m = re.search(r"@([\w\.-]+)|tiktok\.com/@([\w\.-]+)", txt, re.I)
        return (m.group(1) or m.group(2)) if m else None

    async def _get_stream_url(self, url: str) -> Optional[str]:
        try:
            ydl_opts = {"format": "best", "quiet": True, "no_warnings": True, "noplaylist": True}
            cookiefile = self._cookie_file_path()
            if cookiefile:
                ydl_opts["cookiefile"] = cookiefile
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    return None
                stream_url = info.get("url")
                if stream_url:
                    return stream_url
                formats = info.get("formats") or []
                for fmt in formats:
                    if fmt.get("url"):
                        return fmt["url"]
                return None
        except Exception:
            logger.exception("TikTok stream URL extraction failed")
            return None

    def _build_stream_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if StreamType is not None:
            try:
                stream_type = getattr(StreamType(), "local_stream", None)
                if stream_type is not None:
                    kwargs["stream_type"] = stream_type
            except Exception:
                pass
        return kwargs

    async def _join_with_fallbacks(self, chat_id: int, stream_url: str, video: bool) -> None:
        if not self.pytgcalls:
            raise RuntimeError("pytgcalls_not_ready")
        join_kwargs = self._build_stream_kwargs()
        joined = False
        if video and AudioVideoPiped is not None and hasattr(self.pytgcalls, "join_group_call"):
            try:
                await self.pytgcalls.join_group_call(chat_id, AudioVideoPiped(stream_url), **join_kwargs)
                joined = True
            except Exception as exc:
                logger.warning("AudioVideoPiped join failed, fallback to play(): %s", exc)
        if not joined and AudioPiped is not None and hasattr(self.pytgcalls, "join_group_call"):
            try:
                await self.pytgcalls.join_group_call(chat_id, AudioPiped(stream_url), **join_kwargs)
                joined = True
            except Exception as exc:
                logger.warning("AudioPiped join failed, fallback to play(): %s", exc)
        if not joined:
            if hasattr(self.pytgcalls, "play"):
                await self.pytgcalls.play(chat_id, stream_url)
                joined = True
            else:
                raise RuntimeError("pytgcalls_unsupported_api")
        if not joined:
            raise RuntimeError("tiktok_join_failed")

    def _attach_events(self, session: TikTokSession):
        if not session.client:
            return

        @session.client.on(ConnectEvent)
        async def on_connect(_: ConnectEvent):
            session.is_active = True
            now = time.time()
            session.last_seen_at = now
            if not session.started_at:
                session.started_at = now

        @session.client.on(RoomUserSeqEvent)
        async def on_viewers(event: RoomUserSeqEvent):
            session.viewers = int(getattr(event, "user_count", getattr(event, "viewer_count", 0)) or 0)
            session.last_seen_at = time.time()

        @session.client.on(DisconnectEvent)
        async def on_disconnect(_: DisconnectEvent):
            session.is_active = False
            session.last_seen_at = time.time()

        async def _run_client():
            try:
                await session.client.start()
            except Exception as exc:
                logger.warning("TikTok client task stopped: %s", exc)

        session.task = asyncio.create_task(_run_client())

    async def _viewer_loop(self, session: TikTokSession):
        while session.is_active:
            session.last_seen_at = time.time()
            await asyncio.sleep(12)

    def _ensure_session(self, chat_id: int) -> TikTokSession:
        session = self.sessions.get(chat_id)
        if not session:
            session = TikTokSession(chat_id=chat_id)
            self.sessions[chat_id] = session
        return session

    async def start(self, chat_id: int, tiktok_url: str, video: bool = True) -> Dict[str, Any]:
        await self.boot()
        session = self._ensure_session(chat_id)
        async with session.lock:
            try:
                tiktok_url = (tiktok_url or "").strip()
                if not tiktok_url:
                    return {"ok": False, "error": "رابط تيك توك غير موجود"}
                stream_url = await self._get_stream_url(tiktok_url)
                if not stream_url:
                    return {"ok": False, "error": "تعذر استخراج رابط البث"}
                unique_id = self._extract_unique_id(tiktok_url)
                session.client = TikTokLiveClient(unique_id=unique_id) if unique_id else None
                session.source_url = tiktok_url
                session.title = "TikTok Live"
                session.username = unique_id or "unknown"
                session.viewers = 0
                session.is_active = False
                session.started_at = 0.0
                session.last_seen_at = 0.0
                if session.client:
                    self._attach_events(session)
                await self._join_with_fallbacks(chat_id, stream_url, video)
                now = time.time()
                session.is_active = True
                session.started_at = now
                session.last_seen_at = now
                if session.task and not session.task.done():
                    session.task.cancel()
                session.task = asyncio.create_task(self._viewer_loop(session))
                return {
                    "ok": True,
                    "state": {
                        "status": "playing",
                        "viewers": session.viewers,
                        "title": session.title,
                        "username": session.username,
                        "source_url": session.source_url,
                        "duration": 0,
                        "elapsed": 0,
                    },
                }
            except Exception as e:
                logger.error("TikTok start error: %s", e)
                return {"ok": False, "error": str(e)}

    async def stop(self, chat_id: int) -> Dict[str, Any]:
        session = self.sessions.get(chat_id)
        if not session or not session.is_active:
            return {"ok": False, "error": "لا يوجد بث نشط"}
        async with session.lock:
            try:
                if self.pytgcalls:
                    try:
                        if hasattr(self.pytgcalls, "leave_group_call"):
                            await self.pytgcalls.leave_group_call(chat_id)
                    except Exception:
                        pass
                    try:
                        if hasattr(self.pytgcalls, "stop"):
                            await self.pytgcalls.stop(chat_id)
                    except Exception:
                        pass
                if session.client:
                    try:
                        await session.client.disconnect()
                    except Exception:
                        pass
                if session.task and not session.task.done():
                    session.task.cancel()
                session.last_seen_at = time.time()
                session.is_active = False
                session.viewers = 0
                return {
                    "ok": True,
                    "state": {
                        "status": "idle",
                        "viewers": 0,
                        "title": session.title,
                        "username": session.username,
                        "source_url": session.source_url,
                        "duration": session.duration_seconds(),
                        "elapsed": session.duration_seconds(),
                    },
                }
            except Exception as e:
                logger.error("TikTok stop error: %s", e)
                return {"ok": False, "error": str(e)}

    async def get_state(self, chat_id: int) -> Dict[str, Any]:
        session = self.sessions.get(chat_id)
        if not session:
            return {"status": "idle", "viewers": 0, "title": "", "username": "", "source_url": "", "duration": 0, "elapsed": 0}
        return session.as_state()

    async def refresh_state(self, chat_id: int) -> Dict[str, Any]:
        session = self.sessions.get(chat_id)
        if not session:
            return {"ok": False, "state": {"status": "idle", "viewers": 0, "title": "", "username": "", "source_url": "", "duration": 0, "elapsed": 0}}
        session.last_seen_at = time.time()
        return {"ok": True, "state": session.as_state()}

    async def cleanup(self, chat_id: int) -> None:
        session = self.sessions.get(chat_id)
        if not session:
            return
        async with session.lock:
            try:
                if session.task and not session.task.done():
                    session.task.cancel()
            finally:
                session.is_active = False

    async def shutdown(self) -> None:
        for chat_id in list(self.sessions.keys()):
            await self.cleanup(chat_id)
        if self.pytgcalls:
            try:
                if hasattr(self.pytgcalls, "stop"):
                    await self.pytgcalls.stop()
            except Exception:
                pass
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass

    def dump_debug_state(self) -> Dict[str, Any]:
        return {
            "ready": self._ready,
            "sessions": {
                str(chat_id): {
                    "status": "playing" if s.is_active else "idle",
                    "viewers": s.viewers,
                    "title": s.title,
                    "username": s.username,
                    "source_url": s.source_url,
                    "duration": s.duration_seconds(),
                    "started_at": int(s.started_at) if s.started_at else 0,
                    "last_seen_at": int(s.last_seen_at) if s.last_seen_at else 0,
                }
                for chat_id, s in self.sessions.items()
            },
        }


service = TikTokService()

__all__ = ["TikTokSession", "TikTokService", "service"]