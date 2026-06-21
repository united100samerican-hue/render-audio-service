import asyncio
import logging
import os
import re
import tempfile
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
    StreamType = None  # PyTgCalls changed its export surface across versions.

# We try the legacy paths first, then fall back to newer/alternate layouts,
# and finally to a no-stream-object fallback that uses `play(...)`.
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
        self.is_active: bool = False
        self.task: Optional[asyncio.Task] = None


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
        """
        Priority:
        1) TIKTOK_COOKIES_FILE -> path to an existing cookies.txt file
        2) TIKTOK_COOKIES -> raw Netscape cookie text, written to /tmp
        """
        cookiefile = os.getenv("TIKTOK_COOKIES_FILE", "").strip()
        if cookiefile:
            if Path(cookiefile).exists():
                return cookiefile
            logger.warning("TIKTOK_COOKIES_FILE is set but file does not exist: %s", cookiefile)

        raw_cookies = os.getenv("TIKTOK_COOKIES", "").strip()
        if raw_cookies:
            tmp_path = Path(tempfile.gettempdir()) / "tiktok_cookies.txt"
            try:
                tmp_path.write_text(raw_cookies, encoding="utf-8")
                return str(tmp_path)
            except Exception:
                logger.exception("Failed to write TIKTOK_COOKIES to temp file")
        return None

    async def boot(self):
        if self._ready:
            return

        async with self._boot_lock:
            if self._ready:
                return

            self.client = TelegramClient(
                StringSession(self.session_string),
                self.api_id,
                self.api_hash,
            )

            if not self.client.is_connected():
                await self.client.start()

            self.pytgcalls = PyTgCalls(self.client)
            await self.pytgcalls.start()

            self._ready = True
            logger.info("TikTokService booted successfully")

    async def start(self, chat_id: int, tiktok_url: str, video: bool = True) -> Dict[str, Any]:
        await self.boot()

        session = self.sessions.get(chat_id) or TikTokSession(chat_id)
        self.sessions[chat_id] = session

        try:
            stream_url = await self._get_stream_url(tiktok_url)
            if not stream_url:
                return {"ok": False, "error": "تعذر استخراج رابط البث"}

            unique_id = self._extract_unique_id(tiktok_url)
            session.client = TikTokLiveClient(unique_id=unique_id) if unique_id else None

            if session.client:
                self._attach_events(session, chat_id)

            join_kwargs: Dict[str, Any] = {}
            if StreamType is not None:
                try:
                    stream_type = getattr(StreamType(), "local_stream", None)
                    if stream_type is not None:
                        join_kwargs["stream_type"] = stream_type
                except Exception:
                    pass

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
                else:
                    raise RuntimeError("pytgcalls_unsupported_api")

            session.is_active = True
            session.title = "TikTok Live"
            session.username = unique_id or "unknown"

            if session.task:
                session.task.cancel()

            session.task = asyncio.create_task(self._viewer_loop(session, chat_id))

            return {
                "ok": True,
                "state": {
                    "status": "playing",
                    "viewers": session.viewers,
                    "title": session.title,
                    "username": session.username,
                    "source_url": tiktok_url,
                },
            }

        except Exception as e:
            logger.error("TikTok start error: %s", e)
            return {"ok": False, "error": str(e)}

    async def stop(self, chat_id: int) -> Dict[str, Any]:
        session = self.sessions.get(chat_id)
        if not session or not session.is_active:
            return {"ok": False, "error": "لا يوجد بث نشط"}

        try:
            if self.pytgcalls:
                try:
                    await self.pytgcalls.leave_group_call(chat_id)
                except Exception:
                    # Some PyTgCalls versions expose leave_group_call, others may differ.
                    if hasattr(self.pytgcalls, "stop"):
                        await self.pytgcalls.stop(chat_id)

            if session.client:
                try:
                    await session.client.disconnect()
                except Exception:
                    pass

            if session.task:
                session.task.cancel()

            session.is_active = False
            session.viewers = 0

            return {"ok": True, "state": {"status": "idle"}}

        except Exception as e:
            return {"ok": False, "error": str(e)}

    async def get_state(self, chat_id: int) -> Dict[str, Any]:
        session = self.sessions.get(chat_id)
        if not session:
            return {"status": "idle", "viewers": 0}

        return {
            "status": "playing" if session.is_active else "idle",
            "viewers": session.viewers,
            "title": session.title,
            "username": session.username,
        }

    def _extract_unique_id(self, url: str) -> Optional[str]:
        match = re.search(r"@([\w\.-]+)|tiktok\.com/@([\w\.-]+)", url)
        return (match.group(1) or match.group(2)) if match else None

    async def _get_stream_url(self, url: str) -> Optional[str]:
        try:
            ydl_opts = {
                "format": "best",
                "quiet": True,
                "no_warnings": True,
            }

            cookiefile = self._cookie_file_path()
            if cookiefile:
                ydl_opts["cookiefile"] = cookiefile

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    return None

                return info.get("url") or (info.get("formats") or [{}])[0].get("url")

        except Exception:
            logger.exception("TikTok stream URL extraction failed")
            return None

    def _attach_events(self, session: TikTokSession, chat_id: int):
        @session.client.on(ConnectEvent)
        async def on_connect(_: ConnectEvent):
            session.is_active = True

        @session.client.on(RoomUserSeqEvent)
        async def on_viewers(event: RoomUserSeqEvent):
            session.viewers = getattr(event, "user_count", getattr(event, "viewer_count", 0))

        @session.client.on(DisconnectEvent)
        async def on_disconnect(_: DisconnectEvent):
            session.is_active = False

        # Start the TikTok client task without blocking the service.
        asyncio.create_task(session.client.start())

    async def _viewer_loop(self, session: TikTokSession, chat_id: int):
        while session.is_active:
            await asyncio.sleep(12)