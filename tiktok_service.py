# tiktok_service.py
# خدمة مستقلة تماماً لبث تيك توك (لا تعتمد على player.py)

import asyncio
import os
import logging
from typing import Optional, Dict, Any

from telethon import TelegramClient
from telethon.sessions import StringSession
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, DisconnectEvent, ViewerCountUpdateEvent
from pytgcalls import PyTgCalls
from pytgcalls.types import AudioVideoPiped, AudioPiped, StreamType
import yt_dlp

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

    async def boot(self):
        if self._ready:
            return
        async with self._boot_lock:
            if self._ready:
                return

            self.client = TelegramClient(
                StringSession(self.session_string),
                self.api_id,
                self.api_hash
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
            if unique_id:
                session.client = TikTokLiveClient(unique_id=unique_id)
                self._attach_events(session, chat_id)

            piped = AudioVideoPiped(stream_url, stream_type=StreamType().local_stream) if video else \
                    AudioPiped(stream_url, stream_type=StreamType().local_stream)

            await self.pytgcalls.join_group_call(chat_id, piped)

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
                    "source_url": tiktok_url
                }
            }

        except Exception as e:
            logger.error(f"TikTok start error: {e}")
            return {"ok": False, "error": str(e)}

    async def stop(self, chat_id: int) -> Dict[str, Any]:
        session = self.sessions.get(chat_id)
        if not session or not session.is_active:
            return {"ok": False, "error": "لا يوجد بث نشط"}

        try:
            await self.pytgcalls.leave_group_call(chat_id)
            if session.client:
                await session.client.disconnect()
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
            "username": session.username
        }

    # ==================== Helpers ====================

    def _extract_unique_id(self, url: str) -> Optional[str]:
        import re
        match = re.search(r"@([\w\.-]+)|tiktok\.com/@([\w\.-]+)", url)
        return match.group(1) or match.group(2) if match else None

    async def _get_stream_url(self, url: str) -> Optional[str]:
        try:
            ydl_opts = {"format": "best", "quiet": True, "no_warnings": True}
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return info.get("url") or info.get("formats", [{}])[0].get("url")
        except Exception:
            return None

    def _attach_events(self, session: TikTokSession, chat_id: int):
        @session.client.on(ConnectEvent)
        async def on_connect(_: ConnectEvent):
            session.is_active = True

        @session.client.on(ViewerCountUpdateEvent)
        async def on_viewers(event: ViewerCountUpdateEvent):
            session.viewers = event.viewer_count

        @session.client.on(DisconnectEvent)
        async def on_disconnect(_: DisconnectEvent):
            session.is_active = False

        asyncio.create_task(session.client.start())

    async def _viewer_loop(self, session: TikTokSession, chat_id: int):
        while session.is_active:
            await asyncio.sleep(12)