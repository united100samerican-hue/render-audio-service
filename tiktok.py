# tiktok.py - TikTok Live Streamer (منفصل للصيانة)
import asyncio
import logging
from typing import Optional, Dict, Any
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, DisconnectEvent, ViewerCountUpdateEvent
from py_tgcalls import PyTgCalls
from py_tgcalls.types import AudioVideoPiped, AudioPiped, StreamType
import yt_dlp

logger = logging.getLogger("tiktok_streamer")

class TikTokSession:
    def __init__(self, chat_id: int):
        self.chat_id = chat_id
        self.client: Optional[TikTokLiveClient] = None
        self.viewers: int = 0
        self.title: str = ""
        self.username: str = ""
        self.is_active: bool = False
        self.task: Optional[asyncio.Task] = None

class TikTokStreamer:
    def __init__(self, pytgcalls: PyTgCalls, telethon_client):
        self.pytgcalls = pytgcalls
        self.telethon = telethon_client
        self.sessions: Dict[int, TikTokSession] = {}

    async def start_stream(self, chat_id: int, tiktok_url: str, video: bool = True) -> Dict[str, Any]:
        """يبدأ بث تيك توك لايف"""
        session = self.sessions.get(chat_id) or TikTokSession(chat_id)
        self.sessions[chat_id] = session

        try:
            # استخراج رابط البث
            stream_url = await self._extract_stream_url(tiktok_url)
            if not stream_url:
                return {"ok": False, "error": "تعذر استخراج رابط البث من تيك توك"}

            # إعداد TikTokLive للمشاهدين والمعلومات
            unique_id = self._extract_unique_id(tiktok_url)
            if unique_id:
                session.client = TikTokLiveClient(unique_id=unique_id)
                self._setup_tiktok_events(session, chat_id)

            # الانضمام للمكالمة
            piped = AudioVideoPiped(stream_url, stream_type=StreamType().local_stream) if video else \
                    AudioPiped(stream_url, stream_type=StreamType().local_stream)

            await self.pytgcalls.join_group_call(chat_id, piped)
            
            session.is_active = True
            session.title = f"TikTok Live - {unique_id}"
            session.username = unique_id or "unknown"

            # تشغيل مهمة تحديث المشاهدين
            if session.task:
                session.task.cancel()
            session.task = asyncio.create_task(self._viewer_updater(session, chat_id))

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

    async def stop_stream(self, chat_id: int) -> Dict[str, Any]:
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

    async def _extract_stream_url(self, url: str) -> Optional[str]:
        """يحاول استخراج رابط البث باستخدام yt-dlp"""
        try:
            ydl_opts = {"format": "best", "quiet": True, "no_warnings": True}
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return info.get("url") or info.get("formats", [{}])[0].get("url")
        except Exception:
            return None

    def _setup_tiktok_events(self, session: TikTokSession, chat_id: int):
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

    async def _viewer_updater(self, session: TikTokSession, chat_id: int):
        while session.is_active:
            await asyncio.sleep(12)
            # يمكنك هنا إضافة تحديث للوحة في المستقبل إن أردت