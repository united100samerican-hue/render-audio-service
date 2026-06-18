import asyncio
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import httpx
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl import functions
from pytgcalls import PyTgCalls

TMP = Path("/tmp/audio")
TMP.mkdir(parents=True, exist_ok=True)

class VoicePlayer:
    def __init__(self):
        self.api_id = int(os.getenv("API_ID", "0"))
        self.api_hash = os.getenv("API_HASH", "").strip()
        self.session = os.getenv("SESSION_STRING", "").strip()
        self.bot_token = os.getenv("BOT_TOKEN", "").strip()
        self.yt_cookies_text = os.getenv("YT_COOKIES_TEXT", "").strip()
        self.yt_cookies_file = os.getenv("YT_COOKIES_FILE", "").strip()
        self.pot_provider_url = os.getenv("POT_PROVIDER_URL", "").strip() or "http://127.0.0.1:4416"
        if not self.api_id or not self.api_hash or not self.session:
            raise RuntimeError("missing_render_env")
        self.client = None
        self.calls = None
        self.state: dict[str, dict[str, Any]] = {}
        self.ready = False
        self.calls_started = False
        self.lock = asyncio.Lock()
        self.boot_lock = asyncio.Lock()
        self.background_tasks: dict[str, asyncio.Task] = {}

    def _ensure_objects(self):
        if self.client is None or self.calls is None:
            self.client = TelegramClient(StringSession(self.session), self.api_id, self.api_hash)
            self.calls = PyTgCalls(self.client)

    def _cookiefile(self):
        if self.yt_cookies_file:
            p = Path(self.yt_cookies_file)
            if p.exists():
                return str(p)
        if self.yt_cookies_text:
            p = TMP / "yt_cookies.txt"
            try:
                current = p.read_text(encoding="utf-8")
            except Exception:
                current = ""
            if current != self.yt_cookies_text:
                p.write_text(self.yt_cookies_text, encoding="utf-8")
            return str(p)
        return ""

    def _find_deno(self):
        deno_env = os.getenv("DENO_PATH", "").strip()
        if deno_env and Path(deno_env).exists():
            return deno_env
        found = shutil.which("deno")
        if found:
            return found
        for p in ("/root/.deno/bin/deno", "/home/oai/.deno/bin/deno", "/usr/local/bin/deno", "/usr/bin/deno"):
            if Path(p).exists():
                return p
        return ""

    def _is_url(self, s):
        s = str(s or "").strip().lower()
        return s.startswith(("http://", "https://")) or "youtu.be/" in s or "youtube.com/" in s or "music.youtube.com/" in s

    def _looks_like_file_id(self, s):
        s = str(s or "").strip()
        if not s or self._is_url(s):
            return False
        if " " in s:
            return False
        return len(s) > 20 and "/" not in s and "\\" not in s

    def _yt_opts(self, outtmpl, extractor_args):
        opts = {
            "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
            "outtmpl": outtmpl,
            "noplaylist": True,
            "quiet": True,
            "verbose": True,
            "no_warnings": True,
            "retries": 5,
            "fragment_retries": 5,
            "socket_timeout": 45,
            "nocheckcertificate": True,
            "geo_bypass": True,
            "ignoreerrors": False,
            "noprogress": True,
            "concurrent_fragment_downloads": 3,
            "extractor_args": extractor_args,
            "remote_components": ["ejs:github"],
            "js_runtimes": {"deno": {}},
        }
        deno_path = self._find_deno()
        if deno_path:
            opts["js_runtimes"] = {"deno": {"path": deno_path}}
        cookiefile = self._cookiefile()
        if cookiefile:
            opts["cookiefile"] = cookiefile
        return opts

    def _meta_from_info(self, info, fallback_url=""):
        if not isinstance(info, dict):
            return {}
        entry = info
        entries = info.get("entries")
        if entries:
            try:
                first = next((e for e in entries if isinstance(e, dict)), None)
            except TypeError:
                first = None
            if first:
                entry = first
        video_id = str(entry.get("id") or "").strip()
        webpage_url = str(entry.get("webpage_url") or entry.get("original_url") or fallback_url or "").strip()
        title = str(entry.get("title") or "").strip()
        duration = int(entry.get("duration") or 0)
        thumbnail = str(entry.get("thumbnail") or "").strip()
        is_live = bool(entry.get("is_live") or entry.get("live_status") in {"is_live", "is_upcoming"})
        direct_url = str(entry.get("url") or "").strip()
        if not webpage_url and video_id:
            webpage_url = f"https://www.youtube.com/watch?v={video_id}"
        if not thumbnail and video_id:
            thumbnail = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
        return {
            "video_id": video_id,
            "webpage_url": webpage_url,
            "title": title,
            "duration": duration,
            "thumbnail": thumbnail,
            "is_live": is_live,
            "direct_url": direct_url,
        }

    async def boot(self):
        if self.ready and self.calls_started:
            return
        async with self.boot_lock:
            if self.ready and self.calls_started:
                return
            self._ensure_objects()
            if not self.client.is_connected():
                await self.client.start()
            if not self.calls_started:
                res = self.calls.start()
                if asyncio.iscoroutine(res):
                    await res
                self.calls_started = True
            self.ready = True

    async def _invoke(self, name, *args, **kwargs):
        fn = getattr(self.calls, name, None)
        if not fn:
            raise RuntimeError(f"missing_method:{name}")
        res = fn(*args, **kwargs)
        return await res if asyncio.iscoroutine(res) else res

    async def _play_media(self, chat_id, media):
        try:
            await self._invoke("play", int(chat_id), media)
        except TypeError:
            await self._invoke("play", chat_id=int(chat_id), media=media)

    async def _download_telegram_file(self, file_id: str, chat_id: str):
        if not self.bot_token:
            raise RuntimeError("missing_bot_token")
        async with httpx.AsyncClient(timeout=120) as h:
            g = await h.get(f"https://api.telegram.org/bot{self.bot_token}/getFile", params={"file_id": file_id})
            g.raise_for_status()
            j = g.json()
            file_path = j["result"]["file_path"]
            ext = Path(file_path).suffix or ".mp3"
            out = TMP / f"{chat_id}_{uuid.uuid4().hex}{ext}"
            d = await h.get(f"https://api.telegram.org/file/bot{self.bot_token}/{file_path}")
            d.raise_for_status()
            out.write_bytes(d.content)
            return str(out), {}

    async def _probe_url(self, source_id: str):
        try:
            import yt_dlp
        except Exception as e:
            raise RuntimeError(f"missing_yt_dlp:{e}")
        if self._is_url(source_id):
            if not source_id.startswith(("http://", "https://")):
                source_id = f"https://{source_id}"
            url = source_id
        else:
            url = f"ytsearch1:{source_id}"
        def _do():
            opts = self._yt_opts(
                str(TMP / f"probe_{uuid.uuid4().hex}.%(ext)s"),
                {
                    "youtube": {
                        "player_client": ["mweb", "web_safari", "android"],
                        "formats": ["missing_pot"],
                    },
                    "youtubepot-bgutilhttp": {
                        "base_url": [self.pot_provider_url],
                    },
                },
            )
            opts["skip_download"] = True
            opts["quiet"] = True
            opts["verbose"] = False
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        info = await asyncio.wait_for(asyncio.to_thread(_do), timeout=35)
        meta = self._meta_from_info(info, source_id)
        meta["source_type"] = "url"
        meta["source_id"] = source_id
        return meta

    async def meta(self, chat_id, source_type, source_id, title="", duration=0):
        source_type = str(source_type or "").strip().lower()
        source_id = str(source_id or "").strip()
        title = str(title or "").strip()
        duration = int(duration or 0)
        if source_type in {"voice", "audio", "document", "file_id"} or self._looks_like_file_id(source_id):
            return {
                "ok": True,
                "state": {
                    "source_type": source_type or "file_id",
                    "source_id": source_id,
                    "title": title,
                    "duration": duration,
                    "video_id": "",
                    "webpage_url": "",
                    "thumbnail": "",
                    "is_live": False,
                    "direct_url": "",
                },
            }
        meta = await self._probe_url(source_id)
        if title and not meta.get("title"):
            meta["title"] = title
        if not meta.get("title"):
            meta["title"] = source_id
        if duration and not meta.get("duration"):
            meta["duration"] = duration
        return {"ok": True, "state": meta}
async def _download_url(self, url: str, chat_id: str):
        try:
            import yt_dlp
        except Exception as e:
            raise RuntimeError(f"missing_yt_dlp:{e}")
        url = str(url or "").strip()
        if not url:
            raise RuntimeError("empty_url")
        prefix = f"{chat_id}_{uuid.uuid4().hex}"
        outtmpl = str(TMP / f"{prefix}.%(ext)s")
        attempts = [
            {
                "extractor_args": {
                    "youtube": {
                        "player_client": ["mweb", "web_safari", "android"],
                        "formats": ["missing_pot"],
                    },
                    "youtubepot-bgutilhttp": {
                        "base_url": [self.pot_provider_url],
                    },
                },
                "format": "bestaudio[protocol^=m3u8]/bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
            },
            {
                "extractor_args": {
                    "youtube": {
                        "player_client": ["web_safari", "mweb", "android"],
                        "formats": ["missing_pot"],
                    },
                    "youtubepot-bgutilhttp": {
                        "base_url": [self.pot_provider_url],
                    },
                },
                "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
            },
        ]
        last_error = None
        last_meta = {}
        for idx, attempt in enumerate(attempts, start=1):
            def _do():
                opts = self._yt_opts(outtmpl, attempt["extractor_args"])
                opts["format"] = attempt["format"]
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    if info is None:
                        raise RuntimeError("extract_info returned None")
                    return info
            try:
                info = await asyncio.wait_for(asyncio.to_thread(_do), timeout=150)
                last_meta = self._meta_from_info(info, url)
                for cand in TMP.glob(f"{prefix}.*"):
                    if cand.is_file() and cand.stat().st_size > 1024:
                        return str(cand), last_meta
            except Exception as e:
                last_error = e
                msg = str(e).lower()
                print(f"yt_dlp_attempt_{idx}_error", msg)
                if "requested format" in msg or "page needs to be reloaded" in msg or "failed to extract any player response" in msg or "sign in to confirm" in msg or "not available" in msg:
                    continue
        raise RuntimeError(f"yt_dlp_download_failed:{last_error}")

    async def _direct_play_url(self, direct_url: str, chat_id: str):
        if not direct_url:
            return False
        try:
            await self._play_media(chat_id, direct_url)
            return True
        except Exception as e:
            print("direct_play_error", chat_id, str(e))
            return False

    async def _background_download(self, chat_id: str, source_id: str):
        try:
            await self._download_url(source_id, chat_id)
        except Exception as e:
            print("background_download_error", chat_id, str(e))

    async def _resolve_source(self, chat_id: str, source_type: str, source_id: str):
        source_type = str(source_type or "").strip().lower()
        source_id = str(source_id or "").strip()
        if self._is_url(source_id):
            if not source_id.startswith(("http://", "https://")):
                source_id = f"https://{source_id}"
            return await self._download_url(source_id, chat_id)
        if source_type == "url":
            return await self._download_url(f"ytsearch1:{source_id}", chat_id)
        if source_type == "file_id" and self._looks_like_file_id(source_id):
            return await self._download_telegram_file(source_id, chat_id)
        if source_id:
            return await self._download_url(f"ytsearch1:{source_id}", chat_id)
        raise RuntimeError("empty_source")

    async def start(self, chat_id, source_type, source_id, title="", duration=0):
        async with self.lock:
            await self.boot()
            chat_id = str(chat_id)
            source_type = str(source_type or "").strip()
            source_id = str(source_id or "").strip()

            if source_type == "file_id" or self._looks_like_file_id(source_id):
                source_path, source_meta = await self._download_telegram_file(source_id, chat_id)
                resolved_title = str(title or source_meta.get("title") or "")
                resolved_duration = int(duration or source_meta.get("duration") or 0)
                self.state[chat_id] = {
                    "source_type": "file_id",
                    "source_id": source_id,
                    "title": resolved_title,
                    "duration": resolved_duration,
                    "path": source_path,
                    "status": "playing",
                    "position": 0,
                    "webpage_url": "",
                    "thumbnail": "",
                    "video_id": "",
                    "direct_url": "",
                    "is_live": False,
                }
                last_error = None
                for _ in range(2):
                    try:
                        print("before_play", chat_id, source_path)
                        await self._play_media(chat_id, source_path)
                        print("after_play", chat_id)
                        self.state[chat_id]["status"] = "playing"
                        return self.state[chat_id]
                    except Exception as e:
                        last_error = e
                        msg = str(e).lower()
                        print("play_error", chat_id, msg)
                        if "already running" in msg:
                            self.calls_started = True
                            self.ready = True
                            continue
                        await asyncio.sleep(1)
                raise RuntimeError(f"play_failed:{last_error}")

            if self._is_url(source_id):
                if not source_id.startswith(("http://", "https://")):
                    source_id = f"https://{source_id}"
                probe = await self._probe_url(source_id)
                resolved_title = str(title or probe.get("title") or source_id)
                resolved_duration = int(duration or probe.get("duration") or 0)
                direct_url = str(probe.get("direct_url") or "").strip()

                if direct_url:
                    self.state[chat_id] = {
                        "source_type": source_type or "url",
                        "source_id": source_id,
                        "title": resolved_title,
                        "duration": resolved_duration,
                        "path": direct_url,
                        "status": "playing",
                        "position": 0,
                        "webpage_url": str(probe.get("webpage_url") or source_id),
                        "thumbnail": str(probe.get("thumbnail") or ""),
                        "video_id": str(probe.get("video_id") or ""),
                        "direct_url": direct_url,
                        "is_live": bool(probe.get("is_live", False)),
                    }
                    ok = await self._direct_play_url(direct_url, chat_id)
                    if ok:
                        self.background_tasks[chat_id] = asyncio.create_task(self._background_download(chat_id, source_id))
                        return self.state[chat_id]

                source_path, source_meta = await self._download_url(source_id, chat_id)
                self.state[chat_id] = {
                    "source_type": source_type or "url",
                    "source_id": source_id,
                    "title": resolved_title or source_meta.get("title") or "",
                    "duration": resolved_duration or source_meta.get("duration") or 0,
                    "path": source_path,
                    "status": "playing",
                    "position": 0,
                    "webpage_url": str(source_meta.get("webpage_url") or source_id),
                    "thumbnail": str(source_meta.get("thumbnail") or probe.get("thumbnail") or ""),
                    "video_id": str(source_meta.get("video_id") or probe.get("video_id") or ""),
                    "direct_url": str(probe.get("direct_url") or ""),
                    "is_live": bool(probe.get("is_live", False)),
                }
                last_error = None
                for _ in range(2):
                    try:
                        print("before_play", chat_id, source_path)
                        await self._play_media(chat_id, source_path)
                        print("after_play", chat_id)
                        self.state[chat_id]["status"] = "playing"
                        return self.state[chat_id]
                    except Exception as e:
                        last_error = e
                        msg = str(e).lower()
                        print("play_error", chat_id, msg)
                        if "already running" in msg:
                            self.calls_started = True
                            self.ready = True
                            continue
                        await asyncio.sleep(1)
                raise RuntimeError(f"play_failed:{last_error}")

            source_path, source_meta = await self._resolve_source(chat_id, source_type, source_id)
            stype = str(source_type or "").strip()
            sid = str(source_id or "").strip()
            resolved_title = str(title or source_meta.get("title") or "")
            resolved_duration = int(duration or source_meta.get("duration") or 0)
            if stype == "url" and source_meta.get("webpage_url"):
                sid = str(source_meta["webpage_url"]).strip() or sid
            self.state[chat_id] = {
                "source_type": stype,
                "source_id": sid,
                "title": resolved_title,
                "duration": resolved_duration,
                "path": source_path,
                "status": "playing",
                "position": 0,
                "webpage_url": source_meta.get("webpage_url", ""),
                "thumbnail": source_meta.get("thumbnail", ""),
                "video_id": source_meta.get("video_id", ""),
                "direct_url": source_meta.get("direct_url", ""),
                "is_live": bool(source_meta.get("is_live", False)),
            }
            last_error = None
            for _ in range(2):
                try:
                    print("before_play", chat_id, source_path)
                    await self._play_media(chat_id, source_path)
                    print("after_play", chat_id)
                    self.state[chat_id]["status"] = "playing"
                    return self.state[chat_id]
                except Exception as e:
                    last_error = e
                    msg = str(e).lower()
                    print("play_error", chat_id, msg)
                    if "already running" in msg:
                        self.calls_started = True
                        self.ready = True
                        continue
                    await asyncio.sleep(1)
            raise RuntimeError(f"play_failed:{last_error}")

    async def pause(self, chat_id):
        async with self.lock:
            await self.boot()
            chat_id = str(chat_id)
            try:
                await self._invoke("pause", int(chat_id))
            except Exception as e:
                print("pause_error", chat_id, str(e))
            if chat_id in self.state:
                self.state[chat_id]["status"] = "paused"
            return self.state.get(chat_id, {})

    async def resume(self, chat_id):
        async with self.lock:
            await self.boot()
            chat_id = str(chat_id)
            try:
                await self._invoke("resume", int(chat_id))
            except Exception as e:
                print("resume_error", chat_id, str(e))
            if chat_id in self.state:
                self.state[chat_id]["status"] = "playing"
            return self.state.get(chat_id, {})

    async def stop(self, chat_id):
        async with self.lock:
            await self.boot()
            chat_id = int(chat_id)
            key = str(chat_id)
            st = self.state.get(key)
            errors = []
            done = False

            try:
                entity = await self.client.get_entity(chat_id)
                full = await self.client(functions.channels.GetFullChannelRequest(channel=entity))
                call = getattr(getattr(full, "full_chat", None), "call", None)
                if call:
                    try:
                        res = self.client(functions.phone.LeaveGroupCallRequest(call=call, source=0))
                    except TypeError:
                        res = self.client(functions.phone.LeaveGroupCallRequest(call=call))
                    if asyncio.iscoroutine(res):
                        await res
                    done = True
            except Exception as e:
                errors.append(f"raw_leave:{e}")

            if not done:
                targets = (
                    self.calls,
                    getattr(self.calls, "group_call", None),
                    getattr(self.calls, "mtproto", None),
                    getattr(self.calls, "_group_call", None),
                    getattr(self.calls, "_call", None),
                )
                for obj in targets:
                    if not obj:
                        continue
                    for name in ("stop", "leave_current_group_call", "leave_group_call", "hangup", "close"):
                        fn = getattr(obj, name, None)
                        if not callable(fn):
                            continue
                        for args in ((chat_id,), ()):
                            try:
                                res = fn(*args)
                                if asyncio.iscoroutine(res):
                                    await res
                                done = True
                                break
                            except TypeError:
                                continue
                            except Exception as e:
                                errors.append(f"{name}:{e}")
                                break
                        if done:
                            break
                    if done:
                        break

            if not done:
                raise RuntimeError("no_leave_method:" + " | ".join(errors[-3:]))

            st = self.state.pop(key, None)
            if st and st.get("path"):
                try:
                    Path(st["path"]).unlink(missing_ok=True)
                except Exception:
                    pass

            try:
                await self.client.disconnect()
            except Exception as e:
                print("stop_disconnect_error", str(e))

            self.client = None
            self.calls = None
            self.ready = False
            self.calls_started = False
            return st or {}

    async def seek(self, chat_id, delta=0):
        async with self.lock:
            await self.boot()
            chat_id = str(chat_id)
            st = self.state.get(chat_id)
            if not st:
                return {}
            st["position"] = max(0, int(st.get("position", 0)) + int(delta or 0))
            self.state[chat_id] = st
            return st