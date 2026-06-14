import asyncio
import os
import shutil
import uuid
from pathlib import Path

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
        self.state = {}
        self.ready = False
        self.calls_started = False
        self.lock = asyncio.Lock()
        self.boot_lock = asyncio.Lock()

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

        for p in (
            "/root/.deno/bin/deno",
            "/home/oai/.deno/bin/deno",
            "/usr/local/bin/deno",
            "/usr/bin/deno",
        ):
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

    async def _download_telegram_file(self, file_id: str, chat_id: str) -> str:
        if not self.bot_token:
            raise RuntimeError("missing_bot_token")

        async with httpx.AsyncClient(timeout=120) as h:
            g = await h.get(
                f"https://api.telegram.org/bot{self.bot_token}/getFile",
                params={"file_id": file_id},
            )
            g.raise_for_status()
            j = g.json()
            file_path = j["result"]["file_path"]
            ext = Path(file_path).suffix or ".mp3"
            out = TMP / f"{chat_id}_{uuid.uuid4().hex}{ext}"
            d = await h.get(f"https://api.telegram.org/file/bot{self.bot_token}/{file_path}")
            d.raise_for_status()
            out.write_bytes(d.content)
            return str(out)

    async def _download_url(self, url: str, chat_id: str) -> str:
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
                        "player_client": ["mweb", "web_safari"],
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
                        "player_client": ["web_safari", "mweb"],
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

        for idx, attempt in enumerate(attempts, start=1):
            def _do():
                opts = self._yt_opts(outtmpl, attempt["extractor_args"])
                opts["format"] = attempt["format"]
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    if info is None:
                        raise RuntimeError("extract_info returned None")

            try:
                await asyncio.wait_for(asyncio.to_thread(_do), timeout=150)
                for cand in TMP.glob(f"{prefix}.*"):
                    if cand.is_file() and cand.stat().st_size > 1024:
                        return str(cand)
            except Exception as e:
                last_error = e
                msg = str(e).lower()
                print(f"yt_dlp_attempt_{idx}_error", msg)
                if (
                    "requested format" in msg
                    or "page needs to be reloaded" in msg
                    or "failed to extract any player response" in msg
                ):
                    continue

        raise RuntimeError(f"yt_dlp_download_failed:{last_error}")

    async def _resolve_source(self, chat_id: str, source_type: str, source_id: str) -> str:
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
            source_path = await self._resolve_source(chat_id, source_type, source_id)
            self.state[chat_id] = {
                "source_type": str(source_type),
                "source_id": str(source_id),
                "title": str(title or ""),
                "duration": int(duration or 0),
                "path": source_path,
                "status": "playing",
                "position": 0,
            }
            last_error = None
            for _ in range(2):
                try:
                    print("before_play", chat_id, source_path)
                    try:
                        await self._invoke("play", int(chat_id), source_path)
                    except TypeError:
                        await self._invoke("play", chat_id=int(chat_id), media=source_path)
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