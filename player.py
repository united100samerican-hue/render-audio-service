import asyncio, os, uuid
from pathlib import Path
import httpx
from telethon import TelegramClient
from telethon.sessions import StringSession
from pytgcalls import PyTgCalls

TMP = Path("/tmp/audio")
TMP.mkdir(parents=True, exist_ok=True)

class VoicePlayer:
    def __init__(self):
        self.api_id = int(os.getenv("API_ID", "0"))
        self.api_hash = os.getenv("API_HASH", "").strip()
        self.session = os.getenv("SESSION_STRING", "").strip()
        self.bot_token = os.getenv("BOT_TOKEN", "").strip()
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

    async def _resolve_source(self, chat_id: str, source_type: str, source_id: str) -> str:
        source_type = str(source_type or "").strip().lower()
        source_id = str(source_id or "").strip()
        if source_type == "url":
            return source_id
        return await self._download_telegram_file(source_id, chat_id)

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
            st = self.state.pop(str(chat_id), None)
            targets = (
                self.calls,
                getattr(self.calls, "group_call", None),
                getattr(self.calls, "mtproto", None),
                getattr(self.calls, "_group_call", None),
                getattr(self.calls, "_call", None),
            )
            methods = ("leave_current_group_call", "leave_group_call", "stop", "hangup", "close")
            done = False
            errors = []
            for obj in targets:
                if not obj:
                    continue
                for name in methods:
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
            await asyncio.sleep(0.5)
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