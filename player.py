import asyncio, os, uuid
from pathlib import Path
import httpx
from telethon import TelegramClient
from telethon.sessions import StringSession
from pytgcalls import PyTgCalls

TMP=Path("/tmp/audio")
TMP.mkdir(parents=True, exist_ok=True)

class VoicePlayer:
    def __init__(self):
        self.api_id=int(os.getenv("API_ID","0"))
        self.api_hash=os.getenv("API_HASH","")
        self.session=os.getenv("SESSION_STRING","")
        self.bot_token=os.getenv("BOT_TOKEN","")
        self.client=TelegramClient(StringSession(self.session),self.api_id,self.api_hash)
        self.calls=PyTgCalls(self.client)
        self.state={}
        self.ready=False

    async def boot(self):
        if self.ready:
            return
        await self.client.start()
        self.calls.start()
        self.ready=True

    async def _invoke(self,name,*args,**kwargs):
        fn=getattr(self.calls,name,None)
        if not fn:
            raise RuntimeError(f"missing method: {name}")
        res=fn(*args,**kwargs)
        return await res if asyncio.iscoroutine(res) else res

    async def _download_telegram_file(self,file_id,chat_id):
        async with httpx.AsyncClient(timeout=120) as h:
            g=await h.get(f"https://api.telegram.org/bot{self.bot_token}/getFile",params={"file_id":file_id})
            g.raise_for_status()
            j=g.json()
            file_path=j["result"]["file_path"]
            ext=Path(file_path).suffix or ".mp3"
            out=TMP/f"{chat_id}_{uuid.uuid4().hex}{ext}"
            d=await h.get(f"https://api.telegram.org/file/bot{self.bot_token}/{file_path}")
            d.raise_for_status()
            out.write_bytes(d.content)
            return str(out)

    async def start(self,chat_id,source_type,source_id,title="",duration=0):
        await self.boot()
        chat_id=str(chat_id)
        if source_type=="url":
            source_path=str(source_id)
        else:
            source_path=await self._download_telegram_file(str(source_id),chat_id)
        self.state[chat_id]={"source_type":source_type,"source_id":str(source_id),"title":title,"duration":int(duration or 0),"path":source_path,"status":"playing","position":0}
        await self._invoke("play",int(chat_id),source_path)
        return self.state[chat_id]

    async def pause(self,chat_id):
        await self.boot()
        chat_id=str(chat_id)
        await self._invoke("pause",int(chat_id))
        if chat_id in self.state:
            self.state[chat_id]["status"]="paused"
        return self.state.get(chat_id,{})

    async def resume(self,chat_id):
        await self.boot()
        chat_id=str(chat_id)
        await self._invoke("resume",int(chat_id))
        if chat_id in self.state:
            self.state[chat_id]["status"]="playing"
        return self.state.get(chat_id,{})

    async def stop(self,chat_id):
        await self.boot()
        chat_id=str(chat_id)
        await self._invoke("stop",int(chat_id))
        st=self.state.pop(chat_id,None)
        if st and st.get("path"):
            try:
                Path(st["path"]).unlink(missing_ok=True)
            except:
                pass
        return st or {}