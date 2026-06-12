import os
from fastapi import FastAPI, Header, HTTPException, Request
from player import VoicePlayer

app=FastAPI()
player=VoicePlayer()
SECRET=os.getenv("KEEPALIVE_SECRET","").strip()

def guard(v:str|None):
    if SECRET and (v or "").strip()!=SECRET:
        raise HTTPException(status_code=403,detail="forbidden")

@app.get("/")
async def root():
    return {"ok":True}

@app.get("/ping")
async def ping(x_keepalive_secret: str|None=Header(default=None)):
    guard(x_keepalive_secret)
    return {"ok":True}

@app.post("/start")
async def start(req:Request,x_keepalive_secret: str|None=Header(default=None)):
    guard(x_keepalive_secret)
    body=await req.json()
    try:
        state=await player.start(body["chatId"],body["source_type"],body["source_id"],body.get("title",""),body.get("duration",0))
        return {"ok":True,"action":"start","state":state}
    except Exception as e:
        return {"ok":False,"error":str(e)}

@app.post("/pause")
async def pause(req:Request,x_keepalive_secret: str|None=Header(default=None)):
    guard(x_keepalive_secret)
    body=await req.json()
    try:
        state=await player.pause(body["chatId"])
        return {"ok":True,"action":"pause","state":state}
    except Exception as e:
        return {"ok":False,"error":str(e)}

@app.post("/resume")
async def resume(req:Request,x_keepalive_secret: str|None=Header(default=None)):
    guard(x_keepalive_secret)
    body=await req.json()
    try:
        state=await player.resume(body["chatId"])
        return {"ok":True,"action":"resume","state":state}
    except Exception as e:
        return {"ok":False,"error":str(e)}

@app.post("/stop")
async def stop(req:Request,x_keepalive_secret: str|None=Header(default=None)):
    guard(x_keepalive_secret)
    body=await req.json()
    try:
        state=await player.stop(body["chatId"])
        return {"ok":True,"action":"stop","state":state}
    except Exception as e:
        return {"ok":False,"error":str(e)}

@app.post("/seek")
async def seek(req:Request,x_keepalive_secret: str|None=Header(default=None)):
    guard(x_keepalive_secret)
    body=await req.json()
    try:
        state=await player.seek(body["chatId"],body.get("delta",0))
        return {"ok":True,"action":"seek","state":state}
    except Exception as e:
        return {"ok":False,"error":str(e)}