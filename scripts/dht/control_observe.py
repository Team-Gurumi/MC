# MC-main/scripts/dht/control_observe.py
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import os, asyncio
from dht_utils import dht_get

app = FastAPI()
DHT_CLIENT_PORT = int(os.getenv("DHT_CLIENT_PORT", "8473"))

async def _resolve_logs_meta(task_id: str):
    key = f"task:{task_id}:endpoint"
    val = await dht_get(DHT_CLIENT_PORT, key)
    if not val:
        raise HTTPException(status_code=404, detail="endpoint not found")
    url = val.get("url"); token = val.get("rendezvousToken")
    if not url or not token:
        raise HTTPException(status_code=503, detail="agent not ready")
    return {
        "wsUrl": f"{url}?token={token}",
        "agentId": val.get("agentId"),
        "lastSeenAt": val.get("lastSeenAt"),
        "version": val.get("version", 1),
    }

@app.get("/tasks/{task_id}/logs-meta")
async def logs_meta(task_id: str):
    return JSONResponse(await _resolve_logs_meta(task_id))

@app.get("/healthz")
async def healthz():
    return {"ok": True}
