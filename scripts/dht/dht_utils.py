import os
import asyncio
import json
import time
from kademlia.network import Server

def _parse_bootstrap_csv(csv_val: str):
    """
    "100.64.0.10:8468,100.64.0.11:8469,127.0.0.1"
    → [("100.64.0.10",8468), ("100.64.0.11",8469), ("127.0.0.1",8468)]
    """
    pairs = []
    for part in (csv_val or "").split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            h, p = part.split(":", 1)
            pairs.append((h.strip(), int(p.strip())))
        else:
            pairs.append((part, 8468))
    return pairs or [("127.0.0.1", 8468)]

# 우선순위: 환경변수 DHT_BOOTSTRAP → 기본(127.0.0.1:8468)
BOOTSTRAP_ADDRS = _parse_bootstrap_csv(os.getenv("DHT_BOOTSTRAP", "127.0.0.1:8468"))

class DHTClient:
    def __init__(self, port, bootstrap_addrs=None):
        self.port = port
        self.server = Server()
        self.started = False
        self.bootstrap_addrs = bootstrap_addrs or BOOTSTRAP_ADDRS

    async def start(self):
        if self.started:
            return
        # listen on the given client port (0 = ephemeral allowed if caller prefers)
        await self.server.listen(self.port)
        await self.server.bootstrap(self.bootstrap_addrs)
        self.started = True

    async def set_json(self, key: str, value: dict):
        # store as JSON string
        await self.server.set(key, json.dumps(value))

    async def get_json(self, key: str):
        v = await self.server.get(key)
        if v is None:
            return None
        try:
            return json.loads(v)
        except Exception:
            return None

    async def stop(self):
        self.server.stop()

# Helpers for quick get/set from scripts:
async def dht_set(port, key, value, bootstrap_csv=None):
    """
    bootstrap_csv 예: "100.64.0.10:8468,100.64.0.11:8469"
    지정하지 않으면 환경변수 DHT_BOOTSTRAP 또는 기본값 사용
    """
    addrs = _parse_bootstrap_csv(bootstrap_csv) if bootstrap_csv else None
    c = DHTClient(port, addrs)
    await c.start()
    await c.set_json(key, value)
    # 소규모 환경에서 전파 안정화를 위한 짧은 대기
    await asyncio.sleep(0.2)
    await c.stop()

async def dht_get(port, key, bootstrap_csv=None):
    addrs = _parse_bootstrap_csv(bootstrap_csv) if bootstrap_csv else None
    c = DHTClient(port, addrs)
    await c.start()
    v = await c.get_json(key)
    await c.stop()
    return v
