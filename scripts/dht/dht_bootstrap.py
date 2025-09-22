# MC-main/scripts/dht/dht_bootstrap.py
import os, asyncio
try:
    import uvloop
    uvloop.install()
except Exception:
    pass
from kademlia.network import Server

BOOTSTRAP_PORT = int(os.getenv("DHT_BOOTSTRAP_PORT", "8468"))

async def main():
    server = Server()
    await server.listen(BOOTSTRAP_PORT)
    print(f"[bootstrap] DHT bootstrap listening on 0.0.0.0:{BOOTSTRAP_PORT}")
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        server.stop()

if __name__ == "__main__":
    asyncio.run(main())
