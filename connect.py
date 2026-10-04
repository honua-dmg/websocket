import asyncio
import json
import sys

import websockets


HOST = "ws://139.59.32.232:8765/ws"


async def connect(stock: str,count):
    print(f"Connecting to {HOST}...")
    async with websockets.connect(HOST) as ws:
        await ws.send(json.dumps({"stock": stock}))
        print(f"Subscribed to {stock}\n")
        async for message in ws:
            data = json.loads(message)
            count+=1
            #print(count)            
            print(f"Received: {data}")
            


if __name__ == "__main__":
    count = 0
    stock = sys.argv[1] if len(sys.argv) > 1 else "NSE:RELIANCE"
    asyncio.run(connect(stock, count))
    print(f"Received {count} messages for {stock}")
