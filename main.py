import asyncio
from datetime import datetime, timezone
import json
import os
import uuid

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from csv_reader import stream_csv
from redis_consumer import get_stream_tip, tail_stream
from transform import transform_tick

app = FastAPI()

SUBSCRIPTION_TIMEOUT = float(os.getenv("WS_SUBSCRIPTION_TIMEOUT", "10"))

# client_id -> WebSocket, populated on connect, removed on disconnect/error.
# Per-process only: running uvicorn with multiple workers gives each its own dict.
active_clients: dict[str, WebSocket] = {}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    client_id = str(uuid.uuid4())
    active_clients[client_id] = websocket

    async def send(payload: dict) -> None:
        await websocket.send_json({**payload, "client_id": client_id})

    try:
        try:
            try:
                raw = await asyncio.wait_for(
                    websocket.receive_text(), timeout=SUBSCRIPTION_TIMEOUT
                )
            except asyncio.TimeoutError:
                await send(
                    {"error": f"no subscription received within {SUBSCRIPTION_TIMEOUT}s"}
                )
                await websocket.close()
                return

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await send({"error": "expected JSON message"})
                await websocket.close()
                return

            if not isinstance(msg, dict) or "stock" not in msg:
                await send({"error": 'expected {"stock": "EXCHANGE:SYMBOL"}'})
                await websocket.close()
                return

            stock = msg["stock"]
            if ":" not in stock:
                await send(
                    {"error": f"invalid stock format '{stock}', expected EXCHANGE:SYMBOL"}
                )
                await websocket.close()
                return

            exchange, symbol = stock.split(":", 1)
            await send({"message": f"subscribed to {stock}, streaming history..."})

            last_history_row = None
            try:
                async for row in stream_csv(exchange, symbol):
                    await send({"source": "history", "data": row})
                    last_history_row = row
            except FileNotFoundError:
                await send({"error": f"no CSV history for {stock} for the day: {datetime.now(timezone.utc).date().isoformat()}"})

            if last_history_row and "stream_offset" in last_history_row:
                bookmark = last_history_row["stream_offset"]
                with open('/data/logs.csv', 'a') as f:
                    f.write(f"{datetime.now(timezone.utc).isoformat()},{exchange},{symbol},{bookmark},{last_history_row.get('timestamp', 'N/A')}\n")
            else:
                bookmark = await get_stream_tip(symbol)
            async for tick in tail_stream(symbol, last_id=bookmark):
                await send({"source": "live", "data": transform_tick(tick)})

        except WebSocketDisconnect:
            pass
        except Exception as e:
            try:
                await send({"error": str(e)})
            except Exception:
                pass
    finally:
        active_clients.pop(client_id, None)
