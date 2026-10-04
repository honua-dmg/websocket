import asyncio
import contextlib
from datetime import datetime, timezone
import json
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from csv_reader import stream_csv
from redis_consumer import get_stream_tip, tail_stream
from transform import transform_tick

app = FastAPI()

SUBSCRIPTION_TIMEOUT = float(os.getenv("WS_SUBSCRIPTION_TIMEOUT", "10"))


async def _stream(websocket: WebSocket, stock: str, exchange: str, symbol: str) -> None:
    await websocket.send_json({"message": f"subscribed to {stock}, streaming history..."})

    last_history_row = None
    try:
        async for row in stream_csv(exchange, symbol):
            await websocket.send_json({"source": "history", "data": row})
            last_history_row = row
    except FileNotFoundError:
        await websocket.send_json({"error": f"no CSV history for {stock} for the day: {datetime.now(timezone.utc).date().isoformat()}"})

    if last_history_row and last_history_row.get("stream_offset"):
        bookmark = last_history_row["stream_offset"]
    else:
        bookmark = await get_stream_tip(symbol)

    async for tick in tail_stream(symbol, last_id=bookmark):
        await websocket.send_json({"source": "live", "data": transform_tick(tick)})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        try:
            raw = await asyncio.wait_for(
                websocket.receive_text(), timeout=SUBSCRIPTION_TIMEOUT
            )
        except asyncio.TimeoutError:
            await websocket.send_json(
                {"error": f"no subscription received within {SUBSCRIPTION_TIMEOUT}s"}
            )
            await websocket.close()
            return

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            await websocket.send_json({"error": "expected JSON message"})
            await websocket.close()
            return

        if not isinstance(msg, dict) or "stock" not in msg:
            await websocket.send_json({"error": 'expected {"stock": "EXCHANGE:SYMBOL"}'})
            await websocket.close()
            return

        stock = msg["stock"]
        if ":" not in stock:
            await websocket.send_json(
                {"error": f"invalid stock format '{stock}', expected EXCHANGE:SYMBOL"}
            )
            await websocket.close()
            return

        exchange, symbol = stock.split(":", 1)

        stream_task = asyncio.create_task(_stream(websocket, stock, exchange, symbol))
        disconnect_task = asyncio.create_task(websocket.receive())

        done, pending = await asyncio.wait(
            {stream_task, disconnect_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        for task in done:
            if not task.cancelled():
                exc = task.exception()
                if exc and not isinstance(exc, WebSocketDisconnect):
                    raise exc

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"error": str(e)})
        except Exception:
            pass
