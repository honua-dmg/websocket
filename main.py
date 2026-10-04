import asyncio
import contextlib
from datetime import datetime, timezone
import json
import os
import uuid
from typing import Awaitable, Callable

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from csv_reader import stream_csv
from redis_consumer import get_stream_tip, tail_stream
from transform import transform_tick

app = FastAPI()

SUBSCRIPTION_TIMEOUT = float(os.getenv("WS_SUBSCRIPTION_TIMEOUT", "10"))

# client_id -> WebSocket, populated on connect, removed on disconnect/error.
# Per-process only: running uvicorn with multiple workers gives each its own dict.
active_clients: dict[str, WebSocket] = {}

Send = Callable[[dict], Awaitable[None]]


async def _stream(send: Send, stock: str, exchange: str, symbol: str) -> None:
    await send({"message": f"subscribed to {stock}, streaming history..."})

    last_history_row = None
    try:
        async for row in stream_csv(exchange, symbol):
            await send({"source": "history", "data": row})
            last_history_row = row
    except FileNotFoundError:
        await send({"error": f"no CSV history for {stock} for the day: {datetime.now(timezone.utc).date().isoformat()}"})

    if last_history_row and last_history_row.get("stream_offset"):
        bookmark = last_history_row["stream_offset"]
    else:
        bookmark = await get_stream_tip(symbol)

    async for tick in tail_stream(symbol, last_id=bookmark):
        await send({"source": "live", "data": transform_tick(tick)})


async def _watch_disconnect(websocket: WebSocket, send: Send) -> None:
    """Resolve only when the client actually goes away.

    One subscription per connection, so nothing a client sends after it is
    meaningful — but "said something unexpected" is not "hung up". Browsers
    cannot send protocol-level pings at all, so an app-level heartbeat is a
    web client's only keepalive option and must not kill its own stream.
    """
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return
        await send({"error": "one subscription per connection; message ignored"})


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

            if not isinstance(msg, dict) or not isinstance(msg.get("stock"), str):
                await send({"error": 'expected {"stock": "EXCHANGE:SYMBOL"}'})
                await websocket.close()
                return

            stock = msg["stock"]
            exchange, _, symbol = stock.partition(":")
            # exchange and symbol are interpolated into a file path, so reject anything
            # that could climb out of DATA_ROOT
            if not exchange or not symbol or any(c in stock for c in ("/", "\\", "..")):
                await send(
                    {"error": f"invalid stock format '{stock}', expected EXCHANGE:SYMBOL"}
                )
                await websocket.close()
                return

            stream_task = asyncio.create_task(_stream(send, stock, exchange, symbol))
            disconnect_task = asyncio.create_task(_watch_disconnect(websocket, send))

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
                await send({"error": str(e)})
            except Exception:
                pass
    finally:
        active_clients.pop(client_id, None)
