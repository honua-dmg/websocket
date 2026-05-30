import json

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from csv_reader import stream_csv
from redis_consumer import get_stream_tip, tail_stream

app = FastAPI()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    try:
        msg = await websocket.receive_json()
        stock = msg.get("stock", "")
        if ":" not in stock:
            await websocket.send_json({"error": f"invalid stock format '{stock}', expected EXCHANGE:SYMBOL"})
            await websocket.close()
            return

        exchange, symbol = stock.split(":", 1)

        bookmark = await get_stream_tip(symbol)

        try:
            async for row in stream_csv(exchange, symbol):
                await websocket.send_json({"source": "history", "data": row})
        except FileNotFoundError:
            await websocket.send_json({"error": f"no CSV history for {stock} today"})

        async for tick in tail_stream(symbol, last_id=bookmark):
            await websocket.send_json({"source": "live", "data": tick})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"error": str(e)})
        except Exception:
            pass
