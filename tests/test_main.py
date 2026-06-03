import json
import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    import main
    return TestClient(main.app)


def test_invalid_json_returns_error(client):
    with client.websocket_connect("/ws") as ws:
        ws.send_text("not-json")
        data = ws.receive_json()
    assert "error" in data
    assert "JSON" in data["error"]


def test_missing_stock_key_returns_error(client):
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"wrong_key": "NYSE:IBM"})
        data = ws.receive_json()
    assert "error" in data
    assert "stock" in data["error"]


def test_invalid_stock_format_returns_error(client):
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"stock": "NODIVIDER"})
        data = ws.receive_json()
    assert "error" in data
    assert "EXCHANGE:SYMBOL" in data["error"]


def test_subscription_timeout_returns_error(client, monkeypatch):
    import main
    monkeypatch.setattr(main, "SUBSCRIPTION_TIMEOUT", 0.05)
    with client.websocket_connect("/ws") as ws:
        # send nothing — server should time out and send error
        data = ws.receive_json()
    assert "error" in data
    assert "subscription" in data["error"].lower()


def test_valid_subscription_streams_history_then_live(client):
    mock_rows = [{"price": "100", "volume": "500"}]

    async def mock_stream_csv(exchange, symbol, day=None):
        for row in mock_rows:
            yield row

    async def mock_tail_stream(symbol, last_id):
        yield {"price": "101", "volume": "600"}
        return

    with patch("main.stream_csv", mock_stream_csv), \
         patch("main.tail_stream", mock_tail_stream), \
         patch("main.get_stream_tip", new_callable=AsyncMock, return_value="0"):
        with client.websocket_connect("/ws") as ws:
            ws.send_json({"stock": "NYSE:IBM"})
            ws.receive_json()  # subscription confirmation
            history_msg = ws.receive_json()
            live_msg = ws.receive_json()

    from transform import transform_tick
    assert history_msg == {"source": "history", "data": {"price": "100", "volume": "500"}}
    assert live_msg["source"] == "live"
    assert live_msg["data"] == transform_tick({"price": "101", "volume": "600"})
