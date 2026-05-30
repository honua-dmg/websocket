import json
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture(autouse=True)
def reset_redis_client():
    import redis_consumer
    redis_consumer._client = None
    yield
    redis_consumer._client = None


async def test_ping_with_retry_raises_connection_error_after_all_attempts():
    mock_client = AsyncMock()
    mock_client.ping.side_effect = Exception("connection refused")

    with patch("redis_consumer.get_client", return_value=mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        from redis_consumer import _ping_with_retry
        with pytest.raises(ConnectionError, match="redis://localhost:6379"):
            await _ping_with_retry(max_attempts=3)

    assert mock_client.ping.call_count == 3


async def test_ping_with_retry_succeeds_on_third_attempt():
    mock_client = AsyncMock()
    mock_client.ping.side_effect = [Exception("refused"), Exception("refused"), None]

    with patch("redis_consumer.get_client", return_value=mock_client), \
         patch("asyncio.sleep", new_callable=AsyncMock):
        from redis_consumer import _ping_with_retry
        result = await _ping_with_retry(max_attempts=3)

    assert result is mock_client
    assert mock_client.ping.call_count == 3


async def test_get_stream_tip_returns_zero_for_empty_stream():
    mock_client = AsyncMock()
    mock_client.ping.return_value = True
    mock_client.xrevrange.return_value = []

    with patch("redis_consumer.get_client", return_value=mock_client):
        from redis_consumer import get_stream_tip
        tip = await get_stream_tip("IBM")

    assert tip == "0"


async def test_get_stream_tip_returns_latest_id():
    mock_client = AsyncMock()
    mock_client.ping.return_value = True
    mock_client.xrevrange.return_value = [("1700000000000-0", {"data": "{}"})]

    with patch("redis_consumer.get_client", return_value=mock_client):
        from redis_consumer import get_stream_tip
        tip = await get_stream_tip("IBM")

    assert tip == "1700000000000-0"


async def test_tail_stream_yields_parsed_json():
    mock_client = AsyncMock()
    mock_client.ping.return_value = True
    mock_client.xread.side_effect = [
        [("IBM", [("1-1", {"data": json.dumps({"price": "100", "volume": "500"})})])],
        [("IBM", [("1-2", {"data": json.dumps({"price": "101", "volume": "600"})})])],
    ]

    with patch("redis_consumer.get_client", return_value=mock_client):
        from redis_consumer import tail_stream
        results = []
        async for tick in tail_stream("IBM", "0"):
            results.append(tick)
            if len(results) == 2:
                break

    assert results == [{"price": "100", "volume": "500"}, {"price": "101", "volume": "600"}]


async def test_tail_stream_logs_waiting_when_stream_empty(capsys):
    mock_client = AsyncMock()
    mock_client.ping.return_value = True
    # First call empty, second call has data
    mock_client.xread.side_effect = [
        [],
        [("IBM", [("1-1", {"data": json.dumps({"price": "100"})})])],
    ]

    with patch("redis_consumer.get_client", return_value=mock_client):
        from redis_consumer import tail_stream
        async for _ in tail_stream("IBM", "0"):
            break

    captured = capsys.readouterr()
    assert "Waiting for stream" in captured.err or "Waiting for stream" in captured.out
