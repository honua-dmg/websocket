import asyncio
import json
import logging
from typing import AsyncIterator

import redis.asyncio as aioredis
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

from config import REDIS_URL

logger = logging.getLogger(__name__)

_client: aioredis.Redis | None = None


def get_client() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _client


async def _connect_with_retry(max_attempts: int = 4) -> aioredis.Redis:
    client = get_client()
    delay = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            await client.ping()
            return client
        except Exception as exc:
            if attempt == max_attempts:
                raise ConnectionError(
                    f"Cannot reach Redis at {REDIS_URL} after {max_attempts} attempts: {exc}"
                ) from exc
            logger.warning(
                "Redis unavailable (attempt %d/%d), retrying in %.0fs: %s",
                attempt, max_attempts, delay, exc,
            )
            await asyncio.sleep(delay)
            delay *= 2
    raise AssertionError("unreachable")


async def get_stream_tip(symbol: str) -> str:
    client = await _connect_with_retry()
    entries = await client.xrevrange(symbol, count=1)
    return entries[0][0] if entries else "0"


async def tail_stream(symbol: str, last_id: str) -> AsyncIterator[dict]:
    client = await _connect_with_retry()
    current_id = last_id
    waiting_logged = False

    while True:
        try:
            results = await client.xread({symbol: current_id}, count=100, block=100)
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            # Only transport failures are worth retrying. A ResponseError means Redis
            # understood the command and refused it — a malformed stream ID, say — so it
            # propagates instead of spinning here forever against the same bad input.
            logger.warning("Lost Redis connection, reconnecting: %s", exc)
            client = await _connect_with_retry()
            continue

        if not results:
            if not waiting_logged:
                logger.info("Waiting for stream '%s'...", symbol)
                waiting_logged = True
            continue

        waiting_logged = False
        for _stream, messages in results:
            for msg_id, fields in messages:
                current_id = msg_id
                yield json.loads(fields["data"])
