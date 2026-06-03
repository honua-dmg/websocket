import asyncio
import json
import logging
from typing import AsyncIterator

import redis.asyncio as aioredis

from config import REDIS_URL

logger = logging.getLogger(__name__)

_client: aioredis.Redis | None = None


def get_client() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _client


async def _ping_with_retry(max_attempts: int = 3) -> aioredis.Redis:
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
                "Redis unavailable (attempt %d/%d), retrying in %.0fs...",
                attempt, max_attempts, delay,
            )
            await asyncio.sleep(delay)
            delay *= 2
    return client  # unreachable


async def get_stream_tip(symbol: str) -> str:
    client = await _ping_with_retry()
    entries = await client.xrevrange(symbol, count=1)
    return entries[0][0] if entries else "0"


async def _wait_for_redis() -> aioredis.Redis:
    client = get_client()
    delay = 1.0
    attempt = 0
    while True:
        try:
            await client.ping()
            return client
        except Exception as exc:
            attempt += 1
            logger.warning(
                "Redis unavailable (attempt %d), retrying in %.0fs: %s",
                attempt, delay, exc,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)


async def tail_stream(symbol: str, last_id: str) -> AsyncIterator[dict]:
    client = await _wait_for_redis()
    current_id = last_id
    waiting_logged = False

    while True:
        try:
            results = await client.xread({symbol: current_id}, count=100, block=100)
        except Exception as exc:
            logger.warning("Redis went away, waiting for it to come back: %s", exc)
            client = await _wait_for_redis()
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
