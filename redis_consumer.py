import json
from typing import AsyncIterator

import redis.asyncio as aioredis

from config import REDIS_URL

_client: aioredis.Redis | None = None


def get_client() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _client


async def get_stream_tip(symbol: str) -> str:
    entries = await get_client().xrevrange(symbol, count=1)
    return entries[0][0] if entries else "0"


async def tail_stream(symbol: str, last_id: str) -> AsyncIterator[dict]:
    client = get_client()
    current_id = last_id
    while True:
        results = await client.xread({symbol: current_id}, count=100, block=100)
        if not results:
            continue
        for _stream, messages in results:
            for msg_id, fields in messages:
                current_id = msg_id
                yield json.loads(fields["data"])
