"""Redis-backed Data Management Platform (DMP) for audience-segment enrichment.

Segments are stored as Redis sets under `dmp:user:<user_id>`. Lookups run under a tight
timeout: an enrichment miss is far cheaper than blowing the auction's latency budget.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable

from redis.asyncio import Redis

log = logging.getLogger(__name__)

KEY_PREFIX = "dmp:user:"
DEFAULT_TTL_SECONDS = 30 * 86_400


def user_key(user_id: str) -> str:
    return f"{KEY_PREFIX}{user_id}"


class DMPClient:
    def __init__(self, redis: Redis, timeout_ms: int = 5) -> None:
        self.redis = redis
        self.timeout = timeout_ms / 1000.0
        self.hits = 0
        self.misses = 0
        self.timeouts = 0
        self.errors = 0

    async def segments(self, user_id: str | None) -> list[str]:
        if not user_id:
            self.misses += 1
            return []
        try:
            raw = await asyncio.wait_for(self.redis.smembers(user_key(user_id)), self.timeout)
        except asyncio.TimeoutError:
            self.timeouts += 1
            return []
        except Exception:  # noqa: BLE001 - enrichment must never fail an auction
            self.errors += 1
            log.exception("DMP lookup failed for %s", user_id)
            return []
        if not raw:
            self.misses += 1
            return []
        self.hits += 1
        return sorted(s.decode() if isinstance(s, bytes) else s for s in raw)

    async def add_segments(
        self, user_id: str, segments: Iterable[str], ttl: int = DEFAULT_TTL_SECONDS
    ) -> None:
        segs = list(segments)
        if not segs:
            return
        key = user_key(user_id)
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.sadd(key, *segs)
            pipe.expire(key, ttl)
            await pipe.execute()

    def stats(self) -> dict[str, int]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "timeouts": self.timeouts,
            "errors": self.errors,
        }
