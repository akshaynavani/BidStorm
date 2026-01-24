import asyncio

import fakeredis
import pytest

from bidstorm.exchange.dmp import DMPClient


@pytest.fixture
def redis():
    return fakeredis.FakeAsyncRedis()


async def test_lookup_hit_and_miss(redis):
    dmp = DMPClient(redis, timeout_ms=50)
    await dmp.add_segments("u1", ["sports", "auto"])
    assert await dmp.segments("u1") == ["auto", "sports"]
    assert await dmp.segments("nobody") == []
    assert await dmp.segments(None) == []
    assert dmp.stats() == {"hits": 1, "misses": 2, "timeouts": 0, "errors": 0}


async def test_lookup_timeout_degrades_to_empty():
    class SlowRedis:
        async def smembers(self, key):
            await asyncio.sleep(0.2)
            return {b"x"}

    dmp = DMPClient(SlowRedis(), timeout_ms=5)
    assert await dmp.segments("u1") == []
    assert dmp.timeouts == 1


async def test_lookup_error_degrades_to_empty():
    class BrokenRedis:
        async def smembers(self, key):
            raise ConnectionError("down")

    dmp = DMPClient(BrokenRedis(), timeout_ms=5)
    assert await dmp.segments("u1") == []
    assert dmp.errors == 1
