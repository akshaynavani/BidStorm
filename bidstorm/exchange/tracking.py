"""Server-side record of auction wins, used to validate impression and click pixels.

Clearing prices are never put in tracking URLs (they would be trivially forgeable);
instead the exchange stores the win in Redis keyed by auction + impression and the
tracking endpoints look it up. Impressions are de-duplicated with SET NX.
"""

from __future__ import annotations

from dataclasses import dataclass

from redis.asyncio import Redis

WIN_TTL_SECONDS = 15 * 60


@dataclass(slots=True)
class WinRecord:
    auction_id: str
    imp_id: str
    bidder: str
    price: float
    creative_id: str
    predicted_ctr: float
    adomain: str


def _win_key(auction_id: str, imp_id: str) -> str:
    return f"win:{auction_id}:{imp_id}"


class TrackingStore:
    def __init__(self, redis: Redis, ttl: int = WIN_TTL_SECONDS) -> None:
        self.redis = redis
        self.ttl = ttl

    async def record_win(self, win: WinRecord) -> None:
        key = _win_key(win.auction_id, win.imp_id)
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.hset(
                key,
                mapping={
                    "bidder": win.bidder,
                    "price": win.price,
                    "creative_id": win.creative_id,
                    "predicted_ctr": win.predicted_ctr,
                    "adomain": win.adomain,
                },
            )
            pipe.expire(key, self.ttl)
            await pipe.execute()

    async def get_win(self, auction_id: str, imp_id: str) -> WinRecord | None:
        raw = await self.redis.hgetall(_win_key(auction_id, imp_id))
        if not raw:
            return None
        d = {
            (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
            for k, v in raw.items()
        }
        return WinRecord(
            auction_id=auction_id,
            imp_id=imp_id,
            bidder=d["bidder"],
            price=float(d["price"]),
            creative_id=d.get("creative_id", ""),
            predicted_ctr=float(d.get("predicted_ctr", 0.0)),
            adomain=d.get("adomain", ""),
        )

    async def first_seen(self, kind: str, auction_id: str, imp_id: str) -> bool:
        """True the first time an (event kind, auction, imp) triple is observed."""
        key = f"seen:{kind}:{auction_id}:{imp_id}"
        return bool(await self.redis.set(key, 1, nx=True, ex=self.ttl))
