"""Roll analytics events up into Redis counters.

Layout:
    stats:global                 hash  auctions, filled_auctions, imps_offered, bids,
                                       wins, impressions, clicks, spend, latency_ms_sum
    stats:bidder:<name>          hash  calls, timeouts, errors, bids, wins, impressions,
                                       clicks, spend, bid_price_sum
    stats:minute:<epoch_minute>  hash  auctions, impressions, clicks, spend  (TTL 2h)
    stats:bidders                set   known bidder names
"""

from __future__ import annotations

from collections import defaultdict

from redis.asyncio import Redis

from bidstorm.events.schema import AuctionEvent, ClickEvent, Event, ImpressionEvent

GLOBAL = "stats:global"
BIDDERS = "stats:bidders"
MINUTE_TTL = 2 * 3600


def bidder_key(name: str) -> str:
    return f"stats:bidder:{name}"


def minute_key(ts_ms: int) -> str:
    return f"stats:minute:{ts_ms // 60_000}"


class Aggregator:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def apply(self, event: Event) -> None:
        await self.apply_batch([event])

    async def apply_batch(self, events: list[Event]) -> None:
        """Fold a batch of events into one pipelined round trip."""
        g: dict[str, float] = defaultdict(float)
        per_bidder: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        per_minute: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

        for ev in events:
            m = per_minute[minute_key(ev.ts)]
            if isinstance(ev, AuctionEvent):
                g["auctions"] += 1
                g["imps_offered"] += ev.n_imps
                g["bids"] += len(ev.bids)
                g["latency_ms_sum"] += ev.latency_ms
                m["auctions"] += 1
                if ev.winners:
                    g["filled_auctions"] += 1
                g["wins"] += len(ev.winners)
                for bidder, status in ev.dsp_status.items():
                    b = per_bidder[bidder]
                    b["calls"] += 1
                    if status == "timeout":
                        b["timeouts"] += 1
                    elif status == "error":
                        b["errors"] += 1
                for bid in ev.bids:
                    per_bidder[bid.bidder]["bids"] += 1
                    per_bidder[bid.bidder]["bid_price_sum"] += bid.price
                for w in ev.winners:
                    per_bidder[w.bidder]["wins"] += 1
            elif isinstance(ev, ImpressionEvent):
                spend = ev.price / 1000.0
                g["impressions"] += 1
                g["spend"] += spend
                per_bidder[ev.bidder]["impressions"] += 1
                per_bidder[ev.bidder]["spend"] += spend
                m["impressions"] += 1
                m["spend"] += spend
            elif isinstance(ev, ClickEvent):
                g["clicks"] += 1
                per_bidder[ev.bidder]["clicks"] += 1
                m["clicks"] += 1

        async with self.redis.pipeline(transaction=False) as pipe:
            for field, value in g.items():
                pipe.hincrbyfloat(GLOBAL, field, value)
            if per_bidder:
                pipe.sadd(BIDDERS, *per_bidder.keys())
            for name, fields in per_bidder.items():
                for field, value in fields.items():
                    pipe.hincrbyfloat(bidder_key(name), field, value)
            for key, fields in per_minute.items():
                for field, value in fields.items():
                    pipe.hincrbyfloat(key, field, value)
                pipe.expire(key, MINUTE_TTL)
            await pipe.execute()


def _decode_hash(raw: dict) -> dict[str, float]:
    return {
        (k.decode() if isinstance(k, bytes) else k): float(v) for k, v in raw.items()
    }


def _ratio(num: float, den: float) -> float:
    return round(num / den, 6) if den else 0.0


async def read_stats(redis: Redis) -> dict:
    """Return raw counters plus derived KPIs (fill rate, win rate, CTR, eCPM)."""
    g = _decode_hash(await redis.hgetall(GLOBAL))
    names = sorted(
        (n.decode() if isinstance(n, bytes) else n) for n in await redis.smembers(BIDDERS)
    )
    bidders = {}
    for name in names:
        b = _decode_hash(await redis.hgetall(bidder_key(name)))
        bidders[name] = {
            **b,
            "timeout_rate": _ratio(b.get("timeouts", 0), b.get("calls", 0)),
            "win_rate": _ratio(b.get("wins", 0), b.get("bids", 0)),
            "ctr": _ratio(b.get("clicks", 0), b.get("impressions", 0)),
            "ecpm": _ratio(b.get("spend", 0) * 1000, b.get("impressions", 0)),
            "avg_bid_cpm": _ratio(b.get("bid_price_sum", 0), b.get("bids", 0)),
        }
    return {
        "global": {
            **g,
            "fill_rate": _ratio(g.get("filled_auctions", 0), g.get("auctions", 0)),
            "ctr": _ratio(g.get("clicks", 0), g.get("impressions", 0)),
            "ecpm": _ratio(g.get("spend", 0) * 1000, g.get("impressions", 0)),
            "avg_latency_ms": _ratio(g.get("latency_ms_sum", 0), g.get("auctions", 0)),
        },
        "bidders": bidders,
    }
