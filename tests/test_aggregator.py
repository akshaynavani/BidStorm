import fakeredis
import pytest

from bidstorm.analytics.aggregator import Aggregator, read_stats
from bidstorm.events.schema import (
    AuctionEvent,
    BidSummary,
    ClickEvent,
    ImpressionEvent,
    WinSummary,
)


@pytest.fixture
def redis():
    return fakeredis.FakeAsyncRedis()


def auction(aid, winner=True):
    return AuctionEvent(
        auction_id=aid,
        site_domain="espn.example",
        n_imps=1,
        latency_ms=20.0,
        auction_type=2,
        dsp_status={"a": "ok", "b": "timeout"},
        bids=[BidSummary(bidder="a", imp_id="1", price=4.0)],
        winners=[WinSummary(imp_id="1", bidder="a", bid_price=4.0, clearing_price=2.0)]
        if winner
        else [],
    )


async def test_rollup_and_kpis(redis):
    agg = Aggregator(redis)
    await agg.apply_batch(
        [
            auction("x1"),
            auction("x2", winner=False),
            ImpressionEvent(auction_id="x1", imp_id="1", bidder="a", price=2.0),
            ClickEvent(auction_id="x1", imp_id="1", bidder="a"),
        ]
    )
    stats = await read_stats(redis)
    g = stats["global"]
    assert g["auctions"] == 2
    assert g["fill_rate"] == 0.5
    assert g["impressions"] == 1
    assert g["ctr"] == 1.0
    assert g["ecpm"] == pytest.approx(2.0)
    assert g["avg_latency_ms"] == pytest.approx(20.0)

    a = stats["bidders"]["a"]
    assert a["bids"] == 2 and a["wins"] == 1
    assert a["win_rate"] == 0.5
    assert a["spend"] == pytest.approx(0.002)
    b = stats["bidders"]["b"]
    assert b["timeout_rate"] == 1.0


async def test_incremental_batches_accumulate(redis):
    agg = Aggregator(redis)
    await agg.apply(auction("1"))
    await agg.apply(auction("2"))
    stats = await read_stats(redis)
    assert stats["global"]["auctions"] == 2
