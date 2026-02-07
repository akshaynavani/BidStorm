import pytest

from bidstorm.events.publisher import InMemoryPublisher
from bidstorm.events.schema import (
    AuctionEvent,
    BidSummary,
    ClickEvent,
    ImpressionEvent,
    WinSummary,
    decode,
    encode,
    topic_for,
)


def test_roundtrip_all_event_types():
    events = [
        AuctionEvent(
            auction_id="a", site_domain="x.example", n_imps=1, latency_ms=12.5, auction_type=2,
            dsp_status={"d1": "ok"}, bids=[BidSummary(bidder="d1", imp_id="1", price=2.0)],
            winners=[WinSummary(imp_id="1", bidder="d1", bid_price=2.0, clearing_price=1.1)],
        ),
        ImpressionEvent(auction_id="a", imp_id="1", bidder="d1", price=1.1),
        ClickEvent(auction_id="a", imp_id="1", bidder="d1"),
    ]
    for ev in events:
        assert decode(encode(ev)) == ev


def test_topics():
    assert topic_for(ImpressionEvent(auction_id="a", imp_id="1", bidder="d", price=1)) == (
        "impressions"
    )
    assert topic_for(ClickEvent(auction_id="a", imp_id="1", bidder="d")) == "clicks"


def test_unknown_type_rejected():
    with pytest.raises(ValueError):
        decode(b'{"type": "mystery"}')


async def test_in_memory_publisher_collects():
    pub = InMemoryPublisher()
    await pub.publish(ClickEvent(auction_id="a", imp_id="1", bidder="d"))
    assert len(pub.of_type("click")) == 1
