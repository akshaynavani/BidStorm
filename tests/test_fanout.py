from bidstorm.exchange.fanout import CallStatus, FanOut
from bidstorm.proto import bidding_pb2
from tests.fakes import FakeBidder


def proto_request():
    return bidding_pb2.BidRequest(id="r", imps=[bidding_pb2.Impression(id="1", bid_floor=0.1)])


async def test_collects_fast_responses_and_drops_stragglers():
    fast = FakeBidder("fast", price=2.0, delay=0.001)
    slow = FakeBidder("slow", price=9.0, delay=0.5)
    broken = FakeBidder("broken", fail=True)
    silent = FakeBidder("silent", price=None)
    results = {r.bidder: r for r in await FanOut([fast, slow, broken, silent]).broadcast(
        proto_request(), budget_s=0.05)}

    assert results["fast"].status is CallStatus.OK
    assert results["fast"].response.bids[0].price == 2.0
    assert results["slow"].status is CallStatus.TIMEOUT
    assert results["broken"].status is CallStatus.ERROR
    assert results["silent"].status is CallStatus.NO_BID


async def test_arrival_order_recorded():
    a = FakeBidder("a", price=1.0, delay=0.02)
    b = FakeBidder("b", price=1.0, delay=0.001)
    results = {r.bidder: r for r in await FanOut([a, b]).broadcast(proto_request(), 0.1)}
    assert results["b"].arrival_seq < results["a"].arrival_seq


async def test_exhausted_budget_skips_calls():
    a = FakeBidder("a", price=1.0)
    [res] = await FanOut([a]).broadcast(proto_request(), budget_s=0)
    assert res.status is CallStatus.TIMEOUT
    assert a.calls == 0
