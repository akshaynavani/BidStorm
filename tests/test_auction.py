import random

import pytest

from bidstorm.exchange.auction import AuctionEngine, CandidateBid
from bidstorm.models import AuctionType


def bid(bidder, price, imp="1", seq=0):
    return CandidateBid(bidder=bidder, imp_id=imp, price=price, arrival_seq=seq)


@pytest.fixture
def engine():
    return AuctionEngine(bid_increment=0.01, rng=random.Random(7))


def test_second_price_pays_runner_up_plus_increment(engine):
    res = engine.run("r", {"1": 0.5}, [bid("a", 3.0), bid("b", 2.0), bid("c", 1.0)])
    out = res.outcomes["1"]
    assert out.winner.bidder == "a"
    assert out.clearing_price == pytest.approx(2.01)
    assert out.eligible_bids == 3


def test_second_price_single_bidder_pays_floor_plus_increment(engine):
    res = engine.run("r", {"1": 1.0}, [bid("a", 4.0)])
    assert res.outcomes["1"].clearing_price == pytest.approx(1.01)


def test_second_price_never_exceeds_winning_bid(engine):
    res = engine.run("r", {"1": 0.0}, [bid("a", 2.0), bid("b", 2.0, seq=1)])
    out = res.outcomes["1"]
    assert out.winner.bidder == "a"  # earlier arrival wins the tie
    assert out.clearing_price == pytest.approx(2.0)


def test_first_price_pays_own_bid(engine):
    res = engine.run(
        "r", {"1": 0.5}, [bid("a", 3.0), bid("b", 2.0)], auction_type=AuctionType.FIRST_PRICE
    )
    assert res.outcomes["1"].clearing_price == pytest.approx(3.0)


def test_bids_below_floor_are_rejected(engine):
    res = engine.run("r", {"1": 2.5}, [bid("a", 2.0), bid("b", 1.0)])
    out = res.outcomes["1"]
    assert not out.filled
    assert out.rejected_below_floor == 2


def test_unknown_imp_and_bad_prices_ignored(engine):
    res = engine.run(
        "r",
        {"1": 0.0},
        [bid("a", 1.0, imp="nope"), bid("b", -1.0), bid("c", float("nan")), bid("d", float("inf"))],
    )
    assert not res.outcomes["1"].filled
    assert res.outcomes["1"].eligible_bids == 0


def test_bidder_best_bid_only_counts_once(engine):
    res = engine.run("r", {"1": 0.0}, [bid("a", 5.0), bid("a", 4.0), bid("b", 1.0)])
    out = res.outcomes["1"]
    assert out.eligible_bids == 2
    assert out.runner_up_price == pytest.approx(1.0)
    assert out.clearing_price == pytest.approx(1.01)


def test_multi_impression_auctions_are_independent(engine):
    res = engine.run(
        "r",
        {"1": 0.0, "2": 0.0},
        [bid("a", 5.0, imp="1"), bid("b", 3.0, imp="2"), bid("a", 1.0, imp="2")],
    )
    assert res.outcomes["1"].winner.bidder == "a"
    assert res.outcomes["2"].winner.bidder == "b"
    assert len(res.winners()) == 2


def test_negative_increment_rejected():
    with pytest.raises(ValueError):
        AuctionEngine(bid_increment=-0.1)
