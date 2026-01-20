import random

import pytest

from bidstorm.dsp.bidder import DAY_SECONDS, Bidder, BudgetPacer, Opportunity
from bidstorm.ml.features import ImpressionContext


class FixedCTR:
    def __init__(self, ctr):
        self.ctr = ctr

    def predict(self, ctx):
        return self.ctr


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def make_bidder(ctr=0.01, vpc=1.0, budget=100.0, targets=(), max_cpm=25.0):
    pacer = BudgetPacer(daily_budget=budget, clock=FakeClock())
    return Bidder(
        name="t",
        ctr_model=FixedCTR(ctr),
        value_per_click=vpc,
        pacer=pacer,
        target_segments=set(targets),
        max_cpm=max_cpm,
        rng=random.Random(0),
    )


def opp(floor=0.0, segments=()):
    return Opportunity("1", floor, ImpressionContext(segments=list(segments)))


def test_cpm_is_ctr_times_value_times_1000():
    b = make_bidder(ctr=0.01, vpc=1.5)
    [d] = b.decide([opp()], auction_type=2)
    assert d.price == pytest.approx(15.0)
    assert d.predicted_ctr == pytest.approx(0.01)


def test_first_price_bids_are_shaded():
    b = make_bidder(ctr=0.01, vpc=1.0)
    [d] = b.decide([opp()], auction_type=1)
    assert d.price == pytest.approx(8.0)


def test_targeted_segments_get_boost():
    b = make_bidder(ctr=0.01, vpc=1.0, targets=["sports"])
    [d] = b.decide([opp(segments=["sports"])], auction_type=2)
    assert d.price == pytest.approx(12.5)


def test_price_capped_and_floor_respected():
    b = make_bidder(ctr=0.2, vpc=1.0, max_cpm=5.0)
    [d] = b.decide([opp()], auction_type=2)
    assert d.price == 5.0
    assert b.decide([opp(floor=6.0)], auction_type=2) == []


def test_exhausted_budget_stops_bidding():
    b = make_bidder(budget=1.0)
    b.on_win(1000.0)  # $1 spend
    assert b.pacer.exhausted
    assert b.decide([opp()], auction_type=2) == []


def test_pacer_throttles_when_ahead_of_schedule():
    clock = FakeClock()
    pacer = BudgetPacer(daily_budget=100.0, clock=clock)
    clock.t = DAY_SECONDS * 0.1  # 10% of day elapsed
    for _ in range(5):
        pacer.record_spend(10.0)  # 50% spent: way ahead
    assert pacer.rate < 1.0


def test_pacer_recovers_when_behind():
    clock = FakeClock()
    pacer = BudgetPacer(daily_budget=100.0, clock=clock, rate=0.5)
    clock.t = DAY_SECONDS * 0.5
    pacer.record_spend(1.0)
    assert pacer.rate > 0.5
