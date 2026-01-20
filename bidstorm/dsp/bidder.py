"""DSP bidding strategy: value an impression from pCTR, apply targeting and pacing.

Pure logic with injectable clock / RNG so it can be tested without gRPC.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from bidstorm.ml.features import ImpressionContext

DAY_SECONDS = 86_400.0


class CTRPredictor(Protocol):
    def predict(self, ctx: ImpressionContext) -> float: ...


@dataclass(slots=True)
class Opportunity:
    imp_id: str
    floor: float
    context: ImpressionContext


@dataclass(slots=True)
class BidDecision:
    imp_id: str
    price: float  # CPM
    predicted_ctr: float
    creative_id: str


@dataclass
class BudgetPacer:
    """Throttle-based pacing toward an even spend across the day.

    Participation probability is nudged down when spend runs ahead of the linear
    schedule and back up when it lags. Thread-safe because gRPC handlers and win
    notifications may race.
    """

    daily_budget: float
    clock: Callable[[], float] = time.monotonic
    slack: float = 0.05
    step: float = 0.1
    min_rate: float = 0.02
    spent: float = 0.0
    rate: float = 1.0
    _start: float = field(init=False)
    _lock: threading.Lock = field(init=False, repr=False, default_factory=threading.Lock)

    def __post_init__(self) -> None:
        self._start = self.clock()

    @property
    def exhausted(self) -> bool:
        return self.spent >= self.daily_budget

    def expected_fraction(self) -> float:
        elapsed = (self.clock() - self._start) % DAY_SECONDS
        return elapsed / DAY_SECONDS

    def record_spend(self, amount: float) -> None:
        with self._lock:
            self.spent += max(0.0, amount)
            self._adjust()

    def _adjust(self) -> None:
        if self.daily_budget <= 0:
            self.rate = 0.0
            return
        actual = self.spent / self.daily_budget
        expected = self.expected_fraction()
        if actual > expected + self.slack:
            self.rate = max(self.min_rate, self.rate * (1.0 - self.step))
        elif actual < expected:
            self.rate = min(1.0, self.rate * (1.0 + self.step))

    def should_participate(self, rng: random.Random) -> bool:
        if self.exhausted:
            return False
        return rng.random() < self.rate


@dataclass
class Bidder:
    name: str
    ctr_model: CTRPredictor
    value_per_click: float
    pacer: BudgetPacer
    target_segments: set[str] = field(default_factory=set)
    max_cpm: float = 25.0
    targeting_boost: float = 1.25
    first_price_shade: float = 0.80
    rng: random.Random = field(default_factory=random.Random)

    def value_cpm(self, ctx: ImpressionContext) -> tuple[float, float]:
        """Return (CPM valuation, pCTR) for an impression."""
        pctr = self.ctr_model.predict(ctx)
        cpm = pctr * self.value_per_click * 1000.0
        if self.target_segments and self.target_segments.intersection(ctx.segments):
            cpm *= self.targeting_boost
        return cpm, pctr

    def decide(self, opportunities: list[Opportunity], auction_type: int) -> list[BidDecision]:
        if not self.pacer.should_participate(self.rng):
            return []
        decisions: list[BidDecision] = []
        for opp in opportunities:
            cpm, pctr = self.value_cpm(opp.context)
            # Truthful bidding is optimal in second-price auctions; shade in first-price.
            if auction_type == 1:
                cpm *= self.first_price_shade
            price = round(min(cpm, self.max_cpm), 4)
            if price < opp.floor or price <= 0:
                continue
            decisions.append(
                BidDecision(
                    imp_id=opp.imp_id,
                    price=price,
                    predicted_ctr=pctr,
                    creative_id=f"{self.name}-cr-{opp.context.width}x{opp.context.height}",
                )
            )
        return decisions

    def on_win(self, clearing_cpm: float) -> None:
        self.pacer.record_spend(clearing_cpm / 1000.0)
