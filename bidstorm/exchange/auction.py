"""Auction engine: per-impression first-price / second-price clearing with floors.

The engine is deliberately pure (no I/O) so it can be unit tested exhaustively and
reasoned about independently from the network fan-out.
"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field

from bidstorm.models import AuctionType


@dataclass(frozen=True, slots=True)
class CandidateBid:
    bidder: str
    imp_id: str
    price: float  # CPM
    creative_id: str = ""
    adomain: str = ""
    adm: str = ""
    predicted_ctr: float = 0.0
    arrival_seq: int = 0  # lower = arrived earlier; used for tie-breaking


@dataclass(slots=True)
class ImpOutcome:
    imp_id: str
    floor: float
    winner: CandidateBid | None = None
    clearing_price: float = 0.0
    eligible_bids: int = 0
    rejected_below_floor: int = 0
    runner_up_price: float | None = None

    @property
    def filled(self) -> bool:
        return self.winner is not None


@dataclass(slots=True)
class AuctionResult:
    request_id: str
    auction_type: AuctionType
    outcomes: dict[str, ImpOutcome] = field(default_factory=dict)

    def winners(self) -> list[ImpOutcome]:
        return [o for o in self.outcomes.values() if o.filled]


class AuctionEngine:
    def __init__(self, bid_increment: float = 0.01, rng: random.Random | None = None) -> None:
        if bid_increment < 0:
            raise ValueError("bid_increment must be non-negative")
        self.bid_increment = bid_increment
        self._rng = rng or random.Random()

    def run(
        self,
        request_id: str,
        floors: dict[str, float],
        bids: list[CandidateBid],
        auction_type: AuctionType = AuctionType.SECOND_PRICE,
    ) -> AuctionResult:
        """Clear every impression in `floors` against the submitted `bids`.

        Bids for unknown impressions, non-positive or non-finite prices are discarded.
        Each bidder may only win with its single best bid per impression.
        """
        result = AuctionResult(request_id=request_id, auction_type=auction_type)
        by_imp: dict[str, dict[str, CandidateBid]] = defaultdict(dict)
        rejected: dict[str, int] = defaultdict(int)

        for bid in bids:
            if bid.imp_id not in floors:
                continue
            if not (bid.price > 0 and bid.price != float("inf")):
                continue
            if bid.price < floors[bid.imp_id]:
                rejected[bid.imp_id] += 1
                continue
            # Keep only each bidder's best bid per impression.
            current = by_imp[bid.imp_id].get(bid.bidder)
            if current is None or self._beats(bid, current):
                by_imp[bid.imp_id][bid.bidder] = bid

        for imp_id, floor in floors.items():
            outcome = ImpOutcome(imp_id=imp_id, floor=floor, rejected_below_floor=rejected[imp_id])
            ranked = self._rank(list(by_imp.get(imp_id, {}).values()))
            outcome.eligible_bids = len(ranked)
            if ranked:
                winner = ranked[0]
                outcome.winner = winner
                outcome.runner_up_price = ranked[1].price if len(ranked) > 1 else None
                outcome.clearing_price = self._clearing_price(
                    winner.price, outcome.runner_up_price, floor, auction_type
                )
            result.outcomes[imp_id] = outcome
        return result

    @staticmethod
    def _beats(a: CandidateBid, b: CandidateBid) -> bool:
        if a.price != b.price:
            return a.price > b.price
        return a.arrival_seq < b.arrival_seq

    def _rank(self, bids: list[CandidateBid]) -> list[CandidateBid]:
        # Random secondary key makes ties at identical price and arrival fair.
        keyed = [(-b.price, b.arrival_seq, self._rng.random(), b) for b in bids]
        keyed.sort(key=lambda t: t[:3])
        return [t[3] for t in keyed]

    def _clearing_price(
        self,
        winning: float,
        runner_up: float | None,
        floor: float,
        auction_type: AuctionType,
    ) -> float:
        if auction_type == AuctionType.FIRST_PRICE:
            return round(winning, 4)
        # Second price: pay max(runner-up, floor) plus one increment, never above own bid.
        reference = max(runner_up if runner_up is not None else floor, floor)
        price = min(winning, reference + self.bid_increment)
        return round(price, 4)
