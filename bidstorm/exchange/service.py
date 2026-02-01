"""Exchange orchestration: enrich -> fan out -> auction -> respond.

The HTTP layer is a thin shell around `ExchangeService.handle`, which owns the latency
budget for a single auction.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from bidstorm.config import ExchangeSettings
from bidstorm.events.publisher import EventPublisher, NullPublisher
from bidstorm.events.schema import AuctionEvent, BidSummary, WinSummary
from bidstorm.exchange.auction import AuctionEngine, AuctionResult, CandidateBid
from bidstorm.exchange.dmp import DMPClient
from bidstorm.exchange.fanout import CallStatus, DSPResult, FanOut
from bidstorm.exchange.metrics import ExchangeMetrics
from bidstorm.exchange.registry import DSPRegistry
from bidstorm.exchange.tracking import TrackingStore, WinRecord
from bidstorm.models import Bid, BidRequest, BidResponse, NoBidReason, SeatBid
from bidstorm.proto import bidding_pb2

log = logging.getLogger(__name__)


@dataclass(slots=True)
class AuctionTrace:
    """Everything we learned during one auction; used for metrics and analytics."""

    request: BidRequest
    segments: list[str]
    dsp_results: list[DSPResult]
    result: AuctionResult
    latency_ms: float


def to_proto(req: BidRequest, segments: list[str], tmax_ms: int, floors: dict[str, float]):
    geo = req.device.geo
    return bidding_pb2.BidRequest(
        id=req.id,
        imps=[
            bidding_pb2.Impression(
                id=imp.id,
                width=imp.banner.w,
                height=imp.banner.h,
                position=imp.banner.pos,
                bid_floor=floors[imp.id],
                tag_id=imp.tagid or "",
            )
            for imp in req.imp
        ],
        site_domain=req.site.domain,
        site_categories=req.site.cat,
        publisher_id=req.site.publisher.id if req.site.publisher else "",
        device_type=req.device.devicetype,
        os=req.device.os or "",
        country=(geo.country if geo and geo.country else ""),
        user_id=req.user.id or req.user.buyeruid or "",
        user_segments=segments,
        auction_type=int(req.at),
        tmax_ms=tmax_ms,
        timestamp_ms=int(time.time() * 1000),
    )


class ExchangeService:
    def __init__(
        self,
        settings: ExchangeSettings,
        dmp: DMPClient,
        registry: DSPRegistry,
        metrics: ExchangeMetrics | None = None,
        engine: AuctionEngine | None = None,
        publisher: EventPublisher | None = None,
        tracking: TrackingStore | None = None,
    ) -> None:
        self.settings = settings
        self.dmp = dmp
        self.registry = registry
        self.fanout = FanOut(registry.all())
        self.metrics = metrics or ExchangeMetrics()
        self.engine = engine or AuctionEngine(bid_increment=settings.bid_increment)
        self.publisher = publisher or NullPublisher()
        self.tracking = tracking or TrackingStore(dmp.redis)
        self.metrics.registered_dsps.set(len(registry))
        self._background: set[asyncio.Task] = set()

    # ------------------------------------------------------------------ helpers
    def effective_tmax_ms(self, req: BidRequest) -> int:
        if req.tmax and req.tmax > 0:
            return min(req.tmax, self.settings.tmax_ms)
        return self.settings.tmax_ms

    def floors(self, req: BidRequest) -> dict[str, float]:
        return {imp.id: max(imp.bidfloor, self.settings.default_floor) for imp in req.imp}

    async def _enrich(self, req: BidRequest) -> list[str]:
        if req.user.segments:
            self.metrics.dmp_lookups.labels("client_supplied").inc()
            return list(req.user.segments)
        before = self.dmp.stats()
        segments = await self.dmp.segments(req.user.id or req.user.buyeruid)
        after = self.dmp.stats()
        for key in ("hits", "misses", "timeouts", "errors"):
            if after[key] > before[key]:
                self.metrics.dmp_lookups.labels(key).inc()
        return segments

    def spawn(self, coro) -> None:
        """Run a coroutine in the background, keeping a reference so it isn't GC'd."""
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _notify_win(self, bidder: str, notice: bidding_pb2.WinNotice) -> None:
        client = self.registry.get(bidder)
        if client is None:
            return
        try:
            await client.notify_win(notice, timeout=0.5)
        except Exception as exc:  # noqa: BLE001 - best effort
            log.debug("win notice to %s failed: %r", bidder, exc)

    # ------------------------------------------------------------------ main path
    async def run_auction(self, req: BidRequest) -> AuctionTrace:
        started = time.perf_counter()
        tmax_ms = self.effective_tmax_ms(req)
        floors = self.floors(req)

        segments = await self._enrich(req)
        elapsed_s = time.perf_counter() - started
        budget_s = (tmax_ms - self.settings.overhead_ms) / 1000.0 - elapsed_s

        proto = to_proto(req, segments, tmax_ms, floors)
        dsp_results = await self.fanout.broadcast(proto, budget_s)

        candidates: list[CandidateBid] = []
        for res in dsp_results:
            self.metrics.dsp_calls.labels(res.bidder, res.status.value).inc()
            self.metrics.dsp_latency.labels(res.bidder).observe(res.latency_ms)
            if res.status is not CallStatus.OK or res.response is None:
                continue
            for b in res.response.bids:
                candidates.append(
                    CandidateBid(
                        bidder=res.bidder,
                        imp_id=b.imp_id,
                        price=b.price,
                        creative_id=b.creative_id,
                        adomain=b.adomain,
                        adm=b.adm,
                        predicted_ctr=b.predicted_ctr,
                        arrival_seq=res.arrival_seq,
                    )
                )

        result = self.engine.run(req.id, floors, candidates, auction_type=req.at)
        for outcome in result.winners():
            self.metrics.impressions_won.labels(outcome.winner.bidder).inc()
            self.metrics.clearing_price.observe(outcome.clearing_price)
            self.spawn(
                self._notify_win(
                    outcome.winner.bidder,
                    bidding_pb2.WinNotice(
                        request_id=req.id,
                        imp_id=outcome.imp_id,
                        clearing_price=outcome.clearing_price,
                        creative_id=outcome.winner.creative_id,
                    ),
                )
            )

        latency_ms = (time.perf_counter() - started) * 1000.0
        self.metrics.auction_latency.observe(latency_ms)
        self.metrics.requests.labels("filled" if result.winners() else "no_fill").inc()
        trace = AuctionTrace(req, segments, dsp_results, result, latency_ms)
        self.spawn(self._after_auction(trace, candidates))
        return trace

    async def _after_auction(self, trace: AuctionTrace, candidates: list[CandidateBid]) -> None:
        """Off the hot path: persist wins for tracking and emit the analytics event."""
        winners = trace.result.winners()
        for outcome in winners:
            w = outcome.winner
            try:
                await self.tracking.record_win(
                    WinRecord(
                        auction_id=trace.request.id,
                        imp_id=outcome.imp_id,
                        bidder=w.bidder,
                        price=outcome.clearing_price,
                        creative_id=w.creative_id,
                        predicted_ctr=w.predicted_ctr,
                        adomain=w.adomain,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("failed to record win for %s: %r", trace.request.id, exc)

        event = AuctionEvent(
            auction_id=trace.request.id,
            site_domain=trace.request.site.domain,
            segments=trace.segments,
            n_imps=len(trace.request.imp),
            latency_ms=round(trace.latency_ms, 3),
            auction_type=int(trace.request.at),
            dsp_status={r.bidder: r.status.value for r in trace.dsp_results},
            bids=[BidSummary(bidder=c.bidder, imp_id=c.imp_id, price=c.price) for c in candidates],
            winners=[
                WinSummary(
                    imp_id=o.imp_id,
                    bidder=o.winner.bidder,
                    bid_price=o.winner.price,
                    clearing_price=o.clearing_price,
                    predicted_ctr=o.winner.predicted_ctr,
                )
                for o in winners
            ],
        )
        await self.publisher.publish(event, key=trace.request.id)
        self.metrics.events_published.labels("auctions").inc()

    def build_response(self, trace: AuctionTrace) -> BidResponse:
        seats: dict[str, list[Bid]] = {}
        for outcome in trace.result.winners():
            w = outcome.winner
            seats.setdefault(w.bidder, []).append(
                Bid(
                    id=f"{trace.request.id}-{outcome.imp_id}",
                    impid=outcome.imp_id,
                    price=outcome.clearing_price,
                    adm=self.decorate_adm(w.adm, trace.request.id, outcome.imp_id),
                    adomain=[w.adomain] if w.adomain else [],
                    crid=w.creative_id,
                    nurl=self.impression_url(trace.request.id, outcome.imp_id),
                )
            )
        if not seats:
            return BidResponse(id=trace.request.id, nbr=int(NoBidReason.UNKNOWN))
        return BidResponse(
            id=trace.request.id,
            seatbid=[SeatBid(seat=bidder, bid=bids) for bidder, bids in seats.items()],
        )

    def impression_url(self, request_id: str, imp_id: str) -> str:
        return f"{self.settings.public_url}/track/imp?auction={request_id}&imp={imp_id}"

    def click_url(self, request_id: str, imp_id: str) -> str:
        return f"{self.settings.public_url}/track/click?auction={request_id}&imp={imp_id}"

    def decorate_adm(self, adm: str, request_id: str, imp_id: str) -> str:
        """Wrap creative markup with the exchange's click redirect and impression pixel."""
        imp_px = self.impression_url(request_id, imp_id)
        click = self.click_url(request_id, imp_id)
        return (
            f'<div class="bidstorm-ad"><a href="{click}" target="_blank">{adm or ""}</a>'
            f'<img src="{imp_px}" width="1" height="1" style="display:none"/></div>'
        )

    async def handle(self, req: BidRequest) -> BidResponse:
        trace = await self.run_auction(req)
        return self.build_response(trace)

    async def drain(self) -> None:
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
