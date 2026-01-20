"""Async gRPC DSP microservice.

Each instance represents one demand-side platform with its own valuation, budget and
targeting, configured through `DSP_*` environment variables. An artificial latency
(with jitter) simulates real network/compute time so the exchange's deadline handling
is exercised; a small fraction of requests are deliberately slow to produce timeouts.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import random
import signal
import time

import grpc

from bidstorm.config import DSPSettings
from bidstorm.dsp.bidder import Bidder, BudgetPacer, Opportunity
from bidstorm.dsp.ctr_model import load_or_fallback
from bidstorm.ml.features import ImpressionContext
from bidstorm.proto import bidding_pb2, bidding_pb2_grpc

log = logging.getLogger("bidstorm.dsp")

SLOW_TAIL_PROBABILITY = 0.02
SLOW_TAIL_MS = 150.0


def to_opportunities(req: bidding_pb2.BidRequest) -> list[Opportunity]:
    hour = dt.datetime.fromtimestamp(
        (req.timestamp_ms or int(time.time() * 1000)) / 1000, tz=dt.timezone.utc
    ).hour
    base = dict(
        site_domain=req.site_domain,
        site_categories=list(req.site_categories),
        device_type=req.device_type,
        os=req.os,
        country=req.country,
        segments=list(req.user_segments),
        hour=hour,
    )
    return [
        Opportunity(
            imp_id=imp.id,
            floor=imp.bid_floor,
            context=ImpressionContext(
                width=imp.width, height=imp.height, position=imp.position, **base
            ),
        )
        for imp in req.imps
    ]


class BidderService(bidding_pb2_grpc.BidderServicer):
    def __init__(self, bidder: Bidder, latency_ms: float, jitter_ms: float) -> None:
        self.bidder = bidder
        self.latency_ms = latency_ms
        self.jitter_ms = jitter_ms
        self._rng = random.Random()

    def _simulated_delay(self) -> float:
        if self._rng.random() < SLOW_TAIL_PROBABILITY:
            return SLOW_TAIL_MS / 1000.0
        return max(0.0, self.latency_ms + self._rng.uniform(0, self.jitter_ms)) / 1000.0

    async def Bid(self, request, context):  # noqa: N802 (gRPC naming)
        started = time.perf_counter()
        delay = self._simulated_delay()
        remaining = context.time_remaining()
        # Don't bother sleeping past the caller's deadline: the response would be dropped.
        if remaining is not None and delay >= remaining:
            await asyncio.sleep(max(0.0, remaining))
            return bidding_pb2.BidResponse(request_id=request.id, bidder=self.bidder.name)
        if delay:
            await asyncio.sleep(delay)

        decisions = self.bidder.decide(to_opportunities(request), request.auction_type)
        elapsed_us = int((time.perf_counter() - started) * 1e6)
        return bidding_pb2.BidResponse(
            request_id=request.id,
            bidder=self.bidder.name,
            processing_time_us=elapsed_us,
            bids=[
                bidding_pb2.Bid(
                    imp_id=d.imp_id,
                    price=d.price,
                    creative_id=d.creative_id,
                    adomain=f"{self.bidder.name}.example",
                    adm=f'<a href="https://{self.bidder.name}.example"><img src="https://cdn.'
                    f'{self.bidder.name}.example/{d.creative_id}.png"/></a>',
                    predicted_ctr=d.predicted_ctr,
                )
                for d in decisions
            ],
        )

    async def NotifyWin(self, request, context):  # noqa: N802
        self.bidder.on_win(request.clearing_price)
        log.debug(
            "won %s/%s at %.4f CPM (spent %.2f / %.2f)",
            request.request_id,
            request.imp_id,
            request.clearing_price,
            self.bidder.pacer.spent,
            self.bidder.pacer.daily_budget,
        )
        return bidding_pb2.Ack(ok=True)


def build_bidder(settings: DSPSettings) -> Bidder:
    return Bidder(
        name=settings.name,
        ctr_model=load_or_fallback(settings.model_path),
        value_per_click=settings.value_per_click,
        pacer=BudgetPacer(daily_budget=settings.daily_budget),
        target_segments=settings.segments(),
        max_cpm=settings.max_cpm,
    )


async def serve(settings: DSPSettings) -> None:
    server = grpc.aio.server(
        options=[
            ("grpc.max_concurrent_streams", 1000),
            ("grpc.keepalive_time_ms", 30_000),
        ]
    )
    service = BidderService(build_bidder(settings), settings.latency_ms, settings.latency_jitter_ms)
    bidding_pb2_grpc.add_BidderServicer_to_server(service, server)
    server.add_insecure_port(f"[::]:{settings.port}")
    await server.start()
    log.info("DSP %s listening on :%d", settings.name, settings.port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    await stop.wait()
    await server.stop(grace=2)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(serve(DSPSettings()))


if __name__ == "__main__":
    main()
