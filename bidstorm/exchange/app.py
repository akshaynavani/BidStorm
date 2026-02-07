"""FastAPI front door for the exchange.

Run with:
    uvicorn bidstorm.exchange.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import base64
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import ORJSONResponse, RedirectResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from bidstorm.analytics.aggregator import read_stats
from bidstorm.config import exchange_settings, kafka_settings, redis_settings
from bidstorm.events.publisher import build_publisher
from bidstorm.events.schema import ClickEvent, ImpressionEvent
from bidstorm.exchange.dmp import DMPClient
from bidstorm.exchange.registry import DSPRegistry
from bidstorm.exchange.service import ExchangeService
from bidstorm.models import BidRequest, BidResponse

log = logging.getLogger("bidstorm.exchange")

# 1x1 transparent GIF returned by the impression pixel.
PIXEL = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
PIXEL_HEADERS = {"Cache-Control": "no-store, max-age=0"}


async def build_default_service() -> ExchangeService:
    settings = exchange_settings()
    rs = redis_settings()
    redis = Redis.from_url(rs.redis_url)
    registry = DSPRegistry.from_endpoints(settings.endpoints())
    publisher = build_publisher(kafka_settings())
    return ExchangeService(
        settings, DMPClient(redis, rs.dmp_timeout_ms), registry, publisher=publisher
    )


def create_app(service: ExchangeService | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        svc = service or await build_default_service()
        app.state.service = svc
        await svc.publisher.start()
        log.info("exchange ready with %d DSPs", len(svc.registry))
        try:
            yield
        finally:
            await svc.drain()
            await svc.publisher.stop()
            if service is None:
                await svc.registry.close()
                await svc.dmp.redis.aclose()

    app = FastAPI(title="BidStorm Exchange", version="0.1.0", lifespan=lifespan,
                  default_response_class=ORJSONResponse)

    @app.post("/openrtb2/auction", response_model=BidResponse, response_model_exclude_none=True)
    async def auction(bid_request: BidRequest, request: Request):
        svc: ExchangeService = request.app.state.service
        resp = await svc.handle(bid_request)
        if not resp.seatbid:
            # OpenRTB convention: HTTP 204 for no-bid.
            return Response(status_code=204, headers={"X-NBR": str(resp.nbr or 0)})
        return resp

    @app.get("/healthz")
    async def healthz(request: Request):
        svc: ExchangeService = request.app.state.service
        return {"status": "ok", "dsps": [c.name for c in svc.registry.all()]}

    @app.get("/metrics")
    async def metrics(request: Request):
        svc: ExchangeService = request.app.state.service
        return Response(generate_latest(svc.metrics.registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/track/imp")
    async def track_impression(
        request: Request, auction: str = Query(...), imp: str = Query(...)
    ):
        svc: ExchangeService = request.app.state.service
        win = await svc.tracking.get_win(auction, imp)
        if win is not None and await svc.tracking.first_seen("imp", auction, imp):
            await svc.publisher.publish(
                ImpressionEvent(
                    auction_id=auction,
                    imp_id=imp,
                    bidder=win.bidder,
                    price=win.price,
                    creative_id=win.creative_id,
                ),
                key=auction,
            )
            svc.metrics.events_published.labels("impressions").inc()
        return Response(PIXEL, media_type="image/gif", headers=PIXEL_HEADERS)

    @app.get("/track/click")
    async def track_click(request: Request, auction: str = Query(...), imp: str = Query(...)):
        svc: ExchangeService = request.app.state.service
        win = await svc.tracking.get_win(auction, imp)
        if win is None:
            return Response(status_code=404)
        if await svc.tracking.first_seen("click", auction, imp):
            await svc.publisher.publish(
                ClickEvent(
                    auction_id=auction,
                    imp_id=imp,
                    bidder=win.bidder,
                    creative_id=win.creative_id,
                    predicted_ctr=win.predicted_ctr,
                ),
                key=auction,
            )
            svc.metrics.events_published.labels("clicks").inc()
        landing = f"https://{win.adomain}/" if win.adomain else "https://example.com/"
        return RedirectResponse(landing, status_code=302)

    @app.get("/stats")
    async def stats(request: Request):
        svc: ExchangeService = request.app.state.service
        data = await read_stats(svc.tracking.redis)
        data["dmp"] = svc.dmp.stats()
        return data

    return app


app = create_app()
