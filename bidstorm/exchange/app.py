"""FastAPI front door for the exchange.

Run with:
    uvicorn bidstorm.exchange.app:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import ORJSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from bidstorm.config import exchange_settings, redis_settings
from bidstorm.exchange.dmp import DMPClient
from bidstorm.exchange.registry import DSPRegistry
from bidstorm.exchange.service import ExchangeService
from bidstorm.models import BidRequest, BidResponse

log = logging.getLogger("bidstorm.exchange")


async def build_default_service() -> ExchangeService:
    settings = exchange_settings()
    rs = redis_settings()
    redis = Redis.from_url(rs.redis_url)
    registry = DSPRegistry.from_endpoints(settings.endpoints())
    return ExchangeService(settings, DMPClient(redis, rs.dmp_timeout_ms), registry)


def create_app(service: ExchangeService | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        svc = service or await build_default_service()
        app.state.service = svc
        log.info("exchange ready with %d DSPs", len(svc.registry))
        try:
            yield
        finally:
            await svc.drain()
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

    return app


app = create_app()
