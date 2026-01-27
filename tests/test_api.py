import fakeredis
import pytest
from fastapi.testclient import TestClient

from bidstorm.config import ExchangeSettings
from bidstorm.exchange.app import create_app
from bidstorm.exchange.dmp import DMPClient
from bidstorm.exchange.registry import DSPRegistry
from bidstorm.exchange.service import ExchangeService
from tests.fakes import FakeBidder, bid_request_payload


def make_service(bidders, **overrides):
    settings = ExchangeSettings(tmax_ms=80, overhead_ms=5, default_floor=0.1, **overrides)
    dmp = DMPClient(fakeredis.FakeAsyncRedis(), timeout_ms=50)
    return ExchangeService(settings, dmp, DSPRegistry(bidders))


@pytest.fixture
def bidders():
    return [FakeBidder("a", price=3.0), FakeBidder("b", price=2.0), FakeBidder("c", price=1.0)]


def test_second_price_auction_end_to_end(bidders):
    svc = make_service(bidders)
    with TestClient(create_app(svc)) as client:
        r = client.post("/openrtb2/auction", json=bid_request_payload())
        assert r.status_code == 200
        body = r.json()
        [seat] = body["seatbid"]
        assert seat["seat"] == "a"
        assert seat["bid"][0]["price"] == pytest.approx(2.01)
        assert "/track/imp?auction=req-1&imp=1" in seat["bid"][0]["nurl"]
    # Background win notice delivered on shutdown drain.
    assert bidders[0].wins and bidders[0].wins[0].clearing_price == pytest.approx(2.01)


def test_no_bid_returns_204():
    svc = make_service([FakeBidder("a", price=None)])
    with TestClient(create_app(svc)) as client:
        r = client.post("/openrtb2/auction", json=bid_request_payload())
        assert r.status_code == 204


def test_floor_filters_bids(bidders):
    svc = make_service(bidders)
    with TestClient(create_app(svc)) as client:
        r = client.post("/openrtb2/auction", json=bid_request_payload(floor=2.5))
        bid = r.json()["seatbid"][0]["bid"][0]
        assert bid["price"] == pytest.approx(2.51)


def test_dmp_segments_are_forwarded_to_dsps():
    seen = {}

    class Spy(FakeBidder):
        async def bid(self, request, timeout):
            seen["segments"] = list(request.user_segments)
            seen["timeout"] = timeout
            return await super().bid(request, timeout)

    svc = make_service([Spy("spy", price=1.0)])
    with TestClient(create_app(svc)) as client:
        client.portal.call(svc.dmp.add_segments, "u42", ["sports"])
        client.post("/openrtb2/auction", json=bid_request_payload(user_id="u42", tmax=40))
    assert seen["segments"] == ["sports"]
    assert seen["timeout"] < 0.04  # request tmax tightened the budget


def test_invalid_request_rejected():
    svc = make_service([])
    with TestClient(create_app(svc)) as client:
        r = client.post("/openrtb2/auction", json={"id": "x", "imp": []})
        assert r.status_code == 422


def test_health_and_metrics(bidders):
    svc = make_service(bidders)
    with TestClient(create_app(svc)) as client:
        client.post("/openrtb2/auction", json=bid_request_payload())
        assert client.get("/healthz").json()["dsps"] == ["a", "b", "c"]
        text = client.get("/metrics").text
        assert "bidstorm_auction_latency_ms_bucket" in text
        assert 'bidstorm_auction_wins_total{bidder="a"} 1.0' in text
