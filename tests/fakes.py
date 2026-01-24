"""In-process fakes for DSP clients."""

from __future__ import annotations

import asyncio

from bidstorm.proto import bidding_pb2


class FakeBidder:
    def __init__(self, name, price=None, delay=0.0, fail=False, prices=None):
        self.name = name
        self.price = price
        self.prices = prices or {}
        self.delay = delay
        self.fail = fail
        self.calls = 0
        self.wins: list[bidding_pb2.WinNotice] = []

    async def bid(self, request, timeout):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise RuntimeError("boom")
        bids = []
        for imp in request.imps:
            price = self.prices.get(imp.id, self.price)
            if price is not None:
                bids.append(
                    bidding_pb2.Bid(
                        imp_id=imp.id,
                        price=price,
                        creative_id=f"{self.name}-cr",
                        adomain=f"{self.name}.example",
                        adm="<img/>",
                        predicted_ctr=0.02,
                    )
                )
        return bidding_pb2.BidResponse(request_id=request.id, bidder=self.name, bids=bids)

    async def notify_win(self, notice, timeout):
        self.wins.append(notice)

    async def close(self):
        pass


def bid_request_payload(req_id="req-1", user_id="u1", tmax=None, floor=0.0, imps=1):
    payload = {
        "id": req_id,
        "imp": [
            {"id": str(i + 1), "banner": {"w": 300, "h": 250, "pos": 1}, "bidfloor": floor}
            for i in range(imps)
        ],
        "site": {"id": "s1", "domain": "espn.example", "cat": ["IAB17"]},
        "device": {"devicetype": 4, "os": "ios", "geo": {"country": "US"}},
        "user": {"id": user_id},
        "at": 2,
    }
    if tmax is not None:
        payload["tmax"] = tmax
    return payload
