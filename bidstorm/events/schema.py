"""Analytics event schema shared by the exchange (producer) and the consumer."""

from __future__ import annotations

import time
from typing import Literal

import orjson
from pydantic import BaseModel, Field

TOPIC_AUCTIONS = "auctions"
TOPIC_IMPRESSIONS = "impressions"
TOPIC_CLICKS = "clicks"
ALL_TOPICS = (TOPIC_AUCTIONS, TOPIC_IMPRESSIONS, TOPIC_CLICKS)


def now_ms() -> int:
    return int(time.time() * 1000)


class BidSummary(BaseModel):
    bidder: str
    imp_id: str
    price: float


class WinSummary(BaseModel):
    imp_id: str
    bidder: str
    bid_price: float
    clearing_price: float
    predicted_ctr: float = 0.0


class AuctionEvent(BaseModel):
    type: Literal["auction"] = "auction"
    ts: int = Field(default_factory=now_ms)
    auction_id: str
    site_domain: str
    segments: list[str] = Field(default_factory=list)
    n_imps: int
    latency_ms: float
    auction_type: int
    dsp_status: dict[str, str] = Field(default_factory=dict)
    bids: list[BidSummary] = Field(default_factory=list)
    winners: list[WinSummary] = Field(default_factory=list)


class ImpressionEvent(BaseModel):
    type: Literal["impression"] = "impression"
    ts: int = Field(default_factory=now_ms)
    auction_id: str
    imp_id: str
    bidder: str
    price: float  # clearing CPM
    creative_id: str = ""


class ClickEvent(BaseModel):
    type: Literal["click"] = "click"
    ts: int = Field(default_factory=now_ms)
    auction_id: str
    imp_id: str
    bidder: str
    creative_id: str = ""
    predicted_ctr: float = 0.0


Event = AuctionEvent | ImpressionEvent | ClickEvent

_BY_TYPE: dict[str, type[BaseModel]] = {
    "auction": AuctionEvent,
    "impression": ImpressionEvent,
    "click": ClickEvent,
}


def encode(event: BaseModel) -> bytes:
    return orjson.dumps(event.model_dump(mode="json"))


def decode(raw: bytes) -> Event:
    data = orjson.loads(raw)
    model = _BY_TYPE.get(data.get("type"))
    if model is None:
        raise ValueError(f"unknown event type: {data.get('type')!r}")
    return model.model_validate(data)  # type: ignore[return-value]


def topic_for(event: BaseModel) -> str:
    return {
        "auction": TOPIC_AUCTIONS,
        "impression": TOPIC_IMPRESSIONS,
        "click": TOPIC_CLICKS,
    }[event.type]  # type: ignore[attr-defined]
