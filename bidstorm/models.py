"""OpenRTB 2.5-flavoured request/response models (a pragmatic subset).

Only fields the simulator actually uses are modelled; unknown fields are accepted and
ignored so real-world-ish payloads still validate.
"""

from __future__ import annotations

from enum import IntEnum

from pydantic import BaseModel, ConfigDict, Field


class AuctionType(IntEnum):
    FIRST_PRICE = 1
    SECOND_PRICE = 2


class _Lenient(BaseModel):
    model_config = ConfigDict(extra="ignore")


class Banner(_Lenient):
    w: int = 300
    h: int = 250
    pos: int = 0  # OpenRTB ad position: 0 unknown, 1 above fold, 3 below fold, ...


class Imp(_Lenient):
    id: str
    banner: Banner = Field(default_factory=Banner)
    bidfloor: float = 0.0
    bidfloorcur: str = "USD"
    tagid: str | None = None


class Publisher(_Lenient):
    id: str
    name: str | None = None


class Site(_Lenient):
    id: str
    domain: str
    cat: list[str] = Field(default_factory=list)
    page: str | None = None
    publisher: Publisher | None = None


class Geo(_Lenient):
    country: str | None = None
    region: str | None = None


class Device(_Lenient):
    ua: str | None = None
    ip: str | None = None
    devicetype: int = 2  # 1 mobile/tablet, 2 PC, 4 phone, 5 tablet
    os: str | None = None
    geo: Geo | None = None


class User(_Lenient):
    id: str | None = None
    buyeruid: str | None = None
    # Filled in by the exchange from the DMP; clients may also pre-populate.
    segments: list[str] = Field(default_factory=list)


class BidRequest(_Lenient):
    id: str
    imp: list[Imp] = Field(min_length=1)
    site: Site
    device: Device = Field(default_factory=Device)
    user: User = Field(default_factory=User)
    tmax: int | None = Field(None, description="Max time in ms the caller will wait.")
    at: AuctionType = AuctionType.SECOND_PRICE
    test: int = 0


class Bid(_Lenient):
    id: str
    impid: str
    price: float
    adm: str | None = None
    adomain: list[str] = Field(default_factory=list)
    crid: str | None = None
    nurl: str | None = None


class SeatBid(_Lenient):
    seat: str
    bid: list[Bid]


class BidResponse(_Lenient):
    id: str
    seatbid: list[SeatBid] = Field(default_factory=list)
    cur: str = "USD"
    nbr: int | None = Field(None, description="No-bid reason code when seatbid is empty.")


class NoBidReason(IntEnum):
    UNKNOWN = 0
    TECHNICAL_ERROR = 1
    INVALID_REQUEST = 2
    INSUFFICIENT_TIME = 9  # exchange-specific extension used when the budget is blown
