"""DSP registry: one long-lived gRPC channel per bidder."""

from __future__ import annotations

from typing import Protocol

import grpc

from bidstorm.proto import bidding_pb2, bidding_pb2_grpc


class BidderClient(Protocol):
    name: str

    async def bid(
        self, request: bidding_pb2.BidRequest, timeout: float
    ) -> bidding_pb2.BidResponse: ...

    async def notify_win(self, notice: bidding_pb2.WinNotice, timeout: float) -> None: ...

    async def close(self) -> None: ...


class GrpcBidderClient:
    def __init__(self, name: str, target: str) -> None:
        self.name = name
        self.target = target
        self._channel = grpc.aio.insecure_channel(
            target,
            options=[
                ("grpc.keepalive_time_ms", 30_000),
                ("grpc.http2.max_pings_without_data", 0),
                ("grpc.enable_retries", 0),  # retries make no sense inside an 80 ms budget
            ],
        )
        self._stub = bidding_pb2_grpc.BidderStub(self._channel)

    async def bid(self, request: bidding_pb2.BidRequest, timeout: float) -> bidding_pb2.BidResponse:
        return await self._stub.Bid(request, timeout=timeout)

    async def notify_win(self, notice: bidding_pb2.WinNotice, timeout: float = 0.5) -> None:
        await self._stub.NotifyWin(notice, timeout=timeout)

    async def close(self) -> None:
        await self._channel.close()


class DSPRegistry:
    def __init__(self, clients: list[BidderClient]) -> None:
        self._clients = {c.name: c for c in clients}

    @classmethod
    def from_endpoints(cls, endpoints: dict[str, str]) -> DSPRegistry:
        return cls([GrpcBidderClient(name, target) for name, target in endpoints.items()])

    def all(self) -> list[BidderClient]:
        return list(self._clients.values())

    def get(self, name: str) -> BidderClient | None:
        return self._clients.get(name)

    def __len__(self) -> int:
        return len(self._clients)

    async def close(self) -> None:
        for c in self._clients.values():
            await c.close()
