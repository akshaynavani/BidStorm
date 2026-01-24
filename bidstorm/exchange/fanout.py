"""Concurrent bid-request fan-out to DSPs under a hard deadline.

Every DSP is called in parallel with the remaining budget as its gRPC deadline. The
exchange additionally enforces the budget locally with `asyncio.wait(timeout=...)`, so a
misbehaving client library or a slow event loop can never stretch the auction: anything
not finished by the deadline is cancelled and treated as a no-bid.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum

import grpc

from bidstorm.exchange.registry import BidderClient
from bidstorm.proto import bidding_pb2


class CallStatus(str, Enum):
    OK = "ok"
    NO_BID = "no_bid"
    TIMEOUT = "timeout"
    ERROR = "error"


@dataclass(slots=True)
class DSPResult:
    bidder: str
    status: CallStatus
    latency_ms: float
    response: bidding_pb2.BidResponse | None = None
    arrival_seq: int = -1
    error: str | None = None


class FanOut:
    def __init__(self, clients: list[BidderClient]) -> None:
        self.clients = clients

    async def _call(
        self, client: BidderClient, request: bidding_pb2.BidRequest, budget_s: float
    ) -> bidding_pb2.BidResponse:
        return await client.bid(request, timeout=budget_s)

    async def broadcast(
        self, request: bidding_pb2.BidRequest, budget_s: float
    ) -> list[DSPResult]:
        if not self.clients:
            return []
        if budget_s <= 0:
            return [DSPResult(c.name, CallStatus.TIMEOUT, 0.0) for c in self.clients]

        started = time.perf_counter()
        tasks: dict[asyncio.Task, BidderClient] = {}
        finish_order: list[asyncio.Task] = []
        finish_time: dict[asyncio.Task, float] = {}

        def _on_done(task: asyncio.Task) -> None:
            finish_order.append(task)
            finish_time[task] = (time.perf_counter() - started) * 1000.0

        for client in self.clients:
            task = asyncio.create_task(self._call(client, request, budget_s))
            task.add_done_callback(_on_done)
            tasks[task] = client

        done, pending = await asyncio.wait(tasks.keys(), timeout=budget_s)
        for task in pending:
            task.cancel()
        if pending:
            # Let cancellations settle so no "Task exception was never retrieved" leaks.
            await asyncio.gather(*pending, return_exceptions=True)

        seq = {task: i for i, task in enumerate(t for t in finish_order if t in done)}
        results: list[DSPResult] = []
        for task, client in tasks.items():
            if task in pending:
                results.append(DSPResult(client.name, CallStatus.TIMEOUT, budget_s * 1000.0))
                continue
            latency = finish_time.get(task, budget_s * 1000.0)
            exc = task.exception()
            if exc is not None:
                status = CallStatus.ERROR
                if isinstance(exc, grpc.aio.AioRpcError) and (
                    exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED
                ):
                    status = CallStatus.TIMEOUT
                elif isinstance(exc, asyncio.TimeoutError):
                    status = CallStatus.TIMEOUT
                results.append(DSPResult(client.name, status, latency, error=repr(exc)))
                continue
            response = task.result()
            status = CallStatus.OK if response.bids else CallStatus.NO_BID
            order = seq.get(task, len(seq))
            results.append(
                DSPResult(client.name, status, latency, response=response, arrival_seq=order)
            )
        return results
