"""Prometheus metrics for the exchange."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

LATENCY_BUCKETS_MS = (1, 2, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 150, 250)


class ExchangeMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.requests = Counter(
            "bidstorm_auction_requests_total", "Bid requests received", ["result"], registry=r
        )
        self.auction_latency = Histogram(
            "bidstorm_auction_latency_ms",
            "End-to-end auction latency in milliseconds",
            buckets=LATENCY_BUCKETS_MS,
            registry=r,
        )
        self.dsp_latency = Histogram(
            "bidstorm_dsp_latency_ms",
            "Per-DSP response latency in milliseconds",
            ["bidder"],
            buckets=LATENCY_BUCKETS_MS,
            registry=r,
        )
        self.dsp_calls = Counter(
            "bidstorm_dsp_calls_total", "DSP calls by outcome", ["bidder", "status"], registry=r
        )
        self.impressions_won = Counter(
            "bidstorm_auction_wins_total", "Impressions cleared per bidder", ["bidder"], registry=r
        )
        self.clearing_price = Histogram(
            "bidstorm_clearing_price_cpm",
            "Clearing price (CPM)",
            buckets=(0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 12, 20, 30),
            registry=r,
        )
        self.dmp_lookups = Counter(
            "bidstorm_dmp_lookups_total", "DMP enrichment outcomes", ["result"], registry=r
        )
        self.events_published = Counter(
            "bidstorm_events_published_total", "Analytics events sent", ["topic"], registry=r
        )
        self.registered_dsps = Gauge(
            "bidstorm_registered_dsps", "Number of configured DSPs", registry=r
        )
