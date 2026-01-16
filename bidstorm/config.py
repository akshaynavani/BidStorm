"""Runtime configuration loaded from environment variables (and an optional .env file)."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


class ExchangeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EXCHANGE_", env_file=".env", extra="ignore")

    tmax_ms: int = Field(80, description="Hard cap on time given to DSPs to respond.")
    overhead_ms: int = Field(10, description="Reserved for enrichment, auction and serialization.")
    auction_type: int = Field(2, description="1 = first price, 2 = second price (OpenRTB `at`).")
    default_floor: float = 0.10
    bid_increment: float = 0.01
    public_url: str = "http://localhost:8000"
    dsp_endpoints: str = "dsp-alpha=localhost:50051"

    def endpoints(self) -> dict[str, str]:
        """Parse `name=host:port,name=host:port` into a mapping."""
        out: dict[str, str] = {}
        for pair in _parse_csv(self.dsp_endpoints):
            name, _, target = pair.partition("=")
            if not target:
                raise ValueError(f"Malformed DSP endpoint entry: {pair!r}")
            out[name.strip()] = target.strip()
        return out


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    dmp_timeout_ms: int = 5


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", env_file=".env", extra="ignore")

    enabled: bool = True
    bootstrap: str = "localhost:9092"
    topic_prefix: str = "bidstorm"

    def topic(self, name: str) -> str:
        return f"{self.topic_prefix}.{name}"


class DSPSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DSP_", env_file=".env", extra="ignore")

    name: str = "dsp-alpha"
    port: int = 50051
    value_per_click: float = 1.50
    daily_budget: float = 500.0
    target_segments: str = ""
    latency_ms: float = 5.0
    latency_jitter_ms: float = 10.0
    model_path: str = "models/ctr.joblib"
    max_cpm: float = 25.0

    def segments(self) -> set[str]:
        return set(_parse_csv(self.target_segments))


@lru_cache
def exchange_settings() -> ExchangeSettings:
    return ExchangeSettings()


@lru_cache
def redis_settings() -> RedisSettings:
    return RedisSettings()


@lru_cache
def kafka_settings() -> KafkaSettings:
    return KafkaSettings()
