"""Event publishers. Kafka in production, no-op / in-memory for local dev and tests.

Publishing is never allowed to slow down or fail an auction: the Kafka producer batches
in the background (`linger_ms`) and we only await enqueueing, not broker delivery.
"""

from __future__ import annotations

import logging
from typing import Protocol

from pydantic import BaseModel

from bidstorm.config import KafkaSettings
from bidstorm.events.schema import encode, topic_for

log = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def start(self) -> None: ...

    async def publish(self, event: BaseModel, key: str | None = None) -> None: ...

    async def stop(self) -> None: ...


class NullPublisher:
    async def start(self) -> None:
        pass

    async def publish(self, event: BaseModel, key: str | None = None) -> None:
        pass

    async def stop(self) -> None:
        pass


class InMemoryPublisher:
    def __init__(self) -> None:
        self.events: list[tuple[str, BaseModel]] = []

    async def start(self) -> None:
        pass

    async def publish(self, event: BaseModel, key: str | None = None) -> None:
        self.events.append((topic_for(event), event))

    async def stop(self) -> None:
        pass

    def of_type(self, type_: str) -> list[BaseModel]:
        return [e for _, e in self.events if e.type == type_]  # type: ignore[attr-defined]


class KafkaPublisher:
    def __init__(self, settings: KafkaSettings) -> None:
        self.settings = settings
        self._producer = None
        self.failures = 0

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.bootstrap,
            client_id="bidstorm-exchange",
            acks=1,
            linger_ms=5,
            max_batch_size=256 * 1024,
            enable_idempotence=False,
        )
        await self._producer.start()
        log.info("Kafka producer connected to %s", self.settings.bootstrap)

    async def publish(self, event: BaseModel, key: str | None = None) -> None:
        if self._producer is None:
            return
        try:
            await self._producer.send(
                self.settings.topic(topic_for(event)),
                value=encode(event),
                key=key.encode() if key else None,
            )
        except Exception as exc:  # noqa: BLE001 - analytics is best-effort
            self.failures += 1
            log.warning("dropping analytics event: %r", exc)

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()


def build_publisher(settings: KafkaSettings) -> EventPublisher:
    return KafkaPublisher(settings) if settings.enabled else NullPublisher()
