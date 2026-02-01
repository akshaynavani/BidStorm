"""Kafka consumer that aggregates exchange analytics events into Redis.

Run with:
    python -m bidstorm.analytics.consumer
"""

from __future__ import annotations

import asyncio
import logging
import signal

from aiokafka import AIOKafkaConsumer
from redis.asyncio import Redis

from bidstorm.analytics.aggregator import Aggregator
from bidstorm.config import KafkaSettings, RedisSettings
from bidstorm.events.schema import ALL_TOPICS, decode

log = logging.getLogger("bidstorm.analytics")


async def consume(kafka: KafkaSettings, redis_cfg: RedisSettings, stop: asyncio.Event) -> None:
    redis = Redis.from_url(redis_cfg.redis_url)
    aggregator = Aggregator(redis)
    consumer = AIOKafkaConsumer(
        *(kafka.topic(t) for t in ALL_TOPICS),
        bootstrap_servers=kafka.bootstrap,
        group_id="bidstorm-analytics",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    log.info("consuming %s from %s", [kafka.topic(t) for t in ALL_TOPICS], kafka.bootstrap)
    processed = bad = 0
    try:
        while not stop.is_set():
            batches = await consumer.getmany(timeout_ms=500, max_records=2000)
            events = []
            for records in batches.values():
                for record in records:
                    try:
                        events.append(decode(record.value))
                    except Exception:  # noqa: BLE001 - skip poison messages
                        bad += 1
                        log.warning("skipping undecodable record at offset %s", record.offset)
            if events:
                await aggregator.apply_batch(events)
                processed += len(events)
            if batches:
                # At-least-once: commit only after the batch landed in Redis.
                await consumer.commit()
            if processed and processed % 10_000 < len(events):
                log.info("processed=%d bad=%d", processed, bad)
    finally:
        await consumer.stop()
        await redis.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    async def runner() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # pragma: no cover
                pass
        await consume(KafkaSettings(), RedisSettings(), stop)

    asyncio.run(runner())


if __name__ == "__main__":
    main()
