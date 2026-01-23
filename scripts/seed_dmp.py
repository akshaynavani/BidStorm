"""Seed the Redis DMP with synthetic user -> segment memberships.

Users are named `u<N>`; the Locust load test draws user ids from the same space so a
configurable share of bid requests hit the DMP.
"""

from __future__ import annotations

import argparse
import os
import random

import redis

from bidstorm.exchange.dmp import DEFAULT_TTL_SECONDS, user_key
from bidstorm.ml.synth import SEGMENTS


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--users", type=int, default=50_000)
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--batch", type=int, default=5_000)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    client = redis.Redis.from_url(args.redis_url)
    pipe = client.pipeline(transaction=False)
    written = 0
    for i in range(args.users):
        segs = rng.sample(SEGMENTS, rng.choices([1, 2, 3, 4], weights=[0.3, 0.35, 0.25, 0.1])[0])
        key = user_key(f"u{i}")
        pipe.delete(key)
        pipe.sadd(key, *segs)
        pipe.expire(key, DEFAULT_TTL_SECONDS)
        written += 1
        if written % args.batch == 0:
            pipe.execute()
    pipe.execute()
    print(f"seeded {written} users into {args.redis_url}")


if __name__ == "__main__":
    main()
