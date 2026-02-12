"""Locust load test for the BidStorm exchange.

Simulates publisher ad servers sending OpenRTB bid requests and browsers firing the
impression pixel / click redirect for filled impressions.

    locust -f loadtest/locustfile.py --host http://localhost:8000
    locust -f loadtest/locustfile.py --host http://localhost:8000 \
        --headless -u 200 -r 50 -t 2m --csv results/run1

Requests slower than the 100 ms SLA are marked as failures so the Locust failure rate
doubles as an SLA-violation rate.
"""

from __future__ import annotations

import random
import time
import uuid
from urllib.parse import urlsplit

from locust import FastHttpUser, between, events, task

SLA_MS = 100
SITES = [
    ("espn.example", ["IAB17"]),
    ("motortrend.example", ["IAB2"]),
    ("lonelyplanet.example", ["IAB20"]),
    ("bloomberg.example", ["IAB13"]),
    ("ign.example", ["IAB9", "IAB19"]),
    ("vogue.example", ["IAB18"]),
    ("allrecipes.example", ["IAB8"]),
    ("news.example", ["IAB12"]),
]
SIZES = [(300, 250), (728, 90), (320, 50), (160, 600), (300, 600)]
DEVICES = [(2, "windows"), (2, "macos"), (4, "ios"), (4, "android"), (5, "ios")]
COUNTRIES = ["US", "GB", "DE", "IN", "BR", "CA"]
# Users u0..u49999 are seeded in the DMP; the rest are unknown to it.
USER_SPACE = 70_000
IMPRESSION_RENDER_RATE = 0.9
CLICK_RATE = 0.03


def make_bid_request() -> dict:
    domain, cats = random.choice(SITES)
    devicetype, os_name = random.choice(DEVICES)
    n_imps = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
    imps = []
    for i in range(n_imps):
        w, h = random.choice(SIZES)
        imps.append(
            {
                "id": str(i + 1),
                "banner": {"w": w, "h": h, "pos": random.choice([0, 1, 3])},
                "bidfloor": round(random.choice([0.0, 0.1, 0.25, 0.5, 1.0]), 2),
                "tagid": f"{domain}-slot-{i + 1}",
            }
        )
    return {
        "id": uuid.uuid4().hex,
        "imp": imps,
        "site": {"id": domain, "domain": domain, "cat": cats,
                 "publisher": {"id": f"pub-{domain}"}},
        "device": {"devicetype": devicetype, "os": os_name,
                   "geo": {"country": random.choice(COUNTRIES)}},
        "user": {"id": f"u{random.randrange(USER_SPACE)}"},
        "tmax": random.choice([80, 100, 120]),
        "at": random.choices([2, 1], weights=[0.8, 0.2])[0],
    }


class PublisherAdServer(FastHttpUser):
    wait_time = between(0.0, 0.02)

    @task
    def auction(self) -> None:
        started = time.perf_counter()
        with self.client.post(
            "/openrtb2/auction", json=make_bid_request(), name="auction", catch_response=True
        ) as resp:
            elapsed_ms = (time.perf_counter() - started) * 1000
            if resp.status_code not in (200, 204):
                resp.failure(f"unexpected status {resp.status_code}")
                return
            if elapsed_ms > SLA_MS:
                resp.failure(f"SLA breach: {elapsed_ms:.1f} ms")
                return
            resp.success()
            if resp.status_code == 204:
                return
            body = resp.json()

        for seat in body.get("seatbid", []):
            for bid in seat.get("bid", []):
                if random.random() > IMPRESSION_RENDER_RATE or not bid.get("nurl"):
                    continue
                parts = urlsplit(bid["nurl"])
                self.client.get(f"{parts.path}?{parts.query}", name="track/imp")
                if random.random() < CLICK_RATE:
                    click_path = parts.path.replace("/track/imp", "/track/click")
                    self.client.get(
                        f"{click_path}?{parts.query}", name="track/click", allow_redirects=False
                    )


@events.quitting.add_listener
def _report(environment, **_kwargs) -> None:
    stats = environment.stats.get("auction", "POST")
    if stats.num_requests:
        print(
            f"auction p50={stats.get_response_time_percentile(0.5):.1f}ms "
            f"p95={stats.get_response_time_percentile(0.95):.1f}ms "
            f"p99={stats.get_response_time_percentile(0.99):.1f}ms "
            f"fail/SLA={stats.fail_ratio:.2%}"
        )
