"""Synthetic impression-log generator with planted click-through effects.

The generator draws impression contexts from plausible marginal distributions and
assigns clicks from a known logistic model, so a trained CTR model can be checked
against ground truth (it should recover the planted affinities).
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterator

from bidstorm.ml.features import ImpressionContext

SEGMENTS = [
    "sports", "auto", "travel", "finance", "gaming", "fashion",
    "parents", "tech", "foodies", "fitness", "music", "home",
]
CATEGORIES = {
    "IAB17": "sports", "IAB2": "auto", "IAB20": "travel", "IAB13": "finance",
    "IAB9": "gaming", "IAB18": "fashion", "IAB6": "parents", "IAB19": "tech",
    "IAB8": "foodies", "IAB7": "fitness", "IAB1": "music", "IAB10": "home",
}
DOMAINS = {
    "espn.example": ["IAB17"], "motortrend.example": ["IAB2"], "lonelyplanet.example": ["IAB20"],
    "bloomberg.example": ["IAB13"], "ign.example": ["IAB9", "IAB19"], "vogue.example": ["IAB18"],
    "parents.example": ["IAB6"], "theverge.example": ["IAB19"], "allrecipes.example": ["IAB8"],
    "runnersworld.example": ["IAB7"], "pitchfork.example": ["IAB1"], "houzz.example": ["IAB10"],
    "news.example": ["IAB12"], "weather.example": ["IAB15"],
}
SIZES = [(300, 250), (728, 90), (320, 50), (160, 600), (300, 600)]
SIZE_EFFECT = {(300, 250): 0.10, (728, 90): -0.25, (320, 50): -0.10, (160, 600): -0.05,
               (300, 600): 0.20}
COUNTRIES = ["US", "GB", "DE", "IN", "BR", "CA"]
OS_BY_DEVICE = {2: ["windows", "macos", "linux"], 4: ["ios", "android"], 5: ["ios", "android"]}

BASE_LOGIT = -4.2  # ~1.5% baseline CTR


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def true_logit(ctx: ImpressionContext) -> float:
    """Ground-truth click logit used to label synthetic data."""
    z = BASE_LOGIT
    content_topics = {CATEGORIES.get(c) for c in ctx.site_categories}
    for seg in ctx.segments:
        if seg in content_topics:
            z += 0.9  # audience matches content: strongest planted effect
        elif seg in ("gaming", "tech") and ctx.device_type in (4, 5):
            z += 0.3
    z += SIZE_EFFECT.get((ctx.width, ctx.height), 0.0)
    z += {1: 0.45, 3: -0.30}.get(ctx.position, 0.0)
    z += 0.20 if ctx.device_type in (4, 5) else 0.0
    z += 0.15 if 18 <= ctx.hour <= 23 else 0.0
    z -= 0.25 if ctx.site_domain in ("news.example", "weather.example") else 0.0
    return z


def sample_context(rng: random.Random) -> ImpressionContext:
    domain = rng.choice(list(DOMAINS))
    device = rng.choices([2, 4, 5], weights=[0.45, 0.45, 0.10])[0]
    w, h = rng.choice(SIZES)
    n_segs = rng.choices([0, 1, 2, 3], weights=[0.2, 0.35, 0.3, 0.15])[0]
    return ImpressionContext(
        site_domain=domain,
        site_categories=list(DOMAINS[domain]),
        device_type=device,
        os=rng.choice(OS_BY_DEVICE[device]),
        country=rng.choice(COUNTRIES),
        segments=rng.sample(SEGMENTS, n_segs),
        width=w,
        height=h,
        position=rng.choices([0, 1, 3], weights=[0.3, 0.4, 0.3])[0],
        hour=rng.randrange(24),
    )


def generate(n: int, seed: int = 42) -> Iterator[tuple[ImpressionContext, int]]:
    rng = random.Random(seed)
    for _ in range(n):
        ctx = sample_context(rng)
        clicked = 1 if rng.random() < _sigmoid(true_logit(ctx)) else 0
        yield ctx, clicked
