"""Feature extraction shared by CTR training and online DSP scoring.

Keeping one function for both paths avoids training/serving skew. Features are emitted
as a sparse `{name: value}` dict consumed by scikit-learn's `FeatureHasher`, so the model
never needs a vocabulary and unseen domains/segments degrade gracefully.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sklearn.feature_extraction import FeatureHasher

N_FEATURES = 2**18

DEVICE_NAMES = {1: "mobile", 2: "pc", 3: "ctv", 4: "phone", 5: "tablet"}


@dataclass(slots=True)
class ImpressionContext:
    site_domain: str = ""
    site_categories: list[str] = field(default_factory=list)
    device_type: int = 2
    os: str = ""
    country: str = ""
    segments: list[str] = field(default_factory=list)
    width: int = 300
    height: int = 250
    position: int = 0
    hour: int = 12


def _hour_bucket(hour: int) -> str:
    if 0 <= hour < 6:
        return "night"
    if hour < 12:
        return "morning"
    if hour < 18:
        return "afternoon"
    return "evening"


def extract(ctx: ImpressionContext) -> dict[str, float]:
    feats: dict[str, float] = {
        "bias": 1.0,
        f"domain={ctx.site_domain.lower()}": 1.0,
        f"device={DEVICE_NAMES.get(ctx.device_type, 'other')}": 1.0,
        f"os={ctx.os.lower() or 'unknown'}": 1.0,
        f"country={ctx.country.upper() or 'XX'}": 1.0,
        f"size={ctx.width}x{ctx.height}": 1.0,
        f"pos={ctx.position}": 1.0,
        f"hour={_hour_bucket(ctx.hour)}": 1.0,
    }
    cats = ctx.site_categories or ["none"]
    segs = ctx.segments or ["none"]
    for cat in cats:
        feats[f"cat={cat}"] = 1.0
    for seg in segs:
        feats[f"seg={seg}"] = 1.0
        # Segment x content-category crosses capture audience/context affinity.
        for cat in cats:
            feats[f"seg={seg}&cat={cat}"] = 1.0
        feats[f"seg={seg}&device={DEVICE_NAMES.get(ctx.device_type, 'other')}"] = 1.0
    feats["n_segments"] = min(len(ctx.segments), 10) / 10.0
    return feats


def make_hasher(n_features: int = N_FEATURES) -> FeatureHasher:
    return FeatureHasher(n_features=n_features, input_type="dict", alternate_sign=False)
