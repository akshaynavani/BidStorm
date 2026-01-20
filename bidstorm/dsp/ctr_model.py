"""Online CTR scoring for DSP bidders."""

from __future__ import annotations

import logging
import math
import pathlib

import joblib
import numpy as np

from bidstorm.ml.features import ImpressionContext, extract, make_hasher

log = logging.getLogger(__name__)

MIN_CTR = 1e-5
MAX_CTR = 0.5


class CTRModel:
    """Wraps a trained logistic regression; scores via a dot product for low latency.

    `predict_proba` on a 1-row sparse matrix carries noticeable sklearn overhead, so we
    pull out the coefficient vector once and compute sigmoid(w·x + b) directly.
    """

    def __init__(self, coef: np.ndarray, intercept: float, n_features: int) -> None:
        self._coef = coef
        self._intercept = intercept
        self._hasher = make_hasher(n_features)

    @classmethod
    def load(cls, path: str | pathlib.Path) -> CTRModel:
        bundle = joblib.load(path)
        model = bundle["model"]
        log.info("loaded CTR model v%s metrics=%s", bundle.get("version"), bundle.get("metrics"))
        return cls(
            coef=np.asarray(model.coef_[0], dtype=np.float64),
            intercept=float(model.intercept_[0]),
            n_features=int(bundle["n_features"]),
        )

    def predict(self, ctx: ImpressionContext) -> float:
        x = self._hasher.transform([extract(ctx)])
        z = float(x.dot(self._coef)[0]) + self._intercept
        return min(MAX_CTR, max(MIN_CTR, 1.0 / (1.0 + math.exp(-z))))


class HeuristicCTRModel:
    """Fallback used when no trained model is available (e.g. first boot)."""

    def __init__(self, base_ctr: float = 0.015) -> None:
        self.base_ctr = base_ctr

    def predict(self, ctx: ImpressionContext) -> float:
        ctr = self.base_ctr
        if ctx.segments:
            ctr *= 1.0 + 0.2 * min(len(ctx.segments), 3)
        if ctx.position == 1:
            ctr *= 1.4
        return min(MAX_CTR, ctr)


def load_or_fallback(path: str | pathlib.Path) -> CTRModel | HeuristicCTRModel:
    p = pathlib.Path(path)
    if p.exists():
        return CTRModel.load(p)
    log.warning("CTR model %s not found; using heuristic fallback", p)
    return HeuristicCTRModel()
