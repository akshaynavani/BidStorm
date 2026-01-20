"""Train the DSP click-through-rate model on synthetic impression logs.

Usage:
    python -m bidstorm.ml.train_ctr --rows 200000 --out models/ctr.joblib
"""

from __future__ import annotations

import argparse
import json
import pathlib
import time

import joblib
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.model_selection import train_test_split

from bidstorm.ml.features import N_FEATURES, extract, make_hasher
from bidstorm.ml.synth import generate

MODEL_VERSION = 1


def build_dataset(rows: int, seed: int):
    contexts, labels = [], []
    for ctx, clicked in generate(rows, seed=seed):
        contexts.append(extract(ctx))
        labels.append(clicked)
    X = make_hasher(N_FEATURES).transform(contexts)
    y = np.asarray(labels, dtype=np.int8)
    return X, y


def train(rows: int, seed: int = 42, c: float = 0.5) -> tuple[LogisticRegression, dict]:
    t0 = time.perf_counter()
    X, y = build_dataset(rows, seed)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=seed, stratify=y
    )
    model = LogisticRegression(C=c, solver="liblinear", max_iter=200)
    model.fit(X_train, y_train)
    p = model.predict_proba(X_test)[:, 1]
    metrics = {
        "rows": rows,
        "base_ctr": float(y.mean()),
        "test_auc": float(roc_auc_score(y_test, p)),
        "test_logloss": float(log_loss(y_test, p)),
        "mean_pred_ctr": float(p.mean()),
        "train_seconds": round(time.perf_counter() - t0, 2),
    }
    return model, metrics


def save(model: LogisticRegression, metrics: dict, out: pathlib.Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {"version": MODEL_VERSION, "n_features": N_FEATURES, "model": model, "metrics": metrics},
        out,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--C", type=float, default=0.5, help="Inverse L2 regularization.")
    parser.add_argument("--out", type=pathlib.Path, default=pathlib.Path("models/ctr.joblib"))
    args = parser.parse_args(argv)

    model, metrics = train(args.rows, seed=args.seed, c=args.C)
    save(model, metrics, args.out)
    print(json.dumps(metrics, indent=2))
    print(f"saved model to {args.out}")


if __name__ == "__main__":
    main()
