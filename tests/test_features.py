from bidstorm.ml.features import ImpressionContext, extract, make_hasher
from bidstorm.ml.synth import generate, true_logit


def test_extract_includes_crosses_and_defaults():
    ctx = ImpressionContext(site_domain="ESPN.example", site_categories=["IAB17"],
                            segments=["sports"], device_type=4, hour=20)
    f = extract(ctx)
    assert f["domain=espn.example"] == 1.0
    assert f["seg=sports&cat=IAB17"] == 1.0
    assert f["device=phone"] == 1.0
    assert f["hour=evening"] == 1.0


def test_extract_handles_empty_segments():
    f = extract(ImpressionContext())
    assert f["seg=none"] == 1.0
    assert f["cat=none"] == 1.0
    assert f["n_segments"] == 0.0


def test_hasher_is_deterministic():
    ctx = ImpressionContext(site_domain="a.example", segments=["auto"])
    h = make_hasher(1024)
    a = h.transform([extract(ctx)])
    b = h.transform([extract(ctx)])
    assert (a != b).nnz == 0


def test_planted_affinity_raises_logit():
    match = ImpressionContext(site_categories=["IAB17"], segments=["sports"])
    miss = ImpressionContext(site_categories=["IAB17"], segments=["finance"])
    assert true_logit(match) > true_logit(miss)


def test_generator_is_seeded():
    a = [c for _, c in generate(200, seed=1)]
    b = [c for _, c in generate(200, seed=1)]
    assert a == b
