"""
Tests for model training reproducibility.

The original code called np.random.seed(42) inside the data generator, so
every retrain drew the identical sample and produced the identical model.
The leader retrained every five minutes and uploaded the same artifact
forever. Seeding is now opt-in: the image's baked-in model passes a seed,
periodic retrains do not.

Run with pytest, or directly:  python tests/test_train.py
"""

import os
import sys
import warnings

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from model.train import generate_normal_data, train_model

warnings.filterwarnings("ignore", message="X does not have valid feature names")

# A healthy host and an obviously unhealthy one.
NORMAL_POINT = [41.2, 48.7, 118.0, 0.004]
ANOMALOUS_POINT = [95.0, 97.0, 900.0, 0.5]
PROBE = [NORMAL_POINT, ANOMALOUS_POINT]

SAMPLES = 400  # Small enough to keep the suite fast


def test_seeded_training_is_reproducible():
    a = train_model(num_samples=SAMPLES, seed=42).decision_function(PROBE)
    b = train_model(num_samples=SAMPLES, seed=42).decision_function(PROBE)
    assert np.allclose(a, b), "same seed must give the same model"


def test_unseeded_training_varies():
    """Without this, the periodic retrain is a no-op."""
    a = train_model(num_samples=SAMPLES).decision_function(PROBE)
    b = train_model(num_samples=SAMPLES).decision_function(PROBE)
    assert not np.allclose(a, b), "unseeded retrains must produce a new model"


def test_seeded_data_is_reproducible():
    a = generate_normal_data(SAMPLES, seed=7)
    b = generate_normal_data(SAMPLES, seed=7)
    assert a.equals(b)


def test_unseeded_data_varies():
    a = generate_normal_data(SAMPLES)
    b = generate_normal_data(SAMPLES)
    assert not a.equals(b)


def test_model_separates_normal_from_anomalous():
    """decision_function is positive for inliers, negative for outliers."""
    scores = train_model(num_samples=SAMPLES, seed=42).decision_function(PROBE)
    assert scores[0] > 0, "a healthy point should score as normal"
    assert scores[1] < 0, "a spiking point should score as anomalous"


def test_generated_metrics_stay_in_valid_ranges():
    df = generate_normal_data(SAMPLES, seed=1)
    assert df["cpu_usage"].between(0, 100).all()
    assert df["memory_usage"].between(0, 100).all()
    assert df["request_latency"].between(20, 1000).all()
    assert df["error_rate"].between(0, 1).all()


if __name__ == "__main__":
    import contextlib
    import io

    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            with contextlib.redirect_stdout(io.StringIO()):  # training is chatty
                fn()
            print(f"PASS  {name}")
        except AssertionError as e:
            print(f"FAIL  {name}: {e}")
            failures += 1
    sys.exit(1 if failures else 0)
