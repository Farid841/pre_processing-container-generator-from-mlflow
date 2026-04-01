"""
ZTF Real/Bogus Classifier — Training Script.

Generates synthetic ZTF alerts (same schema as the Kafka stream),
extracts features via preprocessing.py, trains a RandomForest classifier,
and logs everything to MLflow — including the preprocessing code and its
requirements so the Fink server can reproduce the exact same pipeline.

Usage:
    # Local server:
    mlflow server --host 127.0.0.1 --port 5000
    python training/train.py

    # Remote server (set credentials as env vars first):
    export MLFLOW_TRACKING_URI="https://mlflow-dev.fink-broker.org"
    export MLFLOW_TRACKING_USERNAME="your_username"
    export MLFLOW_TRACKING_PASSWORD="your_password"
    python training/train.py
"""

import os
import random
import sys
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
from mlflow.models import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split

# Ensure preprocessing.py is importable from this script's directory
sys.path.insert(0, str(Path(__file__).parent))
import preprocessing as prep  # noqa: E402

# ── Configuration ─────────────────────────────────────────────────────────────
# Credentials and server URI are read from environment variables so they are
# never hardcoded.  Set them in your shell before running:
#
#   export MLFLOW_TRACKING_URI="https://mlflow-dev.fink-broker.org"  # or local
#   export MLFLOW_TRACKING_USERNAME="your_username"
#   export MLFLOW_TRACKING_PASSWORD="your_password"

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
MLFLOW_TRACKING_USERNAME = os.environ.get("MLFLOW_TRACKING_USERNAME")
MLFLOW_TRACKING_PASSWORD = os.environ.get("MLFLOW_TRACKING_PASSWORD")

EXPERIMENT_NAME = "ztf-real-bogus"
MODEL_NAME = "ztf-real-bogus"

N_SAMPLES = 2000
N_ESTIMATORS = 100
MAX_DEPTH = 10
RANDOM_STATE = 42
TEST_SIZE = 0.2


# ── Simulator ─────────────────────────────────────────────────────────────────
# Produces synthetic alerts that follow the ZTF AVRO 3.3 schema exactly,
# so we can validate the preprocessing logic before using real Parquet data.


def _beta(rng: random.Random, a: float, b: float) -> float:
    """Beta(a, b) sample via the gamma method (no numpy needed)."""
    x = rng.gammavariate(a, 1.0)
    y = rng.gammavariate(b, 1.0)
    return x / (x + y)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _simulate_candidate(rng: random.Random, is_real: bool) -> dict:
    """Return a synthetic candidate dict matching the ZTF AVRO schema."""
    if is_real:
        return {
            "rb": _clamp(_beta(rng, 8, 2), 0.0, 1.0),
            "drb": _clamp(_beta(rng, 8, 2), 0.0, 1.0),
            "classtar": rng.uniform(0.0, 1.0),
            "fwhm": _clamp(rng.gauss(2.5, 0.4), 1.0, 6.0),
            "elong": _clamp(rng.gauss(1.05, 0.05), 1.0, 2.0),
            "magpsf": _clamp(rng.gauss(19.0, 2.0), 15.0, 23.0),
            "sigmapsf": _clamp(abs(rng.gauss(0.04, 0.01)), 0.005, 0.3),
            "diffmaglim": _clamp(rng.gauss(20.5, 0.5), 18.0, 22.0),
            "ndethist": max(1, int(rng.gauss(8, 4))),
            "scorr": _clamp(rng.gauss(12.0, 3.0), 3.0, 40.0),
            "chinr": _clamp(rng.gauss(1.0, 0.2), 0.0, 5.0),
            "sharpnr": _clamp(rng.gauss(0.0, 0.1), -1.0, 1.0),
            "sgscore1": rng.uniform(0.0, 1.0),
            "distpsnr1": _clamp(abs(rng.gauss(0.8, 0.5)), 0.0, 10.0),
            "isdiffpos": "t",
        }
    return {
        "rb": _clamp(_beta(rng, 2, 8), 0.0, 1.0),
        "drb": _clamp(_beta(rng, 2, 8), 0.0, 1.0),
        "classtar": rng.uniform(0.0, 1.0),
        "fwhm": _clamp(rng.gauss(3.8, 1.2), 1.0, 9.0),
        "elong": _clamp(abs(rng.gauss(2.0, 0.8)) + 1.0, 1.0, 5.0),
        "magpsf": _clamp(rng.gauss(18.5, 3.0), 14.0, 23.0),
        "sigmapsf": _clamp(abs(rng.gauss(0.18, 0.10)), 0.01, 0.9),
        "diffmaglim": _clamp(rng.gauss(19.5, 0.8), 17.0, 22.0),
        "ndethist": max(0, int(rng.gauss(1, 1))),
        "scorr": _clamp(rng.gauss(4.0, 2.0), 0.0, 15.0),
        "chinr": _clamp(rng.gauss(2.8, 1.2), 0.0, 10.0),
        "sharpnr": _clamp(rng.gauss(0.5, 0.35), -1.0, 2.0),
        "sgscore1": rng.uniform(0.0, 1.0),
        "distpsnr1": _clamp(abs(rng.gauss(1.8, 1.2)), 0.0, 10.0),
        "isdiffpos": rng.choice(["t", "f"]),
    }


def _simulate_prv_candidates(rng: random.Random, is_real: bool, base_jd: float) -> list:
    """Return a list of synthetic prv_candidate dicts."""
    n = max(0, int(rng.gauss(5, 2))) if is_real else max(0, int(rng.gauss(0.5, 0.8)))
    prv = []
    for _ in range(n):
        prv.append(
            {
                "jd": base_jd - rng.uniform(1.0, 30.0),
                "magpsf": _clamp(rng.gauss(19.0, 0.5), 15.0, 23.0) if is_real else None,
                "isdiffpos": "t" if is_real else rng.choice(["t", "f"]),
                "fid": rng.choice([1, 2]),
            }
        )
    return prv


def simulate_alerts(n: int, random_state: int = 42) -> tuple:
    """Return (alerts, labels) with n/2 real (1) and n/2 bogus (0) alerts.

    Each alert follows the ZTF AVRO 3.3 schema so it can be fed directly
    to pre_processing().
    """
    rng = random.Random(random_state)
    n_real = n // 2
    n_bogus = n - n_real
    base_jd = 2460000.0

    alerts: list = []
    labels: list = []

    for i in range(n_real):
        alerts.append(
            {
                "objectId": f"ZTF21{i:07d}",
                "candid": 1_000_000_000 + i,
                "candidate": _simulate_candidate(rng, True),
                "prv_candidates": _simulate_prv_candidates(rng, True, base_jd),
            }
        )
        labels.append(1)

    for i in range(n_bogus):
        alerts.append(
            {
                "objectId": f"ZTF22{i:07d}",
                "candid": 2_000_000_000 + i,
                "candidate": _simulate_candidate(rng, False),
                "prv_candidates": _simulate_prv_candidates(rng, False, base_jd),
            }
        )
        labels.append(0)

    return alerts, labels


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    """Train and log a ZTF real/bogus classifier to MLflow."""
    print(f"preprocessing.py loaded — {prep.N_FEATURES} features")
    print(f"Features : {prep.FEATURE_NAMES}\n")

    # 1. Simulate alerts
    print(f"Generating {N_SAMPLES} synthetic ZTF alerts...")
    alerts, labels = simulate_alerts(N_SAMPLES, random_state=RANDOM_STATE)
    n_real = labels.count(1)
    n_bogus = labels.count(0)
    print(f"  {n_real} real, {n_bogus} bogus\n")

    # 2. Extract features via preprocessing.py
    print("Applying preprocessing.py...")
    X = np.array([prep.pre_processing(a) for a in alerts])
    y = np.array(labels)
    print(f"  Feature matrix : {X.shape}")
    print(f"  NaN count      : {int(np.isnan(X).sum())}\n")

    # 3. Train / test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y
    )

    # 4. Train
    print(
        f"Training RandomForestClassifier("
        f"n_estimators={N_ESTIMATORS}, max_depth={MAX_DEPTH})..."
    )
    model = RandomForestClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        class_weight="balanced",
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    # 5. Evaluate
    y_pred = model.predict(X_test)
    acc = float(accuracy_score(y_test, y_pred))
    f1 = float(f1_score(y_test, y_pred))
    prec = float(precision_score(y_test, y_pred))
    rec = float(recall_score(y_test, y_pred))
    cm = confusion_matrix(y_test, y_pred)

    print(f"\nMetrics on test set ({len(y_test)} alerts):")
    print(f"  Accuracy  : {acc:.4f}")
    print(f"  F1-score  : {f1:.4f}")
    print(f"  Precision : {prec:.4f}")
    print(f"  Recall    : {rec:.4f}")
    print("   Confusion matrix (rows=actual, cols=predicted):")
    print(f"    TN={cm[0][0]}  FP={cm[0][1]}")
    print(f"    FN={cm[1][0]}  TP={cm[1][1]}\n")

    # 6. Log to MLflow
    training_dir = Path(__file__).parent
    prep_file = training_dir / "preprocessing.py"
    req_file = training_dir / "requirements.txt"

    # Propagate credentials to the MLflow SDK via env vars (safest method —
    # avoids passing secrets through Python objects or CLI args)
    if MLFLOW_TRACKING_USERNAME:
        os.environ["MLFLOW_TRACKING_USERNAME"] = MLFLOW_TRACKING_USERNAME
    if MLFLOW_TRACKING_PASSWORD:
        os.environ["MLFLOW_TRACKING_PASSWORD"] = MLFLOW_TRACKING_PASSWORD

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    print(f"  Server : {MLFLOW_TRACKING_URI}")
    if MLFLOW_TRACKING_USERNAME:
        print(f"  User   : {MLFLOW_TRACKING_USERNAME}")
    else:
        print("  Auth   : none (local server)")

    print("Logging to MLflow...")
    with mlflow.start_run(run_name=MODEL_NAME) as run:

        mlflow.set_tags({"model_name": MODEL_NAME, "type": "real-bogus"})

        mlflow.log_params(
            {
                "n_estimators": N_ESTIMATORS,
                "max_depth": MAX_DEPTH,
                "n_samples": N_SAMPLES,
                "test_size": TEST_SIZE,
                "random_state": RANDOM_STATE,
                "n_features": prep.N_FEATURES,
            }
        )

        mlflow.log_metrics(
            {
                "accuracy": acc,
                "f1_score": f1,
                "precision": prec,
                "recall": rec,
            }
        )

        signature = infer_signature(X_train, model.predict_proba(X_train)[:, 1])
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            signature=signature,
        )

        # Log preprocessing code + requirements so Fink can reproduce
        # the exact same feature extraction on live Kafka alerts
        mlflow.log_artifact(str(prep_file), artifact_path="preprocessing")
        mlflow.log_artifact(str(req_file), artifact_path="preprocessing")

        run_id = run.info.run_id

    print(f"  Run ID     : {run_id}")
    print(f"  View at    : {MLFLOW_TRACKING_URI}\n")
    print("To build the Docker images:")
    print(f"  python build_scripts/build_image.py " f"{run_id} preprocessing-{MODEL_NAME}-1-0-0")
    print(f"  python build_scripts/build_model_image.py " f"models:/{MODEL_NAME}/1 {MODEL_NAME} 1")


if __name__ == "__main__":
    main()
