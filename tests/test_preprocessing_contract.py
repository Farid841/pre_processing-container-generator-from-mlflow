"""
Contract tests for training/preprocessing.py.

These tests run in CI *before* the Docker image build to guarantee:
  - The preprocessing module is importable and has the expected public API
  - pre_processing() returns a list of exactly N_FEATURES floats
  - All edge cases from the ZTF AVRO schema are handled (None, missing fields)
  - requirements.txt is present next to preprocessing.py

A failure here means the image would silently produce wrong-shaped inputs to
the model, which is caught now rather than at container startup.
"""

import ast
import importlib
import sys
import types
from pathlib import Path

import pytest

# ── Resolve paths ─────────────────────────────────────────────────────────────

TRAINING_DIR = Path(__file__).parent.parent / "training"
PREP_FILE = TRAINING_DIR / "preprocessing.py"
REQ_FILE = TRAINING_DIR / "requirements.txt"


def _load_preprocessing() -> types.ModuleType:
    """Import training/preprocessing.py in isolation."""
    if "preprocessing" in sys.modules:
        del sys.modules["preprocessing"]
    spec = importlib.util.spec_from_file_location("preprocessing", PREP_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def prep():
    """Return the preprocessing module."""
    return _load_preprocessing()


@pytest.fixture
def minimal_alert():
    """Minimal valid ZTF AVRO alert — only mandatory candidate fields."""
    return {
        "objectId": "ZTF21test001",
        "candid": 1234567890,
        "candidate": {
            "rb": 0.92,
            "drb": 0.87,
            "magpsf": 19.3,
            "isdiffpos": "t",
        },
    }


@pytest.fixture
def full_alert():
    """Full ZTF AVRO 3.3 alert with prv_candidates."""
    return {
        "objectId": "ZTF21full001",
        "candid": 9876543210,
        "candidate": {
            "rb": 0.92,
            "drb": 0.87,
            "classtar": 0.1,
            "fwhm": 2.4,
            "elong": 1.02,
            "magpsf": 19.3,
            "sigmapsf": 0.04,
            "diffmaglim": 20.8,
            "ndethist": 3,
            "scorr": 12.1,
            "chinr": 0.98,
            "sharpnr": 0.01,
            "sgscore1": 0.7,
            "distpsnr1": 0.3,
            "isdiffpos": "t",
        },
        "prv_candidates": [
            {"jd": 2459995.5, "magpsf": 19.1, "isdiffpos": "t", "fid": 1},
            {"jd": 2459990.3, "magpsf": 19.4, "isdiffpos": "t", "fid": 2},
            {"jd": 2459985.1, "magpsf": None, "isdiffpos": "f", "fid": 1},
        ],
    }


# ── 1. File presence ──────────────────────────────────────────────────────────


class TestFilesPresent:
    """Verify the required files exist before attempting any import."""

    def test_preprocessing_file_exists(self):
        """Raise clearly if preprocessing.py is missing from training/."""
        assert PREP_FILE.exists(), (
            f"preprocessing.py not found at {PREP_FILE}. "
            "This file must exist before building the Docker image."
        )

    def test_requirements_file_exists(self):
        """Raise clearly if requirements.txt is missing from training/."""
        assert REQ_FILE.exists(), (
            f"requirements.txt not found at {REQ_FILE}. "
            "This file must be logged as an MLflow artifact alongside preprocessing.py."
        )


# ── 2. Module API contract ────────────────────────────────────────────────────


class TestModuleContract:
    """Verify the public API that the Kafka bridge depends on."""

    def test_module_is_importable(self, prep):
        """Import training/preprocessing.py without errors."""
        assert prep is not None

    def test_pre_processing_function_exists(self, prep):
        """pre_processing must be a callable at module level."""
        assert hasattr(prep, "pre_processing"), (
            "pre_processing() function not found in preprocessing.py. "
            "The Kafka bridge calls prep.pre_processing(alert) on every message."
        )
        assert callable(prep.pre_processing)

    def test_feature_names_exported(self, prep):
        """FEATURE_NAMES must be a non-empty list of strings."""
        assert hasattr(prep, "FEATURE_NAMES")
        assert isinstance(prep.FEATURE_NAMES, list)
        assert len(prep.FEATURE_NAMES) > 0
        assert all(isinstance(n, str) for n in prep.FEATURE_NAMES)

    def test_n_features_exported(self, prep):
        """N_FEATURES must equal len(FEATURE_NAMES)."""
        assert hasattr(prep, "N_FEATURES")
        assert prep.N_FEATURES == len(prep.FEATURE_NAMES), (
            f"N_FEATURES={prep.N_FEATURES} does not match "
            f"len(FEATURE_NAMES)={len(prep.FEATURE_NAMES)}"
        )

    def test_no_heavy_dependencies(self):
        """preprocessing.py must only use the standard library (no numpy/pandas).

        The preprocessing container is kept minimal — adding a heavy dependency
        here means adding it to requirements.txt and rebuilding the image.
        """
        source = PREP_FILE.read_text()
        tree = ast.parse(source)
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split(".")[0])

        heavy = {"numpy", "pandas", "scipy", "sklearn", "torch", "tensorflow"}
        found = heavy & imports
        assert not found, (
            f"preprocessing.py imports heavy packages: {found}. "
            f"Add them to requirements.txt or remove the dependency."
        )


# ── 3. Output shape contract ──────────────────────────────────────────────────


class TestOutputShape:
    """pre_processing() must always return a list of exactly N_FEATURES floats."""

    def test_full_alert_returns_correct_length(self, prep, full_alert):
        """Full alert with all fields returns N_FEATURES values."""
        result = prep.pre_processing(full_alert)
        assert isinstance(result, list)
        assert len(result) == prep.N_FEATURES, (
            f"Expected {prep.N_FEATURES} features, got {len(result)}. "
            f"Feature vector shape changed — retrain the model."
        )

    def test_all_values_are_floats(self, prep, full_alert):
        """Every element of the feature vector must be a float."""
        result = prep.pre_processing(full_alert)
        non_floats = [
            (i, type(v).__name__, v) for i, v in enumerate(result) if not isinstance(v, float)
        ]
        assert not non_floats, (
            f"Non-float values in feature vector: {non_floats}. " f"The model expects all floats."
        )

    def test_minimal_alert_returns_correct_length(self, prep, minimal_alert):
        """Alert with only mandatory fields still returns N_FEATURES values."""
        result = prep.pre_processing(minimal_alert)
        assert len(result) == prep.N_FEATURES

    def test_empty_candidate_returns_correct_length(self, prep):
        """Missing candidate dict returns N_FEATURES zeros, not an error."""
        result = prep.pre_processing({"objectId": "ZTF21x", "candid": 1})
        assert len(result) == prep.N_FEATURES

    def test_none_fields_become_zero(self, prep):
        """None values in candidate fields must default to 0.0."""
        alert = {"candidate": {"rb": None, "drb": None, "magpsf": None}}
        result = prep.pre_processing(alert)
        assert result[0] == 0.0  # rb
        assert result[1] == 0.0  # drb
        assert result[5] == 0.0  # magpsf


# ── 4. prv_candidates contract ────────────────────────────────────────────────


class TestPrvCandidates:
    """Derived features from prv_candidates must be computed correctly."""

    def test_positive_detections_counted(self, prep):
        """Only isdiffpos='t' detections with a magpsf are counted."""
        alert = {
            "candidate": {"isdiffpos": "t"},
            "prv_candidates": [
                {"jd": 2460000.0, "magpsf": 19.0, "isdiffpos": "t"},
                {"jd": 2459995.0, "magpsf": 19.5, "isdiffpos": "t"},
                {"jd": 2459990.0, "magpsf": 18.8, "isdiffpos": "f"},  # negative — ignored
                {"jd": 2459985.0, "magpsf": None, "isdiffpos": "t"},  # no mag — ignored
            ],
        }
        result = prep.pre_processing(alert)
        idx_n_prev_det = prep.FEATURE_NAMES.index("n_prev_det")
        assert result[idx_n_prev_det] == 2.0

    def test_empty_prv_candidates_gives_zeros(self, prep, minimal_alert):
        """No prv_candidates → n_prev_det=0, mag_std=0, time_baseline=0."""
        minimal_alert["prv_candidates"] = []
        result = prep.pre_processing(minimal_alert)
        idx = prep.FEATURE_NAMES.index("n_prev_det")
        assert result[idx] == 0.0  # n_prev_det
        assert result[idx + 1] == 0.0  # mag_std
        assert result[idx + 2] == 0.0  # time_baseline

    def test_time_baseline_computed(self, prep):
        """time_baseline = max(jd) - min(jd) of positive detections."""
        alert = {
            "candidate": {},
            "prv_candidates": [
                {"jd": 2460010.0, "magpsf": 19.0, "isdiffpos": "t"},
                {"jd": 2459990.0, "magpsf": 19.5, "isdiffpos": "t"},
            ],
        }
        result = prep.pre_processing(alert)
        idx = prep.FEATURE_NAMES.index("time_baseline")
        assert abs(result[idx] - 20.0) < 1e-6

    def test_isdiffpos_encoding(self, prep):
        """A isdiffpos 't' → 1.0, 'f' → 0.0."""
        idx = prep.FEATURE_NAMES.index("isdiffpos")

        alert_pos = {"candidate": {"isdiffpos": "t"}}
        assert prep.pre_processing(alert_pos)[idx] == 1.0

        alert_neg = {"candidate": {"isdiffpos": "f"}}
        assert prep.pre_processing(alert_neg)[idx] == 0.0
