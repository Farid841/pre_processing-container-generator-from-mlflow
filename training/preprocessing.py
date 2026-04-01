"""
ZTF Alert Preprocessing Module.

Extracts a fixed-length feature vector from a raw ZTF AVRO alert dict,
as received from the Kafka stream (schema version 3.3, no Fink enrichment).

Input format::

    {
        "objectId": "ZTF21abcdefg",
        "candid": 1234567890,
        "candidate": {
            "rb": 0.92, "drb": 0.87, "classtar": 0.1,
            "fwhm": 2.4, "elong": 1.02,
            "magpsf": 19.3, "sigmapsf": 0.04,
            "diffmaglim": 20.8, "ndethist": 3, "scorr": 12.1,
            "chinr": 0.98, "sharpnr": 0.01,
            "sgscore1": 0.7, "distpsnr1": 0.3,
            "isdiffpos": "t",
            ...
        },
        "prv_candidates": [
            {"jd": 2459000.5, "magpsf": 19.1, "isdiffpos": "t", ...},
            ...
        ]
    }

Output::

    [rb, drb, classtar, fwhm, elong, magpsf, sigmapsf, diffmaglim,
     ndethist, scorr, chinr, sharpnr, sgscore1, distpsnr1, isdiffpos,
     n_prev_det, mag_std, time_baseline]
    # 18 floats — one per entry in FEATURE_NAMES
"""

from typing import Any

# Ordered list of features.  Changing this list changes the vector shape — retrain the model.
FEATURE_NAMES: list[str] = [
    # ── from candidate ──────────────────────────────────────────────────────
    "rb",  # RealBogus score (ZTF RF classifier)         [0, 1]
    "drb",  # Deep RealBogus score (CNN)                   [0, 1]
    "classtar",  # Star/Galaxy separator (SExtractor)           [0, 1]
    "fwhm",  # PSF FWHM [pixels]                           > 0
    "elong",  # Elongation aimage/bimage                    >= 1
    "magpsf",  # PSF magnitude [mag]                         ~15-23
    "sigmapsf",  # 1-sigma uncertainty on magpsf               > 0
    "diffmaglim",  # 5-sigma limiting mag in diff image          ~18-22
    "ndethist",  # Prior detections in survey history          >= 0
    "scorr",  # Peak S/N in matched-filter detection image  > 0
    "chinr",  # Chi of nearest ref-image source             > 0
    "sharpnr",  # Sharpness of nearest ref-image source       [-1, 1]
    "sgscore1",  # PS1 star/galaxy score of nearest source     [0, 1]
    "distpsnr1",  # Distance to nearest PS1 source [arcsec]    >= 0
    "isdiffpos",  # Positive subtraction flag (1=pos, 0=neg)    {0, 1}
    # ── derived from prv_candidates ─────────────────────────────────────────
    "n_prev_det",  # Number of previous positive detections      >= 0
    "mag_std",  # Std-dev of previous PSF magnitudes [mag]    >= 0
    "time_baseline",  # Time span of previous detections [days]     >= 0
]

N_FEATURES: int = len(FEATURE_NAMES)


# ── helpers ───────────────────────────────────────────────────────────────────


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Return float(value), or default when value is None or non-numeric."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _encode_isdiffpos(value: Any) -> float:
    """Encode isdiffpos to 1.0 (positive subtraction) or 0.0."""
    return 1.0 if value in ("t", "1", 1, True) else 0.0


def _prv_features(prv_candidates: Any) -> tuple:
    """Compute (n_prev_det, mag_std, time_baseline) from prv_candidates list.

    Only positive-subtraction detections with a measured magnitude are counted.
    """
    if not prv_candidates:
        return 0.0, 0.0, 0.0

    mags: list = []
    jds: list = []

    for prv in prv_candidates:
        if not isinstance(prv, dict):
            continue
        if prv.get("isdiffpos") not in ("t", "1", 1, True):
            continue
        mag = prv.get("magpsf")
        jd = prv.get("jd")
        if mag is not None:
            try:
                mags.append(float(mag))
            except (TypeError, ValueError):
                pass
        if jd is not None:
            try:
                jds.append(float(jd))
            except (TypeError, ValueError):
                pass

    n_prev_det = float(len(mags))

    if len(mags) >= 2:
        mean = sum(mags) / len(mags)
        mag_std = (sum((m - mean) ** 2 for m in mags) / len(mags)) ** 0.5
    else:
        mag_std = 0.0

    time_baseline = (max(jds) - min(jds)) if len(jds) >= 2 else 0.0

    return n_prev_det, mag_std, time_baseline


# ── public API ────────────────────────────────────────────────────────────────


def pre_processing(alert: dict) -> list:
    """Extract a fixed-length feature vector from a raw ZTF AVRO alert dict.

    Args:
        alert: Raw ZTF alert dict with a nested ``candidate`` record and an
               optional ``prv_candidates`` list, as produced by the ZTF Kafka
               stream (schema version 3.3).

    Returns:
        List of :data:`N_FEATURES` floats in the order of
        :data:`FEATURE_NAMES`.  Missing or ``None`` fields default to ``0.0``.

    Examples:
        >>> alert = {"candidate": {"rb": 0.9, "drb": 0.85, "magpsf": 19.3}}
        >>> features = pre_processing(alert)
        >>> len(features) == N_FEATURES
        True
        >>> features[0]  # rb
        0.9
    """
    c = alert.get("candidate") or {}

    candidate_features = [
        _safe_float(c.get("rb")),
        _safe_float(c.get("drb")),
        _safe_float(c.get("classtar")),
        _safe_float(c.get("fwhm")),
        _safe_float(c.get("elong")),
        _safe_float(c.get("magpsf")),
        _safe_float(c.get("sigmapsf")),
        _safe_float(c.get("diffmaglim")),
        _safe_float(c.get("ndethist")),
        _safe_float(c.get("scorr")),
        _safe_float(c.get("chinr")),
        _safe_float(c.get("sharpnr")),
        _safe_float(c.get("sgscore1")),
        _safe_float(c.get("distpsnr1")),
        _encode_isdiffpos(c.get("isdiffpos")),
    ]

    n_prev_det, mag_std, time_baseline = _prv_features(alert.get("prv_candidates") or [])

    return candidate_features + [n_prev_det, mag_std, time_baseline]
