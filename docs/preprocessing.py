"""Prétraitement des alertes ZTF."""


def pre_processing(alert: dict) -> list:
    """Extrait 18 features d'une alerte ZTF (schéma 3.3)."""
    c = alert.get("candidate") or {}
    prv = alert.get("prv_candidates") or []

    _dp = c.get("isdiffpos")
    isdiffpos = 1.0 if _dp in ("t", "1", 1, True) else 0.0

    mags, jds = [], []
    for p in prv:
        if not isinstance(p, dict):
            continue
        if p.get("isdiffpos") not in ("t", "1", 1, True):
            continue
        m, j = p.get("magpsf"), p.get("jd")
        if m is not None:
            try:
                mags.append(float(m))
            except (TypeError, ValueError):
                pass
        if j is not None:
            try:
                jds.append(float(j))
            except (TypeError, ValueError):
                pass
    n_prev_det = float(len(mags))
    if len(mags) >= 2:
        _mean = sum(mags) / len(mags)
        mag_std = (sum((_m - _mean) ** 2 for _m in mags) / len(mags)) ** 0.5
    else:
        mag_std = 0.0
    time_baseline = (max(jds) - min(jds)) if len(jds) >= 2 else 0.0

    def _sf(v):
        if v is None:
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    return [
        _sf(c.get("rb")),
        _sf(c.get("drb")),
        _sf(c.get("classtar")),
        _sf(c.get("fwhm")),
        _sf(c.get("elong")),
        _sf(c.get("magpsf")),
        _sf(c.get("sigmapsf")),
        _sf(c.get("diffmaglim")),
        _sf(c.get("ndethist")),
        _sf(c.get("scorr")),
        _sf(c.get("chinr")),
        _sf(c.get("sharpnr")),
        _sf(c.get("sgscore1")),
        _sf(c.get("distpsnr1")),
        isdiffpos,
        n_prev_det,
        mag_std,
        time_baseline,
    ]


FEATURE_NAMES = [
    "rb",
    "drb",
    "classtar",
    "fwhm",
    "elong",
    "magpsf",
    "sigmapsf",
    "diffmaglim",
    "ndethist",
    "scorr",
    "chinr",
    "sharpnr",
    "sgscore1",
    "distpsnr1",
    "isdiffpos",
    "n_prev_det",
    "mag_std",
    "time_baseline",
]
