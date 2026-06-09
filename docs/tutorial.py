# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo",
#     "numpy",
#     "pandas",
#     "requests",
#     "scikit-learn",
#     "matplotlib",
# ]
# ///
"""MLflow x Fink quickstart tutorial."""

import marimo

__generated_with = "0.23.9"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _():
    import sys

    IN_WASM = "pyodide" in sys.modules
    return (IN_WASM,)


@app.cell(hide_code=True)
def _(IN_WASM, mo):
    import os as _os

    if IN_WASM:
        _banner = mo.callout(
            mo.md(
                "**Mode démonstration** | navigateur. MLflow est simulé.  \n"
                "Lancez `marimo edit docs/tutorial.py` pour le vrai MLflow."
            ),
            kind="info",
        )
    else:
        _uri = _os.environ.get("MLFLOW_TRACKING_URI", "http://127.0.0.1:5000")
        _user = _os.environ.get("MLFLOW_TRACKING_USERNAME")
        _auth_str = f"authentifié en tant que **{_user}**" if _user else "sans authentification"
        _banner = mo.callout(
            mo.md(f"**Mode local** | connecté à `{_uri}` ({_auth_str})."),
            kind="success",
        )
    _banner
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # MLflow Quickstart

    Entraîner un classifieur sur des alertes ZTF et le déployer sur **Fink** en 6 étapes.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## Étape 1: Configurer MLflow
    """
    )
    return


@app.cell(hide_code=True)
def _(IN_WASM, mo):
    import os

    mo.stop(
        IN_WASM,
        mo.callout(
            mo.md(
                "**MLflow non disponible dans le navigateur.**  \n"
                "Lancez `marimo edit docs/tutorial.py` en local."
            ),
            kind="warn",
        ),
    )

    import mlflow

    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    username = os.environ.get("MLFLOW_TRACKING_USERNAME")
    password = os.environ.get("MLFLOW_TRACKING_PASSWORD")
    if username:
        os.environ["MLFLOW_TRACKING_USERNAME"] = username
    if password:
        os.environ["MLFLOW_TRACKING_PASSWORD"] = password
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("fink-real-bogus")
    return (mlflow,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## Étape 2: Charger des alertes ZTF réelles
    """
    )
    return


@app.cell
def _():
    import numpy as np
    import requests
    from sklearn.model_selection import train_test_split

    FINK_API = "https://api.ztf.fink-portal.org/api/v1/latests"
    COLS = (
        "i:objectId,i:rb,i:drb,i:classtar,i:magpsf,i:sigmapsf,"
        "i:diffmaglim,i:ndethist,i:isdiffpos,i:sgscore1,i:distpsnr1"
    )

    def fetch(class_name, n):
        r = requests.post(
            FINK_API,
            json={"class": class_name, "n": n, "columns": COLS, "format": "json"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def to_alert(row):
        c = {k[2:]: v for k, v in row.items() if k.startswith("i:")}
        return {"candidate": c}

    try:
        real_rows = fetch("SN candidate", 200)
        bogus_rows = fetch("Unknown", 200)
        alerts = [to_alert(r) for r in real_rows] + [to_alert(r) for r in bogus_rows]
        labels = [1] * len(real_rows) + [0] * len(bogus_rows)
        data_source = f"Fink Portal | {len(real_rows)} SN candidates + {len(bogus_rows)} Unknown"
    except Exception:
        import random

        rng = random.Random(42)

        def beta(a, b):
            x, y = rng.gammavariate(a, 1), rng.gammavariate(b, 1)
            return max(0.0, min(1.0, x / (x + y)))

        def make_alert(is_real):
            return {
                "candidate": {
                    "rb": beta(8, 2) if is_real else beta(2, 8),
                    "drb": beta(8, 2) if is_real else beta(2, 8),
                    "classtar": rng.uniform(0, 1),
                    "fwhm": abs(rng.gauss(2.2, 0.3) if is_real else rng.gauss(3.5, 0.8)),
                    "elong": max(1.0, rng.gauss(1.05, 0.05) if is_real else rng.gauss(1.3, 0.3)),
                    "magpsf": rng.gauss(19, 2) if is_real else rng.gauss(18.5, 3),
                    "sigmapsf": abs(rng.gauss(0.15, 0.05) if is_real else rng.gauss(0.3, 0.1)),
                    "diffmaglim": rng.gauss(20, 1),
                    "ndethist": (
                        max(1, int(rng.gauss(8, 3))) if is_real else max(0, int(rng.gauss(1, 1)))
                    ),
                    "scorr": rng.gauss(15.0, 3.0) if is_real else rng.gauss(5.0, 2.0),
                    "chinr": abs(rng.gauss(1.0, 0.15)),
                    "sharpnr": rng.gauss(0.0, 0.1),
                    "sgscore1": rng.uniform(0.0, 0.3) if is_real else rng.uniform(0.7, 1.0),
                    "distpsnr1": rng.uniform(0.1, 3.0),
                    "isdiffpos": "t",
                }
            }

        N = 400
        alerts = [make_alert(i < N // 2) for i in range(N)]
        labels = [1] * (N // 2) + [0] * (N - N // 2)
        data_source = "données simulées (API inaccessible)"
    return alerts, data_source, labels, np, train_test_split


@app.cell(hide_code=True)
def _(alerts, data_source, labels, mo):
    import pandas as pd

    sample = pd.DataFrame([a["candidate"] for a in alerts[:5]]).round(3)
    sample.insert(0, "label", labels[:5])
    mo.vstack(
        [
            mo.md(f"**{len(alerts)} alertes** | source : {data_source}"),
            mo.ui.table(sample, selection=None),
        ]
    )
    return (pd,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## Étape 3: Écrire le preprocessing
    """
    )
    return


@app.cell
def _(alerts, labels, np, train_test_split):
    def pre_processing(alert: dict) -> list:
        """Extrait 18 features d'une alerte ZTF."""
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

    X = np.array([pre_processing(a) for a in alerts])
    y = np.array(labels)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    return FEATURE_NAMES, X, X_test, X_train, pre_processing, y_test, y_train


@app.cell(hide_code=True)
def _(FEATURE_NAMES, X, mo, pd):
    df_feat = pd.DataFrame(X[:5], columns=FEATURE_NAMES).round(3)
    mo.vstack(
        [
            mo.md(f"**Matrice X** | shape `{X.shape}` | 5 premières lignes :"),
            mo.ui.table(df_feat, selection=None),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## Étape 4: Entraîner avec MLflow autologging
    """
    )
    return


@app.cell
def _(X_train, mlflow, y_train):
    from sklearn.ensemble import RandomForestClassifier

    mlflow.sklearn.autolog()

    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=8,
        class_weight="balanced",
        random_state=42,
    )
    model.fit(X_train, y_train)
    return (model,)


@app.cell(hide_code=True)
def _(X_test, mo, model, y_test):
    from sklearn.metrics import accuracy_score, f1_score

    acc = accuracy_score(y_test, model.predict(X_test))
    f1 = f1_score(y_test, model.predict(X_test))
    mo.hstack(
        [
            mo.stat(value=f"{acc:.1%}", label="Accuracy", bordered=True),
            mo.stat(value=f"{f1:.1%}", label="F1-score", bordered=True),
        ]
    )
    return accuracy_score, f1_score


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Étape 5: Logger le modèle et `preprocessing.py`

    L'autologging enregistre le modèle, mais pas votre `preprocessing.py`.
    Ce fichier est **indispensable** : Fink l'exécute sur chaque alerte avant
    d'appeler le modèle. Uploadez-le dans le même run.
    """
    )
    return


@app.cell
def _(
    FEATURE_NAMES,
    X_test,
    X_train,
    accuracy_score,
    f1_score,
    mlflow,
    model,
    y_test,
    y_train,
):
    import pathlib

    from sklearn.metrics import precision_score, recall_score

    mlflow.sklearn.autolog(disable=True)

    with mlflow.start_run(run_name="ztf-real-bogus") as run:
        mlflow.log_params(
            {
                "n_estimators": model.n_estimators,
                "max_depth": model.max_depth,
                "features": str(FEATURE_NAMES),
            }
        )

        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        mlflow.log_metrics(
            {
                "accuracy": float(accuracy_score(y_test, y_pred)),
                "f1_score": float(f1_score(y_test, y_pred)),
                "precision": float(precision_score(y_test, y_pred)),
                "recall": float(recall_score(y_test, y_pred)),
            }
        )

        model_info = mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name="ztf-real-bogus",
        )

        # Preprocessing de production (18 features) — requis par Fink
        _prep = pathlib.Path("training/preprocessing.py")
        mlflow.log_artifact(str(_prep), artifact_path="preprocessing")
        _req = pathlib.Path("training/requirements.txt")
        if _req.exists():
            mlflow.log_artifact(str(_req), artifact_path="preprocessing")
        mlflow.set_tag("type", "real-bogus")
    return model_info, run


@app.cell(hide_code=True)
def _(mo, model_info, run):
    mo.vstack(
        [
            mo.md(f"**Run ID** : `{run.info.run_id}`"),
            mo.md(f"**Model URI** : `{model_info.model_uri}`"),
            mo.md(
                "Consultez [http://127.0.0.1:5000](http://127.0.0.1:5000)"
                " pour voir le run dans l'UI MLflow."
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    ## Étape 6: Charger le modèle et prédire
    """
    )
    return


@app.cell
def _(FEATURE_NAMES, alerts, mlflow, model_info, np, pd, pre_processing):
    test_alerts = alerts[:5]
    X_demo = np.array([pre_processing(a) for a in test_alerts])

    loaded_model = mlflow.pyfunc.load_model(model_info.model_uri)
    scores = loaded_model.predict(X_demo)

    result = pd.DataFrame(X_demo, columns=FEATURE_NAMES).round(3)
    result["score"] = scores.round(4)
    result["label"] = result["score"].apply(lambda s: "real ✓" if s >= 0.5 else "bogus ✗")
    return (result,)


@app.cell(hide_code=True)
def _(mo, result):
    mo.ui.table(result, selection=None)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Étape 7: Lire les résultats produits par Fink

    Une fois votre modèle déployé, les prédictions arrivent dans un topic `fink_ai_*` :

    ```bash
    fink_datainference \
        -topic fink_ai_2026-06-08_ztf-real-bogus \
        -servers kafka.fink-broker.org:9093 \
        -outdir ./mes_resultats
    ```
    """
    )
    return


@app.cell
def _(model, np, pd):
    import random as _rnd

    rng7 = _rnd.Random(55)

    def beta7(a, b):
        x, y = rng7.gammavariate(a, 1), rng7.gammavariate(b, 1)
        return max(0.0, min(1.0, x / (x + y)))

    rows = []
    for i in range(15):
        is_real = rng7.random() > 0.45
        x_vec = np.array(
            [
                [
                    beta7(8, 2) if is_real else beta7(2, 8),  # rb
                    beta7(8, 2) if is_real else beta7(2, 8),  # drb
                    rng7.uniform(0, 1),  # classtar
                    rng7.gauss(19.0, 2.0),  # magpsf
                    abs(rng7.gauss(0.15, 0.05) if is_real else rng7.gauss(0.3, 0.1)),  # sigmapsf
                    rng7.gauss(20.0, 1.0),  # diffmaglim
                    float(  # ndethist
                        max(1, int(rng7.gauss(8, 3))) if is_real else max(0, int(rng7.gauss(1, 1)))
                    ),
                ]
            ]
        )
        score = float(model.predict_proba(x_vec)[0][1])
        rows.append(
            {
                "objectId": f"ZTF2{'1' if is_real else '2'}{i:07d}",
                "candid": rng7.randint(10**9, 10**10 - 1),
                "prediction": round(score, 4),
                "bridge": "ztf-real-bogus@1",
            }
        )

    # En production : df = pd.read_parquet("mes_resultats/", dtype_backend="pyarrow")
    df_results = pd.DataFrame(rows)
    return (df_results,)


@app.cell(hide_code=True)
def _(df_results, mo):
    seuil = 0.7
    n_real = int((df_results["prediction"] >= seuil).sum())
    mo.vstack(
        [
            mo.hstack(
                [
                    mo.stat(value=str(n_real), label=f"Réelles  (≥ {seuil})", bordered=True),
                    mo.stat(value=str(15 - n_real), label="Bogus", bordered=True),
                ]
            ),
            mo.ui.table(df_results, selection=None),
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ---

    ## Prochaines étapes

    - **Améliorer le preprocessing** : ajoutez des features dérivées de `prv_candidates`
    - **Promouvoir un modèle** : `python training/promote.py --model ztf-real-bogus --version 1`
    - **MLflow UI** : `http://127.0.0.1:5000` pour comparer vos runs
    """
    )
    return


if __name__ == "__main__":
    app.run()
