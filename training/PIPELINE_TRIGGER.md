# Pipeline Trigger — How a Model Version Becomes a Docker Image

## What MLflow says about model versioning

MLflow's Model Registry documentation defines three mechanisms for labeling and promoting model versions. Understanding the difference is key to choosing the right trigger.

### Tags (informational, not a trigger)

> *"Tags are key-value pairs that you associate with registered models and model versions, allowing you to label and categorize them by function or status."*

Tags are metadata. You can set them on any version to add context:

```python
client.set_model_version_tag("ztf-real-bogus", "3", "validated_by", "farid")
client.set_model_version_tag("ztf-real-bogus", "3", "dataset", "synthetic-v2")
```

MLflow does fire a webhook event on `MODEL_VERSION_TAG_SET`, but tags have no semantic meaning in the registry — they are free-form. Using a tag as a deploy trigger requires agreeing on a convention (e.g. `deploy=true`) and is fragile.

### Stages — deprecated since MLflow 2.9

> *"Model stages are deprecated and will be removed in a future major release. We encourage you to leverage Model Aliases instead."*
> — MLflow documentation

`Staging` and `Production` stages still exist in MLflow 3.x for backwards compatibility but should not be used in new projects.

### Aliases — the recommended trigger (MLflow 2.9+)

> *"Model aliases allow you to assign a mutable, named reference to a particular version of a registered model. [...] You can use aliases to indicate the deployment status of a model version."*

> *"For example, you could assign a `champion` alias to the model version currently in production and target this alias in workloads that use the production model."*

Aliases are:
- **Mutable** — re-pointing `@champion` from v2 to v3 is atomic
- **Audited** — every alias change is recorded in the registry history
- **Semantic** — `@champion` has an agreed meaning across the team
- **Referenceable in code** — `models:/ztf-real-bogus@champion` always resolves to the current production version

This is the mechanism we use as the deploy trigger.

---

## Overview

Training a model does **not** automatically trigger a build.
The build is triggered explicitly when you decide a version is ready for production.

```
train.py                       promote.py
   │                               │
   │  registers model              │  1. sets alias @champion → version N
   │  version 1, 2, 3...           │  2. calls GitHub repository_dispatch
   │  (no build)                   │
   ▼                               ▼
MLflow Model Registry      GitHub Actions
                           build-mlflow-images.yml
                                   │
                                   ▼
                           Docker images pushed to ghcr.io
```

---

## Step 1 — Train and register

```bash
python training/train.py
```

Every run registers a new model version automatically:

| Run | Model version | Build triggered? |
|-----|--------------|-----------------|
| 1st | `ztf-real-bogus v1` | ❌ no |
| 2nd | `ztf-real-bogus v2` | ❌ no |
| 3rd | `ztf-real-bogus v3` | ❌ no |

Compare versions in the MLflow UI, pick the best one.

---

## Step 2 — Promote to production

When you are satisfied with a version, run:

```bash
# Required env vars
export MLFLOW_TRACKING_URI="https://mlflow-dev.fink-broker.org"
export MLFLOW_TRACKING_USERNAME="your_username"
export MLFLOW_TRACKING_PASSWORD="your_password"
export GITHUB_TOKEN="ghp_..."   # needs 'repo' or 'workflow' scope
export GITHUB_REPO="org/repo"   # e.g. astrolabsoftware/fink-ml-models

python training/promote.py --model ztf-real-bogus --version 3
```

This does two things atomically:
1. Sets the `@champion` alias on `ztf-real-bogus v3` in MLflow
2. Fires `repository_dispatch` on GitHub → triggers `build-mlflow-images.yml`

---

## What the build receives

The GitHub Actions workflow receives:

```json
{
  "event_type": "mlflow-model-version",
  "client_payload": {
    "data": {
      "run_id": "<run_id of version 3>",
      "source": "models:/ztf-real-bogus@champion"
    }
  }
}
```

---

## Why `@champion` and not stages?

MLflow stages (`Staging`, `Production`) are **deprecated since MLflow 2.9**.
Aliases are the replacement:

| Old (deprecated) | New |
|-----------------|-----|
| transition to Production | set alias `@champion` |
| transition to Staging | set alias `@candidate` |
| transition to Archived | remove alias |

The alias `@champion` means: **"this is the version I want running in production right now"**.
Changing the alias from v2 → v3 is a one-line operation and is fully audited in MLflow.

---

## Environment variables reference

| Variable | Where set | Required for |
|----------|-----------|-------------|
| `MLFLOW_TRACKING_URI` | shell / `.env` | `train.py`, `promote.py` |
| `MLFLOW_TRACKING_USERNAME` | shell / `.env` | remote server only |
| `MLFLOW_TRACKING_PASSWORD` | shell / `.env` | remote server only |
| `GITHUB_TOKEN` | shell / CI secret | `promote.py` |
| `GITHUB_REPO` | shell / CI secret | `promote.py` |

> ⚠️ Never hardcode credentials in code or commit them to git.

---

## Rollback

To rollback to a previous version:

```bash
python training/promote.py --model ztf-real-bogus --version 2
```

This re-points `@champion` to v2 and triggers a new build of the old version.
