# Build the images

## Prerequisites

```bash
pip install pdm && pdm install -G build
export MLFLOW_TRACKING_URI="https://mlflow.example.org"
export MLFLOW_TRACKING_USERNAME="..."   # if the server requires authentication
export MLFLOW_TRACKING_PASSWORD="..."
```

For artifacts stored on S3, also set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_ENDPOINT_URL`.

## `build.sh`

This is the entry point, also used by the CI.

```bash
# Preprocessing only
./build.sh <run_id>

# Preprocessing + model
./build.sh <run_id> --model-source models:/ztf-real-bogus@champion
```

| Option | Description |
|---|---|
| `--model-source` | MLflow model URI; when set, the model image is built too |
| `--preprocessing-path` | Path of the preprocessing in the artifacts (default: auto-detect) |
| `--python-version` | Python version of the image (e.g. `3.11`) |
| `--dockerfile` | Preprocessing Dockerfile (default: `docker/Dockerfile`) |
| `--push` | Pushes the images to `ghcr.io` (automatic in CI) |

## Image naming

The version lives in the **tag**, not in the name:

- names: `preprocessing-<model_name>` and `model-<model_name>`
- tags: `latest`, `v<version>`, `v<version>-<run_id[:8]>` (the last one is immutable)

## How the model image is built

In two steps (`build_scripts/build_model_image.py`):

1. `mlflow models build-docker` produces a base image (model + MLflow server);
2. `docker/Dockerfile.model` starts from that image and adds the Kafka bridge.

## Calling the scripts directly

```bash
# Preprocessing
python -m build_scripts.build_image <run_id> auto [--tag v1] [--preprocessing-path code/preprocessing.py]

# Model
python -m build_scripts.build_model_image <model_source> <model_name> <model_version> [--tag v3] [--skip-base-build]
```

`python -m build_scripts.build_image --help` lists every option.
