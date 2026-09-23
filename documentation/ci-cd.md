# CI/CD and webhook

![Pipeline triggered by the MLflow webhook](diagrams/webhook-pipeline.drawio.svg)

## Trigger

Training a model does **not** trigger a build. The build starts when a version is promoted:

```bash
python training/promote.py --model ztf-real-bogus --version 3
```

`promote.py`:

1. sets the **`@champion`** alias on that version in the MLflow Model Registry;
2. sends a `repository_dispatch` (the webhook) to GitHub with the `run_id` and `source = models:/ztf-real-bogus@champion`.

To roll back, promote the previous version: the alias moves and the images are rebuilt. Why aliases rather than MLflow stages is explained in `training/PIPELINE_TRIGGER.md`.

## Payload

Only `data.run_id` (required) and `data.source` (optional) are read:

```json
{
  "event_type": "mlflow-model-version",
  "client_payload": {
    "data": {"run_id": "<run_id>", "source": "models:/ztf-real-bogus@champion"}
  }
}
```

Without `source`, only the preprocessing image is built.

## Jobs of the `build-mlflow-images.yml` workflow

| Job | Role |
|---|---|
| `unit-tests` | pytest |
| `build-and-push` | Checks MLflow, runs `./build.sh`, pushes the images to `ghcr.io`; the MLflow base image is cached |
| `smoke-tests` | `/health`, `/preprocess`, `/preprocess/batch` on the preprocessing image |
| `smoke-tests-model` | `/ping`, `/invocations` on the model image (if built) |
| `tag-mlflow-versions` | Writes `preprocessing_image` and `model_image` as tags on the MLflow version |

Other triggers: `workflow_dispatch` (manual, inputs `run_id` and `model_source`) and `push` on `main` (unit tests only).

## Secrets

In the GitHub `CI` environment:

- `MLFLOW_TRACKING_URI`, `MLFLOW_TRACKING_USERNAME`, `MLFLOW_TRACKING_PASSWORD`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL`

For `promote.py`: `GITHUB_TOKEN` (`repo` or `workflow` scope) and `GITHUB_REPO`.
