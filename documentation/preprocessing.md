# Write a preprocessing

## The contract

`preprocessing.py` must expose a `pre_processing(data)` function:

- **input**: one alert (a `dict`, e.g. a ZTF alert decoded from Avro, without cutouts)
- **output**: the feature vector expected by the model (a list of numbers)

```python
def pre_processing(data: dict) -> list[float]:
    candidate = data["candidate"]
    return [candidate["rb"], candidate["drb"], candidate["magpsf"]]
```

The contract is checked by `tests/test_preprocessing_contract.py`. A full example lives in `examples/simple/simple_preprocessing.py`.

## What to log in MLflow

As artifacts of the MLflow run:

| Artifact | Required | Role |
|---|---|---|
| `preprocessing.py` | yes | Preprocessing code |
| `requirements.txt` | no | Python dependencies (pandas, numpy…) installed in the image |
| other `.py` files in the same folder | no | Modules imported by `preprocessing.py` (copied automatically) |

If `preprocessing.py` sits in a subfolder of the artifacts (e.g. `code/preprocessing.py`), pass `--preprocessing-path code/preprocessing.py` at build time.

## MLflow tags used

Image names and versions are read from the run tags:

| Tag | Used for | Default |
|---|---|---|
| `model_name` | Image names (`preprocessing-<model_name>`, `model-<model_name>`) | `mlflow.runName` |
| `version` (or `model_version`) | Image tags `v<version>` | first 8 characters of the `run_id` |
