# Development

## Tests

```bash
pdm install -G test
pdm run test        # unit tests
pdm run test-all    # + integration tests
```

## Local MLflow

```bash
mlflow ui --backend-store-uri file://./mlruns --host 127.0.0.1 --port 5000
python dev_usage/setup_mlflow_local.py
```

The script creates a run with `preprocessing.py`, `requirements.txt` and a scikit-learn model, then prints the `run_id` to use with `build.sh`.

## Local Kafka

```bash
docker compose -f dev_usage/docker-compose.kafka.yml up -d
```

Kafka listens on `localhost:29092`, Kafka UI on `http://localhost:8085`.

## Documentation

```bash
pip install zensical
zensical serve      # http://localhost:8000
```

The sources are in `documentation/`. The diagrams live in `assets/` (`.drawio` and `.drawio.svg`, both editable in draw.io). After editing a `.drawio` file, re-export the SVG from draw.io (*File → Export as → SVG*, with "Include a copy of my diagram").
