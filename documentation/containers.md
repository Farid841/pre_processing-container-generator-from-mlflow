# Run the containers

## Preprocessing container

By default it starts a FastAPI REST API on port `8000` (change it with `API_PORT`).

```bash
docker run -d -p 8000:8000 preprocessing-ztf-real-bogus:latest
```

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Health check |
| POST | `/preprocess` | One record: `{"data": {...}}` |
| POST | `/preprocess/batch` | Several records: `{"data": [{...}, {...}]}` |
| GET | `/docs` | Swagger UI |

```bash
curl -X POST http://localhost:8000/preprocess \
  -H "Content-Type: application/json" \
  -d '{"data": {"candidate": {"rb": 0.9, "drb": 0.8, "magpsf": 19.1}}}'
```

With `KAFKA_ENABLED=true`, the container **also** starts the Kafka consumer/producer: see [Kafka pipeline](kafka.md).

### stdin / stdout mode (legacy)

```bash
echo '{"test": "data"}' | docker run -i preprocessing-ztf-real-bogus:latest python /app/runner/runner.py
```

Input formats: JSONL, JSON, Avro. Output: JSONL.

## Model container

It starts `mlflow models serve` on port `8080`.

```bash
docker run -d -p 8080:8080 model-ztf-real-bogus:latest

curl http://localhost:8080/ping
curl -X POST http://localhost:8080/invocations \
  -H "Content-Type: application/json" \
  -d '{"inputs": [[0.9, 0.8, 19.1]]}'
```

With `KAFKA_ENABLED=true`, it waits for the server to answer on `/ping`, then starts the Kafka bridge.
