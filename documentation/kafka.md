# Kafka pipeline

![Kafka pipeline](diagrams/kafka-pipeline.drawio.svg)

## The steps

### 1. Input topic

ZTF alerts arrive as **Avro** in a topic (e.g. `fink_alerts`).

### 2. Preprocessing container (`runner/kafka_processor.py`)

1. **Consumes** a batch of alerts.
2. **Decodes the Avro** with the schema (file `AVRO_SCHEMA_PATH`, or read from the `SCHEMA_TOPIC` topic) and drops the cutouts (`SKIP_CUTOUTS=true`) to keep messages small.
3. Calls **`pre_processing(alert)`** directly in-process (no HTTP call).
4. **Produces** `{objectId, candid, features}` as JSON to the intermediate topic, keyed by `objectId`.
5. **Commits** the offsets. An alert that makes `pre_processing` fail is logged and skipped.

### 3. Intermediate topic

It holds the features that are already computed (e.g. `fink-ztf-real-bogus-preprocessed`). This is what makes it possible to **never recompute the preprocessing** when the model has a problem: the topic is simply read again.

### 4. Model container (`kafka_bridge/bridge.py`)

1. **Consumes** a batch (`BATCH_SIZE` messages or `BATCH_TIMEOUT_MS`).
2. **Extracts** the feature vectors.
3. Calls **`POST /invocations`** on the local MLflow server (`:8080`), with retries (`API_RETRY_COUNT`).
4. **Produces** `{result, source: {objectId, candid}, bridge}` to the output topic.
5. **Commits** the offsets **only if the model call succeeded**.

### 5. Output topic

The predictions (e.g. `fink-ztf-real-bogus-results`).

!!! warning "Current limitations"
    - If a batch fails at the model but the next batch succeeds, the second commit moves past the first batch's offset: the failed batch is only replayed if the container restarts **before** the next commit, or after a manual reset of the consumer group offsets.
    - `DEAD_LETTER_TOPIC` exists in the configuration, but no message is sent to it yet.

## Configuration

Both containers read the same variables (`kafka_bridge/config.py`).

| Variable | Preprocessing | Model | Description |
|---|---|---|---|
| `KAFKA_ENABLED` | `true` | `true` | Enables Kafka mode |
| `KAFKA_BOOTSTRAP_SERVERS` | required | required | Kafka brokers |
| `INPUT_TOPIC` | `fink-alert` | `preprocessed` | Consumed topic |
| `OUTPUT_TOPIC` | `preprocessed` | `predictions` | Produced topic |
| `INPUT_FORMAT` | `avro` | `json` | `avro`, `json` or `auto` |
| `OUTPUT_FORMAT` | `json` | `json` | `json` or `avro` |
| `AVRO_SCHEMA_PATH` / `SCHEMA_TOPIC` | one of them if Avro | — | Input schema |
| `SKIP_CUTOUTS` | `true` | `true` | Removes images from alerts |
| `CONSUMER_GROUP_ID` | `preprocessing-group` | `kafka-bridge-group` | Consumer group |
| `AUTO_OFFSET_RESET` | `earliest` | `earliest` | Where to start when no offset is committed |
| `BATCH_SIZE` / `BATCH_TIMEOUT_MS` | `10` / `1000` | `10` / `1000` | Batch size and wait time |
| `API_ENDPOINT` | — | `/invocations` | Endpoint called by the bridge |
| `API_TIMEOUT` / `API_RETRY_COUNT` | — | `30` / `3` | HTTP timeout and retries |
| `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*` | optional | optional | Kafka authentication |

## Try it locally

```bash
# Kafka + Kafka UI (http://localhost:8085)
docker compose -f dev_usage/docker-compose.kafka.yml up -d

docker run -d --name preprocessing --network host \
  -e KAFKA_ENABLED=true -e KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
  -e INPUT_TOPIC=fink_alerts -e OUTPUT_TOPIC=preprocessed \
  -e AVRO_SCHEMA_PATH=/app/schemas/ztf_alert_v3.3.avsc \
  preprocessing-ztf-real-bogus:latest

docker run -d --name model --network host \
  -e KAFKA_ENABLED=true -e KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \
  -e INPUT_TOPIC=preprocessed -e OUTPUT_TOPIC=predictions -e INPUT_FORMAT=json \
  model-ztf-real-bogus:latest
```

Messages can be inspected in Kafka UI, or with `kafka-console-consumer` on the `predictions` topic.
