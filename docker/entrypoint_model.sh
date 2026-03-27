#!/bin/bash
set -e

export MODEL_PORT=${MODEL_PORT:-8080}
export MODEL_PATH=${MODEL_PATH:-/opt/ml/model}
KAFKA_ENABLED=${KAFKA_ENABLED:-false}

# Graceful shutdown handler
cleanup() {
    echo "Shutting down..."
    if [ -n "$MODEL_PID" ]; then
        kill "$MODEL_PID" 2>/dev/null || true
        wait "$MODEL_PID" 2>/dev/null || true
    fi
    if [ -n "$BRIDGE_PID" ]; then
        kill "$BRIDGE_PID" 2>/dev/null || true
        wait "$BRIDGE_PID" 2>/dev/null || true
    fi
    exit 0
}

trap cleanup SIGTERM SIGINT

echo "Starting MLflow model server on port ${MODEL_PORT}..."
echo "  Model path: ${MODEL_PATH}"

mlflow models serve \
    -m "${MODEL_PATH}" \
    --host 0.0.0.0 \
    --port "${MODEL_PORT}" \
    --no-conda &
MODEL_PID=$!

if [ "$KAFKA_ENABLED" = "true" ]; then
    echo "Kafka enabled - waiting for model server to be ready..."

    RETRIES=0
    MAX_RETRIES=30
    until curl -sf "http://localhost:${MODEL_PORT}/ping" > /dev/null 2>&1; do
        RETRIES=$((RETRIES + 1))
        if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
            echo "Model server did not start within $((MAX_RETRIES * 2))s - aborting"
            kill "$MODEL_PID" 2>/dev/null || true
            exit 1
        fi
        sleep 2
    done

    echo "Model server ready."

    # Point the bridge at the local model server
    export API_URL="http://localhost:${MODEL_PORT}"
    export API_ENDPOINT="${API_ENDPOINT:-/invocations}"
    export API_HEALTH_ENDPOINT="/ping"
    export PYTHONPATH="/app:${PYTHONPATH}"

    echo "Starting Kafka bridge..."
    echo "  Input topic:  ${INPUT_TOPIC:-preprocessed}"
    echo "  Output topic: ${OUTPUT_TOPIC:-predictions}"
    echo "  Model API:    ${API_URL}${API_ENDPOINT}"

    python -m kafka_bridge.bridge &
    BRIDGE_PID=$!

    # Exit as soon as either process dies
    wait -n "$MODEL_PID" "$BRIDGE_PID"
else
    echo "Kafka disabled - model server only"
    echo "  Invocations: http://0.0.0.0:${MODEL_PORT}/invocations"
    echo "  Health:      http://0.0.0.0:${MODEL_PORT}/ping"
    wait "$MODEL_PID"
fi
