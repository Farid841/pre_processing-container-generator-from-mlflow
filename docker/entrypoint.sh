#!/bin/bash
set -e

# Le preprocessing est déjà dans /app/preprocessing/preprocessing.py
# On démarre l'API REST et optionnellement le Kafka processor

# Port par défaut: 8000 (peut être changé via API_PORT)
export API_PORT=${API_PORT:-8000}
export API_HOST=${API_HOST:-0.0.0.0}

# Vérifier si Kafka est activé
KAFKA_ENABLED=${KAFKA_ENABLED:-false}

# Fonction pour arrêter proprement les processus
cleanup() {
    echo "🛑 Shutting down..."
    if [ -n "$API_PID" ]; then
        kill $API_PID 2>/dev/null || true
        wait $API_PID 2>/dev/null || true
    fi
    if [ -n "$KAFKA_PID" ]; then
        kill $KAFKA_PID 2>/dev/null || true
        wait $KAFKA_PID 2>/dev/null || true
    fi
    exit 0
}

# Capturer les signaux pour arrêt propre
trap cleanup SIGTERM SIGINT

if [ "$KAFKA_ENABLED" = "true" ]; then
    echo "🚀 Starting Preprocessing with Kafka integration"
    echo "   API: http://${API_HOST}:${API_PORT}"
    echo "   Documentation: http://localhost:${API_PORT}/docs"
    echo "   Health check:  http://localhost:${API_PORT}/health"
    echo "   Kafka Consumer: ${INPUT_TOPIC:-fink-alert}"
    echo "   Kafka Producer: ${OUTPUT_TOPIC:-preprocessed}"

    # Démarrer l'API en arrière-plan
    python -m runner.api_server &
    API_PID=$!

    # Attendre un peu pour que l'API démarre
    sleep 2

    # Démarrer le Kafka processor en arrière-plan
    python -m runner.kafka_processor &
    KAFKA_PID=$!

    # Attendre que l'un des processus se termine
    wait -n $API_PID $KAFKA_PID
else
    echo "🚀 Starting Preprocessing API only (Kafka disabled)"
    echo "   Documentation: http://localhost:${API_PORT}/docs"
    echo "   Health check:  http://localhost:${API_PORT}/health"
    echo "   To enable Kafka: set KAFKA_ENABLED=true"

    # Démarrer seulement l'API
    exec python -m runner.api_server
fi
