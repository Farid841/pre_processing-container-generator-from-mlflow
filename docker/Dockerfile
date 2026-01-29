# Base image - Python 3.12 (can be overridden via --python-version)
FROM python:3.12-slim

# Labels for container metadata
LABEL maintainer="AstroLab Software"
LABEL description="MLflow Preprocessing Runner"
LABEL version="1.0.0"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system dependencies (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home appuser

# Install Python dependencies (these rarely change = better cache)
RUN pip install --no-cache-dir \
    fastavro>=1.4.0 \
    confluent-kafka>=2.3.0 \
    fastapi>=0.100.0 \
    uvicorn>=0.20.0

# Copy runner code (changes infrequently)
COPY --chown=appuser:appuser runner/ /app/runner/

# Copy kafka_bridge module (needed for Kafka processor)
COPY --chown=appuser:appuser kafka_bridge/ /app/kafka_bridge/

# Copy Avro schemas (needed for Kafka consumer)
COPY --chown=appuser:appuser schemas/ /app/schemas/

# Copy entrypoint script
COPY --chown=appuser:appuser docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Create preprocessing directory (will be filled at build-time)
RUN mkdir -p /app/preprocessing && chown appuser:appuser /app/preprocessing

# Preprocessing will be copied here at build-time
# (via build_scripts/build_image.py which adds these lines dynamically)
# COPY preprocessing/ /app/preprocessing/
# RUN pip install --no-cache-dir -r /app/preprocessing/requirements.txt

# Switch to non-root user
USER appuser

# Expose API port
EXPOSE 8000

# Health check for container orchestrators (K8s, Docker Swarm, etc.)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${API_PORT:-8000}/health || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
