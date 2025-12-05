FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir fastavro>=1.4.0 confluent-kafka>=2.3.0

COPY runner/ /app/runner/

COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

RUN mkdir -p /app/preprocessing

# Preprocessing will be copied here at build-time
# (via build_scripts/build_image.py which adds these lines dynamically)
# COPY preprocessing/ /app/preprocessing/
# RUN pip install --no-cache-dir -r /app/preprocessing/requirements.txt

ENTRYPOINT ["/app/entrypoint.sh"]
