#!/bin/bash
# Copyright 2025 AstroLab Software
# Author: Farid MAMAN
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Build Docker image for MLflow model serving
# Usage: ./build_model_image.sh <run_id> <image_name> [tag]

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Help
if [ "$1" = "--help" ] || [ "$1" = "-h" ] || [ -z "$1" ]; then
    echo "Usage: ./build_model_image.sh <run_id> <image_name> [tag]"
    echo ""
    echo "Arguments:"
    echo "  run_id      MLflow run ID (required)"
    echo "  image_name  Docker image name (required)"
    echo "  tag         Docker image tag (default: latest)"
    echo ""
    echo "Environment variables:"
    echo "  MLFLOW_TRACKING_URI  MLflow server URL (required)"
    echo ""
    echo "Examples:"
    echo "  export MLFLOW_TRACKING_URI='http://localhost:5000'"
    echo "  ./build_model_image.sh 5af69ee1f3344c5ea445221ac85199e0 my-model"
    echo "  ./build_model_image.sh 5af69ee1f3344c5ea445221ac85199e0 my-model v1.0.0"
    exit 0
fi

# Check arguments
RUN_ID="$1"
IMAGE_NAME="$2"
TAG="${3:-latest}"

if [ -z "$RUN_ID" ]; then
    echo -e "${RED}❌ Error: run_id is required${NC}"
    exit 1
fi

if [ -z "$IMAGE_NAME" ]; then
    echo -e "${RED}❌ Error: image_name is required${NC}"
    exit 1
fi

# Check MLFLOW_TRACKING_URI
if [ -z "$MLFLOW_TRACKING_URI" ]; then
    echo -e "${RED}❌ Error: MLFLOW_TRACKING_URI is not set${NC}"
    echo "Set it with: export MLFLOW_TRACKING_URI='http://localhost:5000'"
    exit 1
fi

# Sanitize image name (lowercase, alphanumeric and hyphens only)
CLEAN_NAME=$(echo "$IMAGE_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g' | sed 's/-\+/-/g' | sed 's/^-\|-$//g')

echo -e "${GREEN}Building MLflow model image${NC}"
echo "  Run ID: $RUN_ID"
echo "  Image: $CLEAN_NAME:$TAG"
echo "  MLflow URI: $MLFLOW_TRACKING_URI"
echo ""

# Build Docker image
mlflow models build-docker \
    --model-uri "runs:/${RUN_ID}/model" \
    --name "$CLEAN_NAME"

# Tag if not latest
if [ "$TAG" != "latest" ]; then
    docker tag "$CLEAN_NAME:latest" "$CLEAN_NAME:$TAG"
fi

# Verify
if docker image inspect "$CLEAN_NAME:$TAG" > /dev/null 2>&1; then
    echo ""
    echo -e "${GREEN}✅ Image built successfully: $CLEAN_NAME:$TAG${NC}"
    echo "   Run with: docker run -p 8080:8080 $CLEAN_NAME:$TAG"
else
    echo -e "${RED}❌ Image build failed${NC}"
    exit 1
fi
