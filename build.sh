#!/bin/bash
# Copyright 2025 AstroLab Software
# Author: Farid MAMAN and improved by IA
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Auto-build Docker images from MLflow (preprocessing + model)
# Usage: ./build.sh <run_id> [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
MODEL_SOURCE=""
TAG="latest"
PREPROCESSING_PATH=""
DOCKERFILE="docker/Dockerfile"
PYTHON_VERSION=""
PUSH_TO_REGISTRY=false

# Check for help flag
if [ "$1" = "--help" ] || [ "$1" = "-h" ] || [ "$1" = "-?" ]; then
    echo "Usage: ./build.sh <run_id> [options]"
    echo ""
    echo "Description:"
    echo "  Auto-build Docker images from MLflow: preprocessing and optionally model."
    echo "  Image names are automatically generated from MLflow metadata."
    echo "  In GitHub Actions CI, images are automatically pushed to ghcr.io."
    echo ""
    echo "Arguments:"
    echo "  run_id                MLflow run ID (required)"
    echo ""
    echo "Options:"
    echo "  --model-source        MLflow model URI (e.g., models:/model-name/1)"
    echo "                        If provided, builds model serving image"
    echo "  --tag, -t             Docker image tag (default: latest)"
    echo "  --preprocessing-path  Path to preprocessing in MLflow (default: auto-detect)"
    echo "  --dockerfile          Path to Dockerfile (default: docker/Dockerfile)"
    echo "  --python-version      Python version (e.g., 3.11, 3.12)"
    echo "  --push                Push images to registry (auto-enabled in CI)"
    echo "  --help, -h            Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Build preprocessing only"
    echo "  ./build.sh e6c1131f4673449aa688ed1ffc3abbbe"
    echo ""
    echo "  # Build preprocessing + model"
    echo "  ./build.sh e6c1131f4673449aa688ed1ffc3abbbe --model-source models:/model-name/1"
    echo ""
    echo "  # Build with specific tag"
    echo "  ./build.sh e6c1131f4673449aa688ed1ffc3abbbe --model-source models:/model-name/1 --tag v1.0.0"
    echo ""
    exit 0
fi

# Check if run_id is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: MLflow run_id is required${NC}"
    echo ""
    echo "Usage: ./build.sh <run_id> [options]"
    echo "Run './build.sh --help' for more information."
    echo ""
    exit 1
fi

RUN_ID=$1
shift  # Remove run_id from arguments

# Parse options
while [[ $# -gt 0 ]]; do
    case $1 in
        --model-source)
            MODEL_SOURCE="$2"
            shift 2
            ;;
        --tag|-t)
            TAG="$2"
            shift 2
            ;;
        --preprocessing-path)
            PREPROCESSING_PATH="$2"
            shift 2
            ;;
        --dockerfile)
            DOCKERFILE="$2"
            shift 2
            ;;
        --python-version)
            PYTHON_VERSION="$2"
            shift 2
            ;;
        --push)
            PUSH_TO_REGISTRY=true
            shift
            ;;
        --*)
            # Unknown option starting with --, pass to build_image.py
            break
            ;;
        *)
            # If MODEL_SOURCE is not set and this doesn't look like an option, treat as model-source
            if [ -z "$MODEL_SOURCE" ] && [[ ! "$1" =~ ^-- ]]; then
                MODEL_SOURCE="$1"
                shift
            else
                # Pass unknown arguments to build_image.py
                break
            fi
            ;;
    esac
done

# Detect if running in GitHub Actions CI
if [ -n "$GITHUB_ACTIONS" ]; then
    PUSH_TO_REGISTRY=true
    GITHUB_REGISTRY="ghcr.io"
    GITHUB_REPOSITORY_LOWER=$(echo "$GITHUB_REPOSITORY" | tr '[:upper:]' '[:lower:]')
    echo -e "${BLUE}🔵 Running in GitHub Actions CI${NC}"
    echo -e "${BLUE}   Registry: ${GITHUB_REGISTRY}/${GITHUB_REPOSITORY_LOWER}${NC}"
fi

# Check if MLFLOW_TRACKING_URI is set
if [ -z "$MLFLOW_TRACKING_URI" ]; then
    echo -e "${YELLOW}Warning: MLFLOW_TRACKING_URI is not set${NC}"
    echo "Set it with: export MLFLOW_TRACKING_URI='http://127.0.0.1:5000'"
    echo ""
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: python3 is not installed${NC}"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Function to get model info from MLflow
get_model_info() {
    python3 -c "
import mlflow
import os
import sys
mlflow.set_tracking_uri(os.environ.get('MLFLOW_TRACKING_URI', 'http://127.0.0.1:5000'))
try:
    run = mlflow.get_run('$RUN_ID')
    model_name = run.data.tags.get('model_name') or run.data.tags.get('mlflow.runName') or 'unknown-model'
    version = run.data.tags.get('version') or run.data.tags.get('model_version') or '${RUN_ID:0:8}'
    print(f'{model_name}|{version}')
except Exception as e:
    print(f'unknown-model|${RUN_ID:0:8}', file=sys.stderr)
"
}

# Function to get model URI from run (auto-detect)
get_model_uri_from_run() {
    python3 -c "
import mlflow
import os
import sys
mlflow.set_tracking_uri(os.environ.get('MLFLOW_TRACKING_URI', 'http://127.0.0.1:5000'))
try:
    # Try to find registered model from run
    client = mlflow.tracking.MlflowClient()
    run = mlflow.get_run('$RUN_ID')

    # Check if there's a registered model linked to this run
    # Look for model artifacts
    artifacts = client.list_artifacts('$RUN_ID')
    has_model = any('model' in a.path.lower() for a in artifacts)

    if has_model:
        # Use runs:/ format
        print(f'runs:/$RUN_ID/model')
    else:
        # Try to find registered model
        try:
            logged_models = client.search_model_versions(f\"run_id='$RUN_ID'\")
            if logged_models:
                model = logged_models[0]
                print(f\"models:/{model.name}/{model.version}\")
            else:
                # Fallback to runs:/ format
                print(f'runs:/$RUN_ID/model')
        except:
            print(f'runs:/$RUN_ID/model')
except Exception as e:
    # Fallback to runs:/ format
    print(f'runs:/$RUN_ID/model', file=sys.stderr)
"
}

# Function to sanitize image name
sanitize_image_name() {
    echo "$1" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]/-/g' | sed 's/-\+/-/g' | sed 's/^-\|-$//g'
}

# Function to push image to registry
push_image() {
    local image_name=$1
    local tag=$2

    if [ "$PUSH_TO_REGISTRY" = true ]; then
        if [ -n "$GITHUB_ACTIONS" ]; then
            # GitHub Container Registry
            local registry_image="${GITHUB_REGISTRY}/${GITHUB_REPOSITORY_LOWER}/${image_name}"
            echo -e "${BLUE}📤 Pushing ${image_name}:${tag} to ${registry_image}:${tag}${NC}"
            docker tag "${image_name}:${tag}" "${registry_image}:${tag}"
            docker push "${registry_image}:${tag}"
            echo -e "${GREEN}✅ Pushed ${registry_image}:${tag}${NC}"
        else
            # Generic registry (if DOCKER_REGISTRY is set)
            if [ -n "$DOCKER_REGISTRY" ]; then
                local registry_image="${DOCKER_REGISTRY}/${image_name}"
                echo -e "${BLUE}📤 Pushing ${image_name}:${tag} to ${registry_image}:${tag}${NC}"
                docker tag "${image_name}:${tag}" "${registry_image}:${tag}"
                docker push "${registry_image}:${tag}"
                echo -e "${GREEN}✅ Pushed ${registry_image}:${tag}${NC}"
            fi
        fi
    fi
}

echo -e "${GREEN}🚀 Starting auto-build from MLflow...${NC}"
echo "Run ID: $RUN_ID"
if [ -n "$MODEL_SOURCE" ]; then
    echo "Model Source: $MODEL_SOURCE"
fi
echo ""

# Get model info from MLflow
MODEL_INFO=$(get_model_info)
MODEL_NAME=$(echo "$MODEL_INFO" | cut -d'|' -f1)
MODEL_VERSION=$(echo "$MODEL_INFO" | cut -d'|' -f2)

# Build preprocessing image
if [ -n "$MODEL_SOURCE" ]; then
    echo -e "${GREEN}📦 Step 1/2: Building preprocessing image...${NC}"
else
    echo -e "${GREEN}📦 Building preprocessing image...${NC}"
fi
PREPROCESSING_ARGS=()
if [ -n "$PREPROCESSING_PATH" ]; then
    PREPROCESSING_ARGS+=(--preprocessing-path "$PREPROCESSING_PATH")
fi
if [ -n "$DOCKERFILE" ]; then
    PREPROCESSING_ARGS+=(--dockerfile "$DOCKERFILE")
fi
if [ -n "$PYTHON_VERSION" ]; then
    PREPROCESSING_ARGS+=(--python-version "$PYTHON_VERSION")
fi
if [ -n "$TAG" ] && [ "$TAG" != "latest" ]; then
    PREPROCESSING_ARGS+=(--tag "$TAG")
fi

python3 build_scripts/build_image.py "$RUN_ID" auto "${PREPROCESSING_ARGS[@]}"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Preprocessing build failed${NC}"
    exit 1
fi

# Get preprocessing image name
PREPROCESSING_IMAGE_NAME=$(sanitize_image_name "preprocessing-${MODEL_NAME}-${MODEL_VERSION}")
echo -e "${GREEN}✅ Preprocessing image built: ${PREPROCESSING_IMAGE_NAME}:${TAG}${NC}"

# Push preprocessing image if needed
push_image "$PREPROCESSING_IMAGE_NAME" "$TAG"

# Build model image if model_source is provided
if [ -n "$MODEL_SOURCE" ]; then
    echo ""
    echo -e "${GREEN}📦 Step 2/2: Building model serving image...${NC}"

    MODEL_IMAGE_NAME=$(sanitize_image_name "model-${MODEL_NAME}-${MODEL_VERSION}")

    # Determine model URI
    MODEL_URI="$MODEL_SOURCE"

    # If MODEL_SOURCE doesn't look like a valid MLflow URI, try to construct it
    if [[ ! "$MODEL_SOURCE" =~ ^(runs:|models:) ]]; then
        # If it's just an ID or path, try to auto-detect from run
        if [[ "$MODEL_SOURCE" =~ ^models/ ]] || [[ "$MODEL_SOURCE" =~ ^m- ]]; then
            # User provided something like "models/m-xxx" or "m-xxx"
            echo -e "${YELLOW}⚠️  Auto-detecting model URI from run...${NC}"
            MODEL_URI=$(get_model_uri_from_run)
            echo -e "${BLUE}   Using: ${MODEL_URI}${NC}"
        else
            # Assume it's a path in the run artifacts
            MODEL_URI="runs:/${RUN_ID}/${MODEL_SOURCE}"
            echo -e "${BLUE}   Constructed model URI: ${MODEL_URI}${NC}"
        fi
    fi

    # Build model image with MLflow
    echo -e "${BLUE}   Building with: mlflow models build-docker -m ${MODEL_URI}${NC}"
    mlflow models build-docker \
        -m "$MODEL_URI" \
        -n "$MODEL_IMAGE_NAME" \
        --install-mlflow

    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Model build failed${NC}"
        exit 1
    fi

    # Tag if not latest
    if [ "$TAG" != "latest" ]; then
        docker tag "${MODEL_IMAGE_NAME}:latest" "${MODEL_IMAGE_NAME}:${TAG}"
    fi

    echo -e "${GREEN}✅ Model image built: ${MODEL_IMAGE_NAME}:${TAG}${NC}"

    # Push model image if needed
    push_image "$MODEL_IMAGE_NAME" "$TAG"
fi

echo ""
echo -e "${GREEN}✅ All builds completed successfully!${NC}"
echo ""
echo "Built images:"
echo "  - ${PREPROCESSING_IMAGE_NAME}:${TAG}"
if [ -n "$MODEL_SOURCE" ]; then
    echo "  - ${MODEL_IMAGE_NAME}:${TAG}"
fi
