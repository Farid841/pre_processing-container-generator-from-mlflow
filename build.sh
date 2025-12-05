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

# Auto-build Docker image from MLflow run
# Usage: ./build.sh <run_id> [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check for help flag
if [ "$1" = "--help" ] || [ "$1" = "-h" ] || [ "$1" = "-?" ]; then
    echo "Usage: ./build.sh <run_id> [options]"
    echo ""
    echo "Description:"
    echo "  Auto-build Docker image from MLflow run with preprocessing."
    echo "  Image name is automatically generated from MLflow metadata."
    echo ""
    echo "Arguments:"
    echo "  run_id                MLflow run ID (required)"
    echo ""
    echo "Options:"
    echo "  --tag, -t              Docker image tag (default: latest)"
    echo "  --preprocessing-path   Path to preprocessing in MLflow (default: auto-detect)"
    echo "                         Auto-detection searches in: preprocessing/preprocessing.py,"
    echo "                         preprocessing.py, pre_processing/pre_processing.py"
    echo "  --dockerfile           Path to Dockerfile (default: docker/Dockerfile)"
    echo "  --python-version       Python version (e.g., 3.11, 3.12)"
    echo "  --help, -h             Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Auto-build with default settings"
    echo "  ./build.sh e6c1131f4673449aa688ed1ffc3abbbe"
    echo ""
    echo "  # Auto-build with specific tag"
    echo "  ./build.sh e6c1131f4673449aa688ed1ffc3abbbe --tag v1.0.0"
    echo ""
    echo "  # Auto-build with preprocessing in subdirectory"
    echo "  ./build.sh e6c1131f4673449aa688ed1ffc3abbbe --preprocessing-path code/preprocessing.py"
    echo ""
    echo "  # Auto-build with Python 3.11"
    echo "  ./build.sh e6c1131f4673449aa688ed1ffc3abbbe --python-version 3.11"
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

echo -e "${GREEN}🚀 Starting auto-build from MLflow...${NC}"
echo "Run ID: $RUN_ID"
echo ""

# Run the build script with auto image name
python3 build_scripts/build_image.py "$RUN_ID" auto "$@"

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Build completed successfully!${NC}"
else
    echo ""
    echo -e "${RED}❌ Build failed${NC}"
    exit 1
fi
