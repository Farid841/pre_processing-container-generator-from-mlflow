#!/usr/bin/env python3
# Copyright 2025 AstroLab Software
# Author: Farid MAMAN
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

r"""
Build a MLflow model image with an integrated Kafka bridge.

Two-step process
----------------
1. ``mlflow models build-docker`` builds the base image that contains
   the model artefacts and the MLflow serving stack.

2. ``docker build -f docker/Dockerfile.model`` wraps that base image by
   adding the kafka_bridge module and a new entrypoint.  When the
   container starts with ``KAFKA_ENABLED=true`` it runs both the MLflow
   server and the Kafka bridge in the same process-group, with the bridge
   calling ``localhost:<MODEL_PORT>/invocations`` — no sidecar needed.

Usage examples
--------------
  # From a registered model version
  python build_scripts/build_model_image.py models:/my-model/3 my-model 3

  # From a run artefact
  python build_scripts/build_model_image.py runs:/abc123def456/model my-model 1.0

  # With a specific tag
  python build_scripts/build_model_image.py models:/my-model/3 my-model 3 --tag v3.0

  # Skip step 1 if the base image is already built
  python build_scripts/build_model_image.py models:/my-model/3 my-model 3 --skip-base-build
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from build_scripts.utils import sanitize_docker_name
from build_scripts.utils import stream_run as _stream_run

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def build_mlflow_base_image(model_uri: str, base_image_name: str) -> str:
    """
    Step 1 — run ``mlflow models build-docker`` to produce the base image.

    Args:
        model_uri:       MLflow model URI (``models:/name/version`` or ``runs:/id/path``)
        base_image_name: Docker image name for the base (no tag — always ``latest``)

    Returns:
        Full image reference, e.g. ``model-foo-3-base:latest``
    """
    _stream_run(
        [
            "mlflow",
            "models",
            "build-docker",
            "-m",
            model_uri,
            "-n",
            base_image_name,
            "--install-mlflow",
        ]
    )
    full = f"{base_image_name}:latest"
    logger.info("Base image ready: %s", full)
    return full


def build_wrapper_image(
    base_image: str,
    final_image_name: str,
    image_tag: str = "latest",
    dockerfile_path: str = "docker/Dockerfile.model",
    build_context: str = ".",
) -> str:
    """
    Step 2 — wrap the base image to add the Kafka bridge.

    Args:
        base_image:       Full reference to the base image produced in step 1
        final_image_name: Name for the final image (without tag)
        image_tag:        Tag for the final image
        dockerfile_path:  Path to ``docker/Dockerfile.model``
        build_context:    Docker build context (repo root)

    Returns:
        Full image reference, e.g. ``model-foo-3:latest``
    """
    # Dockerfile.model must exist
    if not Path(dockerfile_path).exists():
        raise FileNotFoundError(f"Dockerfile not found: {dockerfile_path}")

    full = f"{final_image_name}:{image_tag}"
    _stream_run(
        [
            "docker",
            "build",
            "-t",
            full,
            "-f",
            dockerfile_path,
            "--build-arg",
            f"BASE_IMAGE={base_image}",
            build_context,
        ]
    )
    logger.info("Wrapper image ready: %s", full)
    return full


def main() -> None:
    """Build a MLflow model Docker image with an integrated Kafka bridge."""
    parser = argparse.ArgumentParser(
        description="Build MLflow model image with integrated Kafka bridge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "model_source",
        help="MLflow model URI, e.g. 'models:/name/3' or 'runs:/abc123/model'",
    )
    parser.add_argument("model_name", help="Model name used to name the Docker image")
    parser.add_argument("model_version", help="Model version used to name the Docker image")
    parser.add_argument(
        "--tag",
        "-t",
        default="latest",
        help="Tag for the final image (default: latest)",
    )
    parser.add_argument(
        "--dockerfile",
        default="docker/Dockerfile.model",
        help="Path to Dockerfile.model (default: docker/Dockerfile.model)",
    )
    parser.add_argument(
        "--skip-base-build",
        action="store_true",
        help=(
            "Skip step 1 (mlflow models build-docker). "
            "Use this when the base image is already built locally."
        ),
    )

    args = parser.parse_args()

    name_clean = sanitize_docker_name(args.model_name)
    ver_clean = sanitize_docker_name(str(args.model_version))

    base_image_name = f"model-{name_clean}-{ver_clean}-base"
    final_image_name = f"model-{name_clean}-{ver_clean}"

    try:
        if args.skip_base_build:
            base_image = f"{base_image_name}:latest"
            logger.info("Skipping base build — using existing image: %s", base_image)
        else:
            base_image = build_mlflow_base_image(args.model_source, base_image_name)

        final_image = build_wrapper_image(
            base_image=base_image,
            final_image_name=final_image_name,
            image_tag=args.tag,
            dockerfile_path=args.dockerfile,
        )

        logger.info("Build complete: %s", final_image)
        logger.info("")
        logger.info("Run (API only):       docker run -p 8080:8080 %s", final_image)
        logger.info("Run (with Kafka):     docker run \\")
        logger.info("                        -e KAFKA_ENABLED=true \\")
        logger.info("                        -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \\")
        logger.info("                        -e INPUT_TOPIC=preprocessed \\")
        logger.info("                        -e OUTPUT_TOPIC=predictions \\")
        logger.info("                        -p 8080:8080 %s", final_image)

    except subprocess.CalledProcessError as e:
        logger.error("Build step failed (exit code %d)", e.returncode)
        sys.exit(1)
    except Exception as e:
        logger.error("Build failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
