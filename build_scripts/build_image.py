#!/usr/bin/env python3
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

r"""
Script to build Docker image by downloading preprocessing from MLflow.

Usage examples:

1. Basic build:
   export MLFLOW_TRACKING_URI="http://127.0.0.1:5000"
   python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1

2. Build with specific tag:
   python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
       preprocessing-model-v1 --tag v1.0.0

3. Build with preprocessing in a subdirectory MLflow (e.g., code/preprocessing.py):
   python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
       preprocessing-model-v1 --preprocessing-path code/preprocessing.py
   # Note: All Python files in the directory (processor.py, __init__.py, etc.)
   # will be copied automatically

4. Build with custom Dockerfile:
   python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1 \\
       --dockerfile docker/custom.Dockerfile

5. Build with specific Python version (if requirements need Python >=3.11):
   python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1 \\
       --python-version 3.11

Prerequisites:
- MLflow installed: pip install mlflow
- MLFLOW_TRACKING_URI configured
- Docker installed and running
- Preprocessing must be uploaded to MLflow with preprocessing.py (and optionally requirements.txt)
"""

import argparse
import logging
import shutil
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def download_preprocessing_from_mlflow(run_id: str, artifact_path: str = "preprocessing.py"):
    """
    Download preprocessing from MLflow.

    If preprocessing is in a directory (e.g., code/preprocessing.py), download the entire directory.

    Args:
        run_id: MLflow run ID
        artifact_path: Artifact path (e.g., "preprocessing.py" or "code/preprocessing.py")

    Returns:
        tuple: (path to preprocessing.py file, path to parent directory or None)
    """
    try:
        import mlflow
    except ImportError:
        raise ImportError(
            "MLflow not installed. Install with: pip install mlflow\n"
            "Also set MLFLOW_TRACKING_URI environment variable."
        )

    logger.info(f"Downloading preprocessing from MLflow run: {run_id}, artifact: {artifact_path}")

    # Determine if it's a file or directory
    artifact_dir = None
    if "/" in artifact_path:
        # It's a path with directory (e.g., code/preprocessing.py)
        parts = artifact_path.split("/")
        artifact_dir_path = "/".join(parts[:-1])  # E.g., "code"
        filename = parts[-1]  # E.g., "preprocessing.py"

        # Download the entire directory
        try:
            artifact_dir_uri = mlflow.artifacts.download_artifacts(
                run_id=run_id, artifact_path=artifact_dir_path
            )
            artifact_dir = Path(artifact_dir_uri)
            preprocessing_file = artifact_dir / filename

            if not preprocessing_file.exists():
                raise FileNotFoundError(
                    f"{filename} not found in downloaded directory: {artifact_dir_uri}"
                )

            logger.info(f"Downloaded directory '{artifact_dir_path}' containing {filename}")
            logger.info(f"Found files in directory: {list(artifact_dir.glob('*.py'))}")
            return preprocessing_file, artifact_dir

        except Exception as e:
            # If directory download fails, try downloading the file directly
            logger.warning(f"Could not download directory '{artifact_dir_path}': {e}")
            logger.info(f"Trying to download file directly: {artifact_path}")

    # Download the artifact (file or directory)
    try:
        artifact_uri = mlflow.artifacts.download_artifacts(
            run_id=run_id, artifact_path=artifact_path
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to download artifact '{artifact_path}' from run '{run_id}': {e}\n"
            f"Make sure:\n"
            f"  1. MLFLOW_TRACKING_URI is set correctly\n"
            f"  2. The run_id exists\n"
            f"  3. The artifact path is correct"
        )

    artifact_path_obj = Path(artifact_uri)

    # If it's a directory, look for preprocessing.py inside
    if artifact_path_obj.is_dir():
        preprocessing_file = artifact_path_obj / "preprocessing.py"
        if not preprocessing_file.exists():
            raise FileNotFoundError(
                f"preprocessing.py not found in downloaded artifact directory: {artifact_uri}"
            )
        logger.info(f"Found preprocessing.py in directory: {preprocessing_file}")
        logger.info(f"Directory contains: {list(artifact_path_obj.glob('*.py'))}")
        return preprocessing_file, artifact_path_obj

    logger.info(f"Downloaded to: {artifact_uri}")
    return artifact_path_obj, None


def build_docker_image(
    run_id: str,
    image_name: str,
    image_tag: str = "latest",
    preprocessing_path: str = "preprocessing.py",
    dockerfile_path: str = "docker/Dockerfile",
    build_context: str = ".",
    python_version: str = None,
):
    """
    Build Docker image with preprocessing from MLflow.

    Args:
        run_id: MLflow run ID
        image_name: Image name (e.g., "preprocessing-model-v1")
        image_tag: Image tag
        preprocessing_path: Artifact path in MLflow
        dockerfile_path: Path to base Dockerfile
        build_context: Docker build context
        python_version: Python version (e.g., "3.11", "3.12"). If None, uses version from Dockerfile

    Returns:
        str: Full Docker image name (e.g., "preprocessing-model-v1:latest")

    Examples:
        >>> # Basic build
        >>> build_docker_image(
        ...     run_id="e6c1131f4673449aa688ed1ffc3abbbe",
        ...     image_name="preprocessing-model-v1"
        ... )
        'preprocessing-model-v1:latest'

        >>> # Build with specific tag
        >>> build_docker_image(
        ...     run_id="e6c1131f4673449aa688ed1ffc3abbbe",
        ...     image_name="preprocessing-model-v1",
        ...     image_tag="v1.0.0"
        ... )
        'preprocessing-model-v1:v1.0.0'

        >>> # Build with preprocessing in a subdirectory
        >>> build_docker_image(
        ...     run_id="e6c1131f4673449aa688ed1ffc3abbbe",
        ...     image_name="preprocessing-model-v1",
        ...     preprocessing_path="models/preprocessing.py"
        ... )
        'preprocessing-model-v1:latest'
    """
    # 1. Download preprocessing (and its parent directory if in a directory)
    preprocessing_file, preprocessing_dir = download_preprocessing_from_mlflow(
        run_id, preprocessing_path
    )

    # 2. Use source directory as build context
    # This allows Docker to use cache for the runner which doesn't change
    base_path = Path(build_context).resolve()

    # Create a temporary directory ONLY for preprocessing
    # The runner stays in the source directory (doesn't change between builds)
    preprocessing_temp_dir = Path("/tmp/preprocessing-build")
    preprocessing_temp_dir.mkdir(parents=True, exist_ok=True)

    # If preprocessing is in a directory (e.g., code/preprocessing.py), copy the entire directory
    if preprocessing_dir:
        # Copy all Python files from the directory (for imports like processor.py)
        py_files_copied = []
        for py_file in preprocessing_dir.glob("*.py"):
            # Ignore __pycache__ and hidden files
            if "__pycache__" not in str(py_file) and not py_file.name.startswith("."):
                shutil.copy(py_file, preprocessing_temp_dir / py_file.name)
                py_files_copied.append(py_file.name)
                logger.info(f"Copied {py_file.name} from directory")

        # Also copy __init__.py if it exists (for imports)
        init_file = preprocessing_dir / "__init__.py"
        if init_file.exists():
            shutil.copy(init_file, preprocessing_temp_dir / "__init__.py")
            py_files_copied.append("__init__.py")
            logger.info("Copied __init__.py")

        logger.info(f"Copied {len(py_files_copied)} Python files from directory: {py_files_copied}")
    else:
        # Copy ONLY the downloaded preprocessing
        shutil.copy(preprocessing_file, preprocessing_temp_dir / "preprocessing.py")
        logger.info("Copied preprocessing.py")

    logger.info(f"Preprocessing ready in: {preprocessing_temp_dir}")

    # 3. Download requirements.txt if it exists
    requirements_installed = False
    try:
        import mlflow

        requirements_file = mlflow.artifacts.download_artifacts(
            run_id=run_id, artifact_path="requirements.txt"
        )
        requirements_path = Path(requirements_file)

        # If it's a directory, look for requirements.txt inside
        if requirements_path.is_dir():
            requirements_path = requirements_path / "requirements.txt"

        if requirements_path.exists():
            shutil.copy(requirements_path, preprocessing_temp_dir / "requirements.txt")
            logger.info("Found and copied requirements.txt")
            requirements_installed = True
    except Exception as e:
        logger.warning(f"No requirements.txt found: {e}")

    # 4. Create a temporary Dockerfile
    # The base Dockerfile stays in docker/ and uses the source directory as context
    dockerfile_base = Path(dockerfile_path).read_text()

    # Modify Python version if specified
    if python_version:
        # Replace the FROM python:X.Y-slim line with the specified version
        import re

        dockerfile_content = re.sub(
            r"FROM python:\d+\.\d+-slim", f"FROM python:{python_version}-slim", dockerfile_base
        )
        logger.info(f"Using Python {python_version} (specified via --python-version)")
    else:
        dockerfile_content = dockerfile_base
        logger.info("Using Python version from Dockerfile (default: 3.10)")

    # Add instructions to copy preprocessing
    # Preprocessing will be in preprocessing/ of the source directory (build context)
    dockerfile_content += (
        "\n# Copy preprocessing (added by build script - only element that changes)\n"
    )
    dockerfile_content += "COPY preprocessing/ /app/preprocessing/\n"

    # Install requirements if they exist
    # IMPORTANT: Requirements are installed AFTER copying preprocessing
    # If requirements change, this layer will be rebuilt (normal)
    # If only preprocessing.py changes (not requirements), this layer will be cached
    if requirements_installed:
        dockerfile_content += "\n# Install preprocessing requirements\n"
        dockerfile_content += "# This layer will be rebuilt only if requirements.txt changes\n"
        dockerfile_content += (
            "RUN pip install --no-cache-dir -r /app/preprocessing/requirements.txt\n"
        )

    # Create a temporary Dockerfile in the source directory
    temp_dockerfile = base_path / ".Dockerfile.tmp"
    temp_dockerfile.write_text(dockerfile_content)
    logger.info("Created temporary Dockerfile")

    # 5. Copy preprocessing to source directory temporarily for the build
    # (Docker needs files to be in the build context)
    preprocessing_source_dir = base_path / "preprocessing"
    preprocessing_source_dir.mkdir(exist_ok=True)

    # Copy all Python files from the temporary directory
    for py_file in preprocessing_temp_dir.glob("*.py"):
        if not py_file.name.startswith("."):
            shutil.copy(py_file, preprocessing_source_dir / py_file.name)
            logger.info(f"Copied {py_file.name} to build context")

    # Copy requirements.txt if it exists
    if requirements_installed:
        shutil.copy(
            preprocessing_temp_dir / "requirements.txt",
            preprocessing_source_dir / "requirements.txt",
        )

    # 6. Build Docker image with source directory as context
    # Docker will use cache for runner layers that don't change
    image_full_name = f"{image_name}:{image_tag}"
    logger.info(f"Building Docker image: {image_full_name}")
    logger.info("Note: Docker cache will be used for runner layers (they don't change)")

    try:
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                image_full_name,
                "-f",
                str(temp_dockerfile),
                str(base_path),  # Build context = source directory (runner doesn't change)
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        logger.info(f"✅ Image built successfully: {image_full_name}")

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Docker build failed: {e}")

        # Analyze error to detect Python compatibility issues
        error_output = e.stdout if e.stdout else ""
        python_version_hint = None
        current_python_version = None

        # Detect current Python version from Dockerfile
        import re

        current_python_match = re.search(r"FROM python:(\d+\.\d+)-slim", dockerfile_content)
        if current_python_match:
            current_python_version = current_python_match.group(1)

        # Detect required Python version errors
        python_version_pattern = r"Requires-Python >=(\d+\.\d+)"
        matches = re.findall(python_version_pattern, error_output)
        if matches:
            # Take the highest required Python version
            required_versions = [float(v) for v in matches]
            max_version = max(required_versions)
            python_version_hint = f"{int(max_version)}.{int((max_version - int(max_version)) * 10)}"

        # Also detect "No matching distribution found" errors
        if "No matching distribution found" in error_output and not python_version_hint:
            # Suggest Python 3.11 or 3.12 for recent dependencies
            if current_python_version and float(current_python_version) < 3.11:
                python_version_hint = "3.11"

        if python_version_hint:
            logger.error("")
            logger.error("=" * 70)
            logger.error("⚠️  PYTHON VERSION COMPATIBILITY ISSUE DETECTED")
            logger.error("=" * 70)
            logger.error(f"The requirements.txt requires Python >= {python_version_hint}")
            logger.error(f"Current Docker image uses Python {current_python_version or '3.10'}")
            logger.error("")
            logger.error("💡 SOLUTION: Use --python-version flag:")
            logger.error(f"   python build_scripts/build_image.py {run_id} {image_name} \\")
            logger.error(f"       --python-version {python_version_hint}")
            logger.error("=" * 70)
            logger.error("")

        if e.stdout:
            logger.error("Build output (last 50 lines):")
            output_lines = e.stdout.split("\n")
            # Show the most relevant lines
            relevant_lines = [
                line
                for line in output_lines
                if "ERROR" in line
                or "Requires-Python" in line
                or "No matching distribution" in line
            ]
            if relevant_lines:
                for line in relevant_lines[-10:]:  # Last 10 error lines
                    logger.error(f"  {line}")
            else:
                # If no specific lines, show the last lines
                for line in output_lines[-20:]:
                    logger.error(f"  {line}")

        raise

    # Clean up temporary files
    try:
        temp_dockerfile.unlink()
        # Clean up preprocessing from source directory (it's now in the image)
        for py_file in preprocessing_source_dir.glob("*.py"):
            py_file.unlink()
        if (preprocessing_source_dir / "requirements.txt").exists():
            (preprocessing_source_dir / "requirements.txt").unlink()
        logger.info("Cleaned temporary files")
    except Exception as e:
        logger.warning(f"Could not clean temp files: {e}")

    if requirements_installed:
        logger.info("✅ Build complete! Preprocessing + requirements installed.")
        logger.info("   - Runner layers: cached (don't change)")
        logger.info("   - Preprocessing: new")
        logger.info("   - Requirements: installed from MLflow")
    else:
        logger.info("✅ Build complete! Only preprocessing changed, runner layers were cached.")

    return image_full_name


def main():
    r"""
    CLI entry point.

    Usage examples:

    1. Basic build with run_id and image name:
       python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
           preprocessing-model-v1

    2. Build with specific tag:
       python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
           preprocessing-model-v1 --tag v1.0.0

    3. Build with preprocessing in a subdirectory MLflow:
       python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
           preprocessing-model-v1 --preprocessing-path models/preprocessing.py

    4. Build with custom Dockerfile:
       python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
           preprocessing-model-v1 --dockerfile docker/custom.Dockerfile

    5. Complete example with all options:
       export MLFLOW_TRACKING_URI="http://127.0.0.1:5000"
       python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
           preprocessing-model-v1 --tag latest \\
           --preprocessing-path preprocessing.py \\
           --dockerfile docker/Dockerfile
    """
    parser = argparse.ArgumentParser(
        description="Build Docker image with preprocessing from MLflow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic build
  python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe preprocessing-model-v1

  # Build with tag
  python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
      preprocessing-model-v1 --tag v1.0.0

  # Build with preprocessing in a subdirectory (e.g., code/preprocessing.py)
  # All Python files in the directory will be copied automatically
  python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
      preprocessing-model-v1 --preprocessing-path code/preprocessing.py

  # Build with specific Python version (if requirements need Python >=3.11)
  python build_scripts/build_image.py e6c1131f4673449aa688ed1ffc3abbbe \\
      preprocessing-model-v1 --python-version 3.11

Prerequisites:
  - MLflow installed: pip install mlflow
  - MLFLOW_TRACKING_URI configured:
    export MLFLOW_TRACKING_URI="http://127.0.0.1:5000"
  - Docker installed and running
        """,
    )
    parser.add_argument("run_id", help="MLflow run ID")
    parser.add_argument("image_name", help="Docker image name (e.g., preprocessing-model-v1)")
    parser.add_argument("--tag", "-t", default="latest", help="Docker image tag (default: latest)")
    parser.add_argument(
        "--preprocessing-path",
        default="preprocessing.py",
        help="Path to preprocessing artifact in MLflow (default: preprocessing.py)",
    )
    parser.add_argument(
        "--dockerfile",
        default="docker/Dockerfile",
        help="Path to base Dockerfile (default: docker/Dockerfile)",
    )
    parser.add_argument(
        "--python-version",
        default=None,
        help=(
            "Python version to use (e.g., 3.11, 3.12). "
            "Default: use version from Dockerfile (3.10)"
        ),
    )

    args = parser.parse_args()

    try:
        build_docker_image(
            run_id=args.run_id,
            image_name=args.image_name,
            image_tag=args.tag,
            preprocessing_path=args.preprocessing_path,
            dockerfile_path=args.dockerfile,
            python_version=args.python_version,
        )
    except Exception as e:
        logger.error(f"Build failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
