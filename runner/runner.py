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

"""
Simple runner that loads preprocessing code and calls pre_processing().

The preprocessing code is already included in the Docker image.
Supports input formats: JSONL (stdin) and Avro files.
"""

import importlib.util
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Preprocessing code is located in /app/preprocessing/
PREPROCESSING_PATH = Path("/app/preprocessing/preprocessing.py")


def load_preprocessing():
    """
    Load the pre_processing() function from the preprocessing module.

    Returns:
        callable: The pre_processing function or method

    Raises:
        FileNotFoundError: If preprocessing file doesn't exist
        ValueError: If pre_processing function cannot be found
    """
    if not PREPROCESSING_PATH.exists():
        raise FileNotFoundError(f"Preprocessing not found at {PREPROCESSING_PATH}")

    logger.info(f"Loading preprocessing from {PREPROCESSING_PATH}")

    # Load the module
    spec = importlib.util.spec_from_file_location("preprocessing", PREPROCESSING_PATH)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load module from {PREPROCESSING_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Pattern 1: Direct function
    if hasattr(module, "pre_processing") and callable(module.pre_processing):
        func = getattr(module, "pre_processing")
        if not isinstance(func, type):  # Not a class
            logger.info("Found pre_processing() as function")
            return func

    # Pattern 2: Class with pre_processing() method
    for attr_name in dir(module):
        if attr_name.startswith("_"):
            continue

        attr = getattr(module, attr_name)

        # If it's a class with pre_processing method
        if isinstance(attr, type) and hasattr(attr, "pre_processing"):
            try:
                # Try to instantiate (without arguments if possible)
                instance = attr()
                if callable(getattr(instance, "pre_processing")):
                    logger.info(f"Found pre_processing() in class {attr_name}")
                    return instance.pre_processing
            except Exception as e:
                logger.warning(f"Could not instantiate {attr_name}: {e}")
                # Try as class method
                if callable(getattr(attr, "pre_processing")):
                    logger.info(f"Found pre_processing() as class method in {attr_name}")
                    return getattr(attr, "pre_processing")

        # If it's an instance with pre_processing method
        if hasattr(attr, "pre_processing") and callable(getattr(attr, "pre_processing")):
            logger.info(f"Found pre_processing() in instance {attr_name}")
            return attr.pre_processing

    # Pattern 3: Variable pre_processing that is a function
    if "pre_processing" in dir(module):
        pre_processing_attr = getattr(module, "pre_processing")
        if callable(pre_processing_attr):
            logger.info("Found pre_processing() as variable")
            return pre_processing_attr

    raise ValueError(
        "No pre_processing() function found in preprocessing. "
        "The code must define a function or class method named 'pre_processing'."
    )


def read_avro_file(file_path):
    """
    Read an Avro file and yield records one by one.

    Args:
        file_path: Path to the Avro file

    Yields:
        dict: Each Avro record converted to a Python dictionary

    Raises:
        ImportError: If fastavro is not installed
    """
    try:
        import fastavro
    except ImportError:
        raise ImportError("fastavro not installed. Install with: pip install fastavro")

    with open(file_path, "rb") as f:
        reader = fastavro.reader(f)
        schema = reader.schema
        schema_name = schema.get("name", "unknown")
        logger.info(f"Reading Avro file with schema: {schema_name}")

        for record in reader:
            yield record


def read_avro_from_stdin():
    """
    Read Avro data from stdin (binary mode).

    Yields:
        dict: Each Avro record converted to a Python dictionary

    Raises:
        ImportError: If fastavro is not installed
    """
    try:
        import fastavro
    except ImportError:
        raise ImportError("fastavro not installed. Install with: pip install fastavro")

    # Read from stdin in binary mode
    reader = fastavro.reader(sys.stdin.buffer)
    schema = reader.schema
    schema_name = schema.get("name", "unknown")
    logger.info(f"Reading Avro from stdin with schema: {schema_name}")

    for record in reader:
        yield record


def is_avro_file(file_path_or_stdin):
    """
    Detect if the input is an Avro file.

    Args:
        file_path_or_stdin: File path or None for stdin

    Returns:
        bool: True if it's an Avro file
    """
    if file_path_or_stdin is None:
        # For stdin, try to detect by reading first bytes
        # Avro files start with "Obj" + version
        try:
            pos = sys.stdin.tell() if hasattr(sys.stdin, "tell") else 0
            if pos == 0:  # Can read from the beginning
                # Cannot easily peek stdin, so default to JSONL for stdin
                return False
        except Exception:
            return False
    else:
        # Check file extension
        if str(file_path_or_stdin).endswith(".avro"):
            return True

        # Check magic number (first bytes)
        try:
            with open(file_path_or_stdin, "rb") as f:
                header = f.read(4)
                # Avro files start with "Obj" + version (1 byte)
                if header.startswith(b"Obj"):
                    return True
        except Exception:
            pass

    return False


def _read_json_from_file(file_path):
    """
    Read JSON or JSONL data from a file.

    Args:
        file_path: Path to the JSON/JSONL file

    Yields:
        dict: Each JSON object from the file
    """
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return

        # Try to parse as complete JSON first
        try:
            data = json.loads(content)
            # If it's a list, iterate over items
            if isinstance(data, list):
                for item in data:
                    yield item
            else:
                # Otherwise, it's a single JSON object
                yield data
        except json.JSONDecodeError:
            # Otherwise, treat as JSONL (line by line)
            for line in content.split("\n"):
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON line ignored: {line[:50]}... ({e})")
                        continue


def _read_json_from_stdin():
    """
    Read JSON or JSONL data from stdin.

    Yields:
        dict: Each JSON object from stdin
    """
    # Try to read all content first
    content = sys.stdin.read().strip()
    if not content:
        return

    # Try to parse as complete JSON
    try:
        data = json.loads(content)
        # If it's a list, iterate over items
        if isinstance(data, list):
            for item in data:
                yield item
        else:
            # Otherwise, it's a single JSON object
            yield data
    except json.JSONDecodeError:
        # Otherwise, treat as JSONL (line by line)
        for line in content.split("\n"):
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON line ignored: {line[:50]}... ({e})")
                    continue


def main():
    """
    Run preprocessing on input data.

    Reads input data (JSONL or Avro), applies preprocessing, and outputs results.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Run preprocessing on input data (JSONL or Avro)")
    parser.add_argument(
        "input_file",
        nargs="?",
        default=None,
        help="Input file (Avro or JSONL). If not provided, reads from stdin.",
    )
    parser.add_argument(
        "--format",
        choices=["auto", "jsonl", "avro"],
        default="auto",
        help="Input format (default: auto-detect)",
    )

    args = parser.parse_args()

    try:
        # Load pre_processing() function
        pre_processing_func = load_preprocessing()
        logger.info("Preprocessing loaded successfully")

        # Determine input format
        input_format = args.format
        if input_format == "auto":
            if args.input_file:
                input_format = "avro" if is_avro_file(args.input_file) else "jsonl"
            else:
                # For stdin, default to JSONL
                # User can force with --format avro
                input_format = "jsonl"

        logger.info(f"Input format: {input_format}")

        # Read data according to format
        if input_format == "avro":
            if args.input_file:
                data_generator = read_avro_file(args.input_file)
            else:
                data_generator = read_avro_from_stdin()
        else:
            # JSONL (line by line) or complete JSON format
            if args.input_file:
                data_generator = _read_json_from_file(args.input_file)
            else:
                data_generator = _read_json_from_stdin()

        # Process each record
        for data in data_generator:
            try:
                # Apply preprocessing
                result = pre_processing_func(data)

                # Output (to Kafka or stdout)
                # Format: JSON line by line
                print(json.dumps(result, ensure_ascii=False))
                sys.stdout.flush()  # Important for streaming

            except Exception as e:
                logger.error(f"Preprocessing failed: {e}", exc_info=True)
                continue

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
