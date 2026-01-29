"""
Serialization/deserialization module for Kafka messages.

Supports both Avro (schemaless) and JSON formats.
"""

import base64
import json
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import fastavro

from kafka_bridge.logger import BridgeLogger


class AvroSerializer:
    """Avro serializer/deserializer using fastavro schemaless format.

    This matches the format used by fink-alert-simulator which uses
    fastavro.schemaless_writer/schemaless_reader without schema registry.
    """

    def __init__(self, schema_path: str, logger: Optional[BridgeLogger] = None):
        """Initialize with schema file path.

        Args:
            schema_path: Path to Avro schema file (.avsc JSON or .avro container)
            logger: Optional logger for debugging
        """
        self.logger = logger
        self.schema = self._load_schema(schema_path)
        self.parsed_schema = fastavro.parse_schema(self.schema)

    def _load_schema(self, schema_path: str) -> dict:
        """Load Avro schema from file.

        Supports both .avsc (JSON schema) and .avro (container with embedded schema) files.
        """
        path = Path(schema_path)

        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        if path.suffix == ".avsc":
            # JSON schema file
            with open(path, "r") as f:
                return json.load(f)
        elif path.suffix == ".avro":
            # Avro container file - extract schema
            with open(path, "rb") as f:
                reader = fastavro.reader(f)
                return reader.writer_schema
        else:
            # Try JSON first, then Avro container
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError):
                with open(path, "rb") as f:
                    reader = fastavro.reader(f)
                    return reader.writer_schema

    def deserialize(self, data: bytes) -> dict:
        """Deserialize Avro schemaless bytes to Python dict.

        Args:
            data: Raw Avro schemaless bytes

        Returns:
            Deserialized Python dict
        """
        buffer = BytesIO(data)
        return fastavro.schemaless_reader(buffer, self.parsed_schema)

    def serialize(self, data: dict) -> bytes:
        """Serialize Python dict to Avro schemaless bytes.

        Args:
            data: Python dict matching schema

        Returns:
            Avro schemaless bytes
        """
        buffer = BytesIO()
        fastavro.schemaless_writer(buffer, self.parsed_schema, data)
        return buffer.getvalue()


class JsonSerializer:
    """JSON serializer/deserializer with bytes handling.

    Handles bytes fields (like cutout stampData) by encoding to base64.
    """

    def __init__(self, logger: Optional[BridgeLogger] = None):
        """Initialize JSON serializer.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger

    def _handle_bytes_for_json(self, obj: Any) -> Any:
        """Recursively convert bytes to base64 strings for JSON serialization."""
        if isinstance(obj, bytes):
            return {
                "_type": "bytes",
                "_encoding": "base64",
                "_value": base64.b64encode(obj).decode("ascii"),
            }
        elif isinstance(obj, dict):
            return {k: self._handle_bytes_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._handle_bytes_for_json(item) for item in obj]
        return obj

    def _handle_bytes_from_json(self, obj: Any) -> Any:
        """Recursively convert base64 strings back to bytes."""
        if isinstance(obj, dict):
            if obj.get("_type") == "bytes" and obj.get("_encoding") == "base64":
                return base64.b64decode(obj["_value"])
            return {k: self._handle_bytes_from_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._handle_bytes_from_json(item) for item in obj]
        return obj

    def deserialize(self, data: bytes) -> dict:
        """Deserialize JSON bytes to Python dict.

        Args:
            data: UTF-8 encoded JSON bytes

        Returns:
            Deserialized Python dict with bytes restored
        """
        parsed = json.loads(data.decode("utf-8"))
        return self._handle_bytes_from_json(parsed)

    def serialize(self, data: dict) -> bytes:
        """Serialize Python dict to JSON bytes.

        Args:
            data: Python dict (may contain bytes fields)

        Returns:
            UTF-8 encoded JSON bytes
        """
        json_safe = self._handle_bytes_for_json(data)
        return json.dumps(json_safe).encode("utf-8")


class MessageSerializer:
    """Factory class for creating the appropriate serializer based on format."""

    def __init__(
        self,
        input_format: str = "avro",
        output_format: str = "json",
        avro_schema_path: Optional[str] = None,
        output_avro_schema_path: Optional[str] = None,
        skip_cutouts: bool = True,
        logger: Optional[BridgeLogger] = None,
    ):
        """Initialize serializers based on configuration.

        Args:
            input_format: Input message format ('avro', 'json', 'auto')
            output_format: Output message format ('avro', 'json')
            avro_schema_path: Path to input Avro schema (required for avro input)
            output_avro_schema_path: Path to output Avro schema (required for avro output)
            skip_cutouts: Whether to remove cutout data (large binary fields) from messages
            logger: Optional logger
        """
        self.input_format = input_format
        self.output_format = output_format
        self.skip_cutouts = skip_cutouts
        self.logger = logger

        # Input serializers
        self.avro_deserializer: Optional[AvroSerializer] = None
        self.json_deserializer = JsonSerializer(logger)

        if input_format in ("avro", "auto") and avro_schema_path:
            self.avro_deserializer = AvroSerializer(avro_schema_path, logger)

        # Output serializers
        self.json_serializer = JsonSerializer(logger)
        self.avro_serializer: Optional[AvroSerializer] = None

        if output_format == "avro" and output_avro_schema_path:
            self.avro_serializer = AvroSerializer(output_avro_schema_path, logger)

    def _strip_cutouts(self, data: dict) -> dict:
        """Remove cutout fields to reduce message size.

        Cutouts contain large binary FITS images that are often not needed
        for preprocessing/inference.
        """
        if not self.skip_cutouts:
            return data

        cutout_fields = ["cutoutScience", "cutoutTemplate", "cutoutDifference"]
        return {k: v for k, v in data.items() if k not in cutout_fields}

    def deserialize(self, data: bytes) -> dict:
        """Deserialize message bytes to Python dict.

        Args:
            data: Raw message bytes

        Returns:
            Deserialized Python dict

        Raises:
            ValueError: If deserialization fails
        """
        if self.input_format == "avro":
            if not self.avro_deserializer:
                raise ValueError("Avro deserializer not configured")
            result = self.avro_deserializer.deserialize(data)
        elif self.input_format == "json":
            result = self.json_deserializer.deserialize(data)
        elif self.input_format == "auto":
            # Try JSON first (more forgiving), then Avro
            try:
                result = self.json_deserializer.deserialize(data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                if not self.avro_deserializer:
                    raise ValueError("Avro deserializer not configured for auto mode")
                result = self.avro_deserializer.deserialize(data)
        else:
            raise ValueError(f"Unknown input format: {self.input_format}")

        return self._strip_cutouts(result)

    def serialize(self, data: dict) -> bytes:
        """Serialize Python dict to message bytes.

        Args:
            data: Python dict

        Returns:
            Serialized bytes in configured output format
        """
        if self.output_format == "json":
            return self.json_serializer.serialize(data)
        elif self.output_format == "avro":
            if not self.avro_serializer:
                raise ValueError("Avro serializer not configured for output")
            return self.avro_serializer.serialize(data)
        else:
            raise ValueError(f"Unknown output format: {self.output_format}")
