"""
Configuration module for Kafka Bridge.

All configuration is done via environment variables for easy Docker deployment.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BridgeConfig:
    """Configuration for the Kafka Bridge.

    All values can be set via environment variables.
    """

    # Kafka connection
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    )
    kafka_security_protocol: str = field(
        default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    )
    kafka_sasl_mechanism: Optional[str] = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_MECHANISM")
    )
    kafka_sasl_username: Optional[str] = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_USERNAME")
    )
    kafka_sasl_password: Optional[str] = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_PASSWORD")
    )

    # Consumer settings
    input_topic: str = field(default_factory=lambda: os.getenv("INPUT_TOPIC", "fink_alerts"))
    input_format: str = field(
        default_factory=lambda: os.getenv("INPUT_FORMAT", "avro")  # avro, json, auto
    )
    consumer_group_id: str = field(
        default_factory=lambda: os.getenv("CONSUMER_GROUP_ID", "kafka-bridge-group")
    )
    auto_offset_reset: str = field(
        default_factory=lambda: os.getenv("AUTO_OFFSET_RESET", "earliest")
    )

    # Producer settings
    output_topic: str = field(default_factory=lambda: os.getenv("OUTPUT_TOPIC", "preprocessed"))
    output_format: str = field(
        default_factory=lambda: os.getenv("OUTPUT_FORMAT", "json")  # avro, json
    )

    # Avro schema settings
    avro_schema_path: Optional[str] = field(default_factory=lambda: os.getenv("AVRO_SCHEMA_PATH"))
    schema_topic: Optional[str] = field(default_factory=lambda: os.getenv("SCHEMA_TOPIC"))
    output_avro_schema_path: Optional[str] = field(
        default_factory=lambda: os.getenv("OUTPUT_AVRO_SCHEMA_PATH")
    )

    # API settings
    api_url: str = field(default_factory=lambda: os.getenv("API_URL", "http://localhost:8000"))
    api_endpoint: str = field(
        default_factory=lambda: os.getenv("API_ENDPOINT", "/preprocess/batch")
    )
    api_health_endpoint: str = field(
        default_factory=lambda: os.getenv("API_HEALTH_ENDPOINT", "/health")
    )
    api_timeout: int = field(default_factory=lambda: int(os.getenv("API_TIMEOUT", "30")))
    api_retry_count: int = field(default_factory=lambda: int(os.getenv("API_RETRY_COUNT", "3")))
    api_retry_delay: float = field(
        default_factory=lambda: float(os.getenv("API_RETRY_DELAY", "1.0"))
    )

    # Batching settings
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "10")))
    batch_timeout_ms: int = field(
        default_factory=lambda: int(os.getenv("BATCH_TIMEOUT_MS", "1000"))
    )
    # Batch mode: stop after this many seconds with no messages (0 = disabled, run forever)
    idle_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("IDLE_TIMEOUT_SECONDS", "0"))
    )

    # Logging settings
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))
    log_file: Optional[str] = field(default_factory=lambda: os.getenv("LOG_FILE"))
    log_format: str = field(default_factory=lambda: os.getenv("LOG_FORMAT", "json"))  # json, text

    # Error handling
    dead_letter_topic: Optional[str] = field(default_factory=lambda: os.getenv("DEAD_LETTER_TOPIC"))
    skip_cutouts: bool = field(
        default_factory=lambda: os.getenv("SKIP_CUTOUTS", "true").lower() == "true"
    )

    # Bridge identification
    bridge_name: str = field(default_factory=lambda: os.getenv("BRIDGE_NAME", "kafka-bridge"))

    def _is_plaintext(self) -> bool:
        return self.kafka_security_protocol.upper() == "PLAINTEXT"

    def _is_sasl_protocol(self) -> bool:
        return self.kafka_security_protocol.upper() not in ("PLAINTEXT", "SSL")

    def get_kafka_consumer_config(self) -> dict:
        """Get Kafka consumer configuration dictionary."""
        config = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False,
        }

        if not self._is_plaintext():
            config["security.protocol"] = self.kafka_security_protocol

        if self._is_sasl_protocol():
            if self.kafka_sasl_mechanism:
                config["sasl.mechanism"] = self.kafka_sasl_mechanism
            if self.kafka_sasl_username:
                config["sasl.username"] = self.kafka_sasl_username
            if self.kafka_sasl_password:
                config["sasl.password"] = self.kafka_sasl_password

        return config

    def get_kafka_producer_config(self) -> dict:
        """Get Kafka producer configuration dictionary."""
        config = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
        }

        if not self._is_plaintext():
            config["security.protocol"] = self.kafka_security_protocol

        if self._is_sasl_protocol():
            if self.kafka_sasl_mechanism:
                config["sasl.mechanism"] = self.kafka_sasl_mechanism
            if self.kafka_sasl_username:
                config["sasl.username"] = self.kafka_sasl_username
            if self.kafka_sasl_password:
                config["sasl.password"] = self.kafka_sasl_password

        return config

    def validate(self) -> None:
        """Validate configuration and raise ValueError if invalid."""
        if self.input_format not in ("avro", "json", "auto"):
            raise ValueError(f"Invalid input_format: {self.input_format}")

        if self.output_format not in ("avro", "json"):
            raise ValueError(f"Invalid output_format: {self.output_format}")

        if self.input_format == "avro" and not self.avro_schema_path and not self.schema_topic:
            raise ValueError(
                "Either AVRO_SCHEMA_PATH or SCHEMA_TOPIC is required when INPUT_FORMAT is 'avro'"
            )

        if self.output_format == "avro" and not self.output_avro_schema_path:
            raise ValueError("OUTPUT_AVRO_SCHEMA_PATH is required when OUTPUT_FORMAT is 'avro'")

        if not self.api_url:
            raise ValueError("API_URL is required")

        if not self.api_endpoint:
            raise ValueError("API_ENDPOINT is required")

    def __str__(self) -> str:
        """Return a string representation with sensitive fields masked."""
        return (
            f"BridgeConfig(\n"
            f"  kafka_bootstrap_servers={self.kafka_bootstrap_servers},\n"
            f"  input_topic={self.input_topic},\n"
            f"  input_format={self.input_format},\n"
            f"  output_topic={self.output_topic},\n"
            f"  output_format={self.output_format},\n"
            f"  api_url={self.api_url},\n"
            f"  api_endpoint={self.api_endpoint},\n"
            f"  batch_size={self.batch_size},\n"
            f"  bridge_name={self.bridge_name}\n"
            f")"
        )
