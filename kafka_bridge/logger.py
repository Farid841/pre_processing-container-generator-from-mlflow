"""
Structured logging module for Kafka Bridge.

Supports both JSON and text output formats for flexibility in different environments.
"""

import json
import logging
import sys
from datetime import datetime
from typing import Any

from kafka_bridge.config import BridgeConfig


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def __init__(self, bridge_name: str = "kafka-bridge"):
        """Initialize JSON formatter.

        Args:
            bridge_name: Name of the bridge for logging context
        """
        super().__init__()
        self.bridge_name = bridge_name

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON string."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "bridge": self.bridge_name,
        }

        # Add extra fields if present
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)


class TextFormatter(logging.Formatter):
    """Human-readable text formatter."""

    def __init__(self, bridge_name: str = "kafka-bridge"):
        """Initialize text formatter.

        Args:
            bridge_name: Name of the bridge for logging context
        """
        super().__init__(
            fmt=f"%(asctime)s [{bridge_name}] %(levelname)s %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


class BridgeLogger:
    """Logger wrapper with structured logging support."""

    def __init__(self, config: BridgeConfig):
        """Initialize bridge logger.

        Args:
            config: Bridge configuration
        """
        self.config = config
        self.logger = logging.getLogger("kafka_bridge")
        self._setup_logger()

        # Metrics for monitoring
        self.metrics = {
            "messages_consumed": 0,
            "messages_produced": 0,
            "api_calls": 0,
            "api_errors": 0,
            "deserialization_errors": 0,
            "start_time": datetime.utcnow().isoformat() + "Z",
        }

    def _setup_logger(self) -> None:
        """Configure the logger based on config."""
        self.logger.setLevel(getattr(logging, self.config.log_level.upper()))
        self.logger.handlers.clear()

        # Choose formatter based on config
        if self.config.log_format == "json":
            formatter = JsonFormatter(self.config.bridge_name)
        else:
            formatter = TextFormatter(self.config.bridge_name)

        # Console handler (always)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # File handler (optional)
        if self.config.log_file:
            file_handler = logging.FileHandler(self.config.log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def _log_with_extra(self, level: int, message: str, **extra_fields: Any) -> None:
        """Log with extra structured fields."""
        record = self.logger.makeRecord(
            name=self.logger.name,
            level=level,
            fn="",
            lno=0,
            msg=message,
            args=(),
            exc_info=None,
        )
        record.extra_fields = extra_fields
        self.logger.handle(record)

    def info(self, message: str, **extra: Any) -> None:
        """Log info message with optional extra fields."""
        self._log_with_extra(logging.INFO, message, **extra)

    def debug(self, message: str, **extra: Any) -> None:
        """Log debug message with optional extra fields."""
        self._log_with_extra(logging.DEBUG, message, **extra)

    def warning(self, message: str, **extra: Any) -> None:
        """Log warning message with optional extra fields."""
        self._log_with_extra(logging.WARNING, message, **extra)

    def error(self, message: str, **extra: Any) -> None:
        """Log error message with optional extra fields."""
        self._log_with_extra(logging.ERROR, message, **extra)

    def exception(self, message: str, **extra: Any) -> None:
        """Log exception with traceback."""
        self.logger.exception(message, extra={"extra_fields": extra})

    # Metric tracking methods
    def record_consumed(self, count: int = 1) -> None:
        """Record messages consumed."""
        self.metrics["messages_consumed"] += count

    def record_produced(self, count: int = 1) -> None:
        """Record messages produced."""
        self.metrics["messages_produced"] += count

    def record_api_call(self, success: bool = True) -> None:
        """Record API call."""
        self.metrics["api_calls"] += 1
        if not success:
            self.metrics["api_errors"] += 1

    def record_deserialization_error(self) -> None:
        """Record deserialization error."""
        self.metrics["deserialization_errors"] += 1

    def get_metrics(self) -> dict:
        """Get current metrics."""
        return {
            **self.metrics,
            "current_time": datetime.utcnow().isoformat() + "Z",
        }

    def log_metrics(self) -> None:
        """Log current metrics."""
        self.info("Bridge metrics", **self.get_metrics())
