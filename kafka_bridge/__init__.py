"""
Kafka Bridge - A reusable sidecar for bridging Kafka topics with REST APIs.

This module provides a generic Kafka consumer/producer bridge that can:
- Consume messages from a Kafka topic (Avro or JSON format)
- Call a configurable REST API endpoint with the message data
- Produce the API response to an output Kafka topic
- Log all operations for monitoring and debugging

Typical usage:
    from kafka_bridge import KafkaBridge

    bridge = KafkaBridge()
    bridge.run()
"""

__version__ = "1.0.0"
__all__ = ["KafkaBridge", "BridgeConfig"]


def __getattr__(name):
    """Lazy import to avoid RuntimeWarning when running as module."""
    if name == "KafkaBridge":
        from kafka_bridge.bridge import KafkaBridge

        return KafkaBridge
    elif name == "BridgeConfig":
        from kafka_bridge.config import BridgeConfig

        return BridgeConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
