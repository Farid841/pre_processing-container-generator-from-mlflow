"""
Kafka producer module for the bridge.

Handles message production with proper delivery confirmation.
"""

from typing import Callable, List, Optional

from confluent_kafka import Producer

from kafka_bridge.config import BridgeConfig
from kafka_bridge.logger import BridgeLogger
from kafka_bridge.serializers import MessageSerializer


class KafkaProducerWrapper:
    """Wrapper around confluent-kafka Producer with delivery tracking."""

    def __init__(
        self,
        config: BridgeConfig,
        serializer: MessageSerializer,
        logger: BridgeLogger,
    ):
        """Initialize the Kafka producer.

        Args:
            config: Bridge configuration
            serializer: Message serializer for serialization
            logger: Logger instance
        """
        self.config = config
        self.serializer = serializer
        self.logger = logger
        self.producer: Optional[Producer] = None
        self._delivery_failures: List[str] = []

    def connect(self) -> None:
        """Connect to Kafka."""
        kafka_config = self.config.get_kafka_producer_config()
        self.producer = Producer(kafka_config)

        self.logger.info(
            "Producer connected to Kafka",
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            output_topic=self.config.output_topic,
        )

    def close(self) -> None:
        """Flush and close the producer."""
        if self.producer:
            # Wait for all messages to be delivered
            remaining = self.producer.flush(timeout=30)
            if remaining > 0:
                self.logger.warning(
                    "Producer closed with undelivered messages",
                    remaining=remaining,
                )
            else:
                self.logger.info("Producer closed successfully")

    def _delivery_callback(self, err, msg) -> None:
        """Handle message delivery confirmation callback."""
        if err:
            error_msg = f"Delivery failed: {err}"
            self._delivery_failures.append(error_msg)
            self.logger.error(
                "Message delivery failed",
                error=str(err),
                topic=msg.topic() if msg else "unknown",
            )
        else:
            self.logger.record_produced()
            self.logger.debug(
                "Message delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def produce(
        self,
        data: dict,
        topic: Optional[str] = None,
        key: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> None:
        """Produce a single message.

        Args:
            data: Message data as Python dict
            topic: Target topic (default: config.output_topic)
            key: Optional message key
            headers: Optional message headers
        """
        if not self.producer:
            raise RuntimeError("Producer not connected")

        topic = topic or self.config.output_topic

        try:
            serialized = self.serializer.serialize(data)

            kwargs = {
                "topic": topic,
                "value": serialized,
                "callback": self._delivery_callback,
            }

            if key:
                kwargs["key"] = key.encode("utf-8") if isinstance(key, str) else key

            if headers:
                kwargs["headers"] = [
                    (k, v.encode("utf-8") if isinstance(v, str) else v) for k, v in headers.items()
                ]

            self.producer.produce(**kwargs)

            # Trigger delivery callbacks without blocking
            self.producer.poll(0)

        except Exception as e:
            self.logger.error(
                "Failed to produce message",
                error=str(e),
                topic=topic,
            )
            raise

    def produce_batch(
        self,
        messages: List[dict],
        topic: Optional[str] = None,
        key_extractor: Optional[Callable[[dict], str]] = None,
    ) -> int:
        """Produce a batch of messages.

        Args:
            messages: List of message data dicts
            topic: Target topic (default: config.output_topic)
            key_extractor: Optional function to extract key from message

        Returns:
            Number of messages successfully queued
        """
        topic = topic or self.config.output_topic
        queued = 0

        for msg in messages:
            try:
                key = key_extractor(msg) if key_extractor else None
                self.produce(msg, topic=topic, key=key)
                queued += 1
            except Exception as e:
                self.logger.error(
                    "Failed to queue message in batch",
                    error=str(e),
                )

        self.logger.debug(
            "Batch queued for production",
            queued=queued,
            total=len(messages),
            topic=topic,
        )

        return queued

    def flush(self, timeout: float = 30.0) -> int:
        """Flush all pending messages.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in queue (0 if all delivered)
        """
        if not self.producer:
            return 0

        remaining = self.producer.flush(timeout=timeout)

        if remaining > 0:
            self.logger.warning(
                "Flush timeout with pending messages",
                remaining=remaining,
            )

        return remaining

    def produce_to_dlq(
        self,
        original_data: bytes,
        error: str,
        source_topic: str,
    ) -> None:
        """Produce a failed message to the dead letter queue.

        Args:
            original_data: Original raw message bytes
            error: Error description
            source_topic: Original topic the message came from
        """
        if not self.config.dead_letter_topic:
            return

        if not self.producer:
            raise RuntimeError("Producer not connected")

        dlq_message = {
            "original_value": original_data.hex(),  # Store as hex for safety
            "error": error,
            "source_topic": source_topic,
            "bridge_name": self.config.bridge_name,
        }

        try:
            self.producer.produce(
                topic=self.config.dead_letter_topic,
                value=self.serializer.serialize(dlq_message),
                callback=self._delivery_callback,
            )
            self.logger.info(
                "Message sent to DLQ",
                dlq_topic=self.config.dead_letter_topic,
                error=error,
            )
        except Exception as e:
            self.logger.error(
                "Failed to send to DLQ",
                error=str(e),
            )
