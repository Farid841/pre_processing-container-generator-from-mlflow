"""
Kafka consumer module for the bridge.

Handles message consumption with batching and proper offset management.
"""

import time
from typing import Iterator, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from kafka_bridge.config import BridgeConfig
from kafka_bridge.logger import BridgeLogger
from kafka_bridge.serializers import MessageSerializer


class KafkaConsumerWrapper:
    """Wrapper around confluent-kafka Consumer with batching support."""

    def __init__(
        self,
        config: BridgeConfig,
        serializer: MessageSerializer,
        logger: BridgeLogger,
    ):
        """Initialize the Kafka consumer.

        Args:
            config: Bridge configuration
            serializer: Message serializer for deserialization
            logger: Logger instance
        """
        self.config = config
        self.serializer = serializer
        self.logger = logger
        self.consumer: Optional[Consumer] = None
        self._running = False

    def connect(self) -> None:
        """Connect to Kafka and subscribe to the input topic."""
        kafka_config = self.config.get_kafka_consumer_config()
        self.consumer = Consumer(kafka_config)
        self.consumer.subscribe([self.config.input_topic])

        self.logger.info(
            "Connected to Kafka",
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            topic=self.config.input_topic,
            group_id=self.config.consumer_group_id,
        )

    def close(self) -> None:
        """Close the consumer connection."""
        self._running = False
        if self.consumer:
            self.consumer.close()
            self.logger.info("Consumer closed")

    def _poll_single(self, timeout: float = 1.0) -> Optional[Message]:
        """Poll for a single message.

        Args:
            timeout: Poll timeout in seconds

        Returns:
            Message if available, None otherwise
        """
        if not self.consumer:
            raise RuntimeError("Consumer not connected")

        msg = self.consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, not an error
                return None
            raise KafkaException(msg.error())

        return msg

    def consume_batch(
        self,
        batch_size: Optional[int] = None,
        timeout_ms: Optional[int] = None,
    ) -> List[Tuple[dict, Message]]:
        """Consume a batch of messages.

        Args:
            batch_size: Maximum messages to consume (default: config.batch_size)
            timeout_ms: Maximum time to wait for batch (default: config.batch_timeout_ms)

        Returns:
            List of (deserialized_data, original_message) tuples
        """
        batch_size = batch_size or self.config.batch_size
        timeout_ms = timeout_ms or self.config.batch_timeout_ms

        messages: List[Tuple[dict, Message]] = []
        start_time = time.time() * 1000  # ms

        while len(messages) < batch_size:
            elapsed = (time.time() * 1000) - start_time
            remaining_timeout = max(0, (timeout_ms - elapsed) / 1000)

            if remaining_timeout <= 0:
                break

            msg = self._poll_single(timeout=min(remaining_timeout, 0.1))

            if msg is None:
                continue

            try:
                data = self.serializer.deserialize(msg.value())
                messages.append((data, msg))
                self.logger.record_consumed()

            except Exception as e:
                self.logger.record_deserialization_error()
                self.logger.error(
                    "Failed to deserialize message",
                    error=str(e),
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                # Continue processing other messages
                continue

        if messages:
            self.logger.debug(
                "Consumed batch",
                batch_size=len(messages),
                topic=self.config.input_topic,
            )

        return messages

    def commit(self, message: Optional[Message] = None) -> None:
        """Commit offsets.

        Args:
            message: Specific message to commit up to, or None for all consumed
        """
        if not self.consumer:
            return

        if message:
            self.consumer.commit(message=message, asynchronous=False)
        else:
            self.consumer.commit(asynchronous=False)

    def iter_batches(
        self,
        batch_size: Optional[int] = None,
        timeout_ms: Optional[int] = None,
    ) -> Iterator[List[Tuple[dict, Message]]]:
        """Iterate over batches of messages indefinitely.

        Args:
            batch_size: Maximum messages per batch
            timeout_ms: Maximum time to wait per batch

        Yields:
            Non-empty batches of (data, message) tuples
        """
        self._running = True

        while self._running:
            batch = self.consume_batch(batch_size, timeout_ms)
            if batch:
                yield batch

    def stop(self) -> None:
        """Signal the consumer to stop iterating."""
        self._running = False
