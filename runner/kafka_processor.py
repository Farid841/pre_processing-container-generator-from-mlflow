#!/usr/bin/env python3
"""
Kafka processor for preprocessing container.

Consumes messages from Kafka, processes them with pre_processing(),
and produces results to another Kafka topic.
"""

import logging
import os
import signal
import sys
from pathlib import Path
from typing import Callable, Optional

# Add parent directory to path to import kafka_bridge modules
if str(Path(__file__).parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka import Message

from kafka_bridge.config import BridgeConfig
from kafka_bridge.consumer import KafkaConsumerWrapper
from kafka_bridge.logger import BridgeLogger
from kafka_bridge.producer import KafkaProducerWrapper
from kafka_bridge.serializers import MessageSerializer
from runner.runner import load_preprocessing

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class KafkaProcessor:
    """Kafka processor that consumes, processes, and produces messages."""

    def __init__(self, pre_processing_func: Callable):
        """Initialize the Kafka processor.

        Args:
            pre_processing_func: The preprocessing function to call
        """
        self.pre_processing_func = pre_processing_func
        self.config = self._create_config()
        self.config.validate()

        self.logger = BridgeLogger(self.config)

        # Initialize serializers
        self.input_serializer = MessageSerializer(
            input_format=self.config.input_format,
            output_format="json",  # Internal format
            avro_schema_path=self.config.avro_schema_path,
            skip_cutouts=self.config.skip_cutouts,
            logger=self.logger,
        )

        self.output_serializer = MessageSerializer(
            input_format="json",  # Internal format
            output_format=self.config.output_format,
            output_avro_schema_path=self.config.output_avro_schema_path,
            skip_cutouts=self.config.skip_cutouts,
            logger=self.logger,
        )

        # Initialize Kafka components
        self.consumer = KafkaConsumerWrapper(
            self.config,
            self.input_serializer,
            self.logger,
        )

        self.producer = KafkaProducerWrapper(
            self.config,
            self.output_serializer,
            self.logger,
        )

        self._running = False
        self._setup_signal_handlers()

    def _create_config(self) -> BridgeConfig:
        """Create configuration from environment variables."""
        # Override default values for preprocessing
        os.environ.setdefault("INPUT_TOPIC", "fink-alert")
        os.environ.setdefault("OUTPUT_TOPIC", "preprocessed")
        os.environ.setdefault("CONSUMER_GROUP_ID", "preprocessing-group")
        os.environ.setdefault("BRIDGE_NAME", "preprocessing-kafka-processor")

        return BridgeConfig()

    def _setup_signal_handlers(self) -> None:
        """Set up graceful shutdown on SIGTERM/SIGINT."""

        def signal_handler(signum, frame):
            self.logger.info(
                "Received shutdown signal",
                signal=signal.Signals(signum).name,
            )
            self.stop()

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _extract_key(self, data: dict) -> Optional[str]:
        """Extract a message key from the data.

        Uses objectId for ZTF alerts, or falls back to candid.
        """
        return data.get("objectId") or str(data.get("candid", ""))

    def _process_batch(self, batch: list[tuple[dict, Message]]) -> None:
        """Process a batch of messages.

        Args:
            batch: List of (data, message) tuples
        """
        if not batch:
            return

        results = []
        messages_to_commit = []

        for data, message in batch:
            try:
                # Call preprocessing function directly
                result = self.pre_processing_func(data)

                # Extract key for output message
                key = self._extract_key(data)

                # Produce to output topic
                self.producer.produce(result, key=key)
                results.append(result)
                messages_to_commit.append(message)

            except Exception as e:
                self.logger.error(
                    "Preprocessing failed for message",
                    error=str(e),
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                )
                # Continue processing other messages
                continue

        # Flush producer to ensure delivery
        if results:
            self.producer.flush(timeout=10)
            self.logger.debug(
                "Processed batch",
                input_count=len(batch),
                output_count=len(results),
            )

        # Commit offsets after successful processing
        if messages_to_commit:
            self.consumer.commit(messages_to_commit[-1])

    def connect(self) -> None:
        """Connect to Kafka."""
        self.logger.info("Starting Kafka Processor", config=str(self.config))

        # Connect to Kafka
        self.consumer.connect()
        self.producer.connect()

        self.logger.info(
            "Kafka Processor ready",
            input_topic=self.config.input_topic,
            output_topic=self.config.output_topic,
        )

    def close(self) -> None:
        """Close all connections."""
        self.logger.info("Closing Kafka Processor")
        self.consumer.close()
        self.producer.close()
        self.logger.log_metrics()

    def run(self) -> None:
        """Run the processor in a continuous loop."""
        self._running = True

        try:
            self.connect()

            self.logger.info(
                "Kafka Processor running",
                input_topic=self.config.input_topic,
                output_topic=self.config.output_topic,
            )

            for batch in self.consumer.iter_batches():
                if not self._running:
                    break

                if not batch:
                    continue

                # Process batch
                self._process_batch(batch)

                # Log periodic metrics
                if self.logger.metrics["messages_consumed"] % 100 == 0:
                    self.logger.log_metrics()

        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")

        except Exception as e:
            self.logger.exception("Processor error", error=str(e))
            raise

        finally:
            self.close()

    def stop(self) -> None:
        """Stop the processor gracefully."""
        self._running = False
        self.consumer.stop()


def main():
    """Entry point for the Kafka processor."""
    try:
        logger.info("Loading preprocessing function...")
        pre_processing_func = load_preprocessing()
        logger.info("✅ Preprocessing loaded successfully")

        processor = KafkaProcessor(pre_processing_func)
        processor.run()

    except Exception as e:
        logger.error(f"❌ Failed to start Kafka processor: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
