"""
Main Kafka Bridge orchestrator.

Coordinates consumption, API calls, and production in a continuous loop.
"""

import signal
from typing import List, Optional, Tuple

from confluent_kafka import Message

from kafka_bridge.api_client import APIClient
from kafka_bridge.config import BridgeConfig
from kafka_bridge.consumer import KafkaConsumerWrapper
from kafka_bridge.logger import BridgeLogger
from kafka_bridge.producer import KafkaProducerWrapper
from kafka_bridge.serializers import MessageSerializer


class KafkaBridge:
    """Main bridge class that orchestrates the consume-process-produce loop."""

    def __init__(self, config: Optional[BridgeConfig] = None):
        """Initialize the Kafka Bridge.

        Args:
            config: Bridge configuration (default: load from environment)
        """
        self.config = config or BridgeConfig()
        self.config.validate()

        self.logger = BridgeLogger(self.config)

        # Initialize serializers
        self.input_serializer = MessageSerializer(
            input_format=self.config.input_format,
            output_format="json",  # Internal format for API
            avro_schema_path=self.config.avro_schema_path,
            skip_cutouts=self.config.skip_cutouts,
            logger=self.logger,
        )

        self.output_serializer = MessageSerializer(
            input_format="json",  # API returns JSON
            output_format=self.config.output_format,
            output_avro_schema_path=self.config.output_avro_schema_path,
            skip_cutouts=self.config.skip_cutouts,
            logger=self.logger,
        )

        # Initialize components
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

        self.api_client = APIClient(self.config, self.logger)

        self._running = False
        self._setup_signal_handlers()

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

    def connect(self) -> None:
        """Connect to Kafka and verify API availability."""
        self.logger.info("Starting Kafka Bridge", config=str(self.config))

        # Connect to Kafka
        self.consumer.connect()
        self.producer.connect()

        # Wait for API to be available
        if not self.api_client.wait_for_api(timeout=60):
            raise RuntimeError("API is not available")

    def close(self) -> None:
        """Close all connections."""
        self.logger.info("Closing Kafka Bridge")
        self.consumer.close()
        self.producer.close()
        self.api_client.close()
        self.logger.log_metrics()

    def _extract_key(self, data: dict) -> Optional[str]:
        """Extract a message key from the data.

        Uses objectId for ZTF alerts, or falls back to candid.
        """
        return data.get("objectId") or str(data.get("candid", ""))

    def _process_batch(
        self,
        batch: List[Tuple[dict, Message]],
    ) -> Tuple[List[dict], List[Message]]:
        """Process a batch through the API.

        Args:
            batch: List of (data, message) tuples

        Returns:
            Tuple of (results, messages_to_commit)
        """
        data_list = [item[0] for item in batch]
        messages = [item[1] for item in batch]

        # Check if this is an MLflow model endpoint
        if "/invocations" in self.config.api_endpoint:
            results = self.api_client.call_mlflow_invocations(data_list)
        else:
            results = self.api_client.call_batch(data_list)

        if results is None:
            self.logger.error(
                "Batch processing failed, no results from API",
                batch_size=len(batch),
            )
            return [], []

        # Ensure results match input count
        if len(results) != len(data_list):
            self.logger.warning(
                "Result count mismatch",
                input_count=len(data_list),
                output_count=len(results),
            )
            # Pad or truncate results
            if len(results) < len(data_list):
                results.extend([{}] * (len(data_list) - len(results)))
            else:
                results = results[: len(data_list)]

        return results, messages

    def _produce_results(
        self,
        results: List[dict],
        original_data: List[dict],
    ) -> None:
        """Produce results to the output topic.

        Args:
            results: Processed results from API
            original_data: Original input data for key extraction
        """
        for i, result in enumerate(results):
            # Enrich result with original data reference
            enriched = {
                "result": result,
                "source": {
                    "objectId": (
                        original_data[i].get("objectId") if i < len(original_data) else None
                    ),
                    "candid": original_data[i].get("candid") if i < len(original_data) else None,
                },
                "bridge": self.config.bridge_name,
            }

            key = self._extract_key(original_data[i]) if i < len(original_data) else None

            self.producer.produce(enriched, key=key)

        # Flush to ensure delivery
        self.producer.flush(timeout=10)

    def run(self) -> None:
        """Run the bridge in a continuous loop."""
        self._running = True

        try:
            self.connect()

            self.logger.info(
                "Bridge running",
                input_topic=self.config.input_topic,
                output_topic=self.config.output_topic,
                api_url=f"{self.config.api_url}{self.config.api_endpoint}",
            )

            for batch in self.consumer.iter_batches():
                if not self._running:
                    break

                if not batch:
                    continue

                # Extract data for processing
                data_list = [item[0] for item in batch]

                # Process through API
                results, messages = self._process_batch(batch)

                if results:
                    # Produce results
                    self._produce_results(results, data_list)

                    # Commit offsets after successful processing
                    if messages:
                        self.consumer.commit(messages[-1])

                # Log periodic metrics
                if self.logger.metrics["messages_consumed"] % 100 == 0:
                    self.logger.log_metrics()

        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")

        except Exception as e:
            self.logger.exception("Bridge error", error=str(e))
            raise

        finally:
            self.close()

    def stop(self) -> None:
        """Stop the bridge gracefully."""
        self._running = False
        self.consumer.stop()


def main():
    """Entry point for the bridge."""
    config = BridgeConfig()

    # Print configuration (without sensitive data)
    print("Starting Kafka Bridge with configuration:")
    print(config)

    bridge = KafkaBridge(config)
    bridge.run()


if __name__ == "__main__":
    main()
