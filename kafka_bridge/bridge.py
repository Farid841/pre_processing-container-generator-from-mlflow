"""
Main Kafka Bridge orchestrator.

Coordinates consumption, API calls, and production in a continuous loop.
"""

import signal
from typing import Any, List, Optional, Tuple

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

    def _extract_key(self, data: Any) -> Optional[str]:
        """Extract a message key from the data.

        Uses objectId for ZTF alerts, or falls back to candid.
        Handles both dict and list inputs.
        """
        if data is None:
            return None
        if isinstance(data, dict):
            return data.get("objectId") or str(data.get("candid", ""))
        # For lists or other types, return None (no key)
        return None

    def _process_batch(
        self,
        batch: List[Tuple[Any, Message]],
    ) -> Tuple[List[dict], List[Message]]:
        """Process a batch through the API.

        Args:
            batch: List of (data, message) tuples
                  data can be dict or list (for JSON arrays)

        Returns:
            Tuple of (results, messages_to_commit)
        """
        # Handle lists in messages: if a message is a list, flatten it
        data_list = []
        messages = []

        self.logger.debug(
            "Processing batch",
            batch_size=len(batch),
            first_data_type=type(batch[0][0]).__name__ if batch else "empty",
        )

        for data, msg in batch:
            # Skip None or invalid data
            if data is None:
                self.logger.warning(
                    "Skipping None data in batch",
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                continue

            if isinstance(data, list):
                # Message contains a list from preprocessing
                if len(data) == 0:
                    self.logger.warning(
                        "Empty list in message, skipping",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                    continue

                # Check if list contains scalars (float, int) - this is a single feature vector
                # or if it contains lists/dicts - these are multiple feature vectors
                first_item = next((item for item in data if item is not None), None)

                if first_item is None:
                    # All items are None, skip
                    self.logger.warning(
                        "All items in list are None, skipping",
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                    )
                    continue

                if isinstance(first_item, (int, float)) or first_item is None:
                    # List of scalars = single feature vector
                    # Replace None with 0.0 to keep the correct shape for the model
                    cleaned_list = [item if item is not None else 0.0 for item in data]
                    if cleaned_list:
                        data_list.append(cleaned_list)
                        messages.append(msg)
                    else:
                        self.logger.warning(
                            "Empty feature vector, skipping",
                            topic=msg.topic(),
                            partition=msg.partition(),
                            offset=msg.offset(),
                        )
                elif isinstance(first_item, list):
                    # List of lists = multiple feature vectors
                    for item in data:
                        if item is not None:
                            data_list.append(item)
                            messages.append(msg)
                elif isinstance(first_item, dict):
                    # List of dicts = multiple records
                    for item in data:
                        if item is not None:
                            data_list.append(item)
                            messages.append(msg)
                else:
                    # Unknown format, treat as single feature vector
                    filtered_list = [item for item in data if item is not None]
                    if filtered_list:
                        data_list.append(filtered_list)
                        messages.append(msg)
            else:
                # Single dict/list message
                data_list.append(data)
                messages.append(msg)

        # Filter out None values before sending to API
        valid_data_list = []
        valid_messages = []
        for i, data_item in enumerate(data_list):
            if data_item is None:
                self.logger.warning(
                    "None data in data_list after processing, skipping",
                    index=i,
                )
                continue
            valid_data_list.append(data_item)
            valid_messages.append(messages[i] if i < len(messages) else messages[-1])

        if not valid_data_list:
            self.logger.error(
                "No valid data after filtering None values",
                original_batch_size=len(batch),
                filtered_size=0,
            )
            return [], []

        # Check if this is an MLflow model endpoint
        if "/invocations" in self.config.api_endpoint:
            results = self.api_client.call_mlflow_invocations(valid_data_list)
        else:
            results = self.api_client.call_batch(valid_data_list)

        if results is None:
            self.logger.error(
                "Batch processing failed, no results from API",
                batch_size=len(batch),
            )
            return [], []

        # Ensure results match input count
        if len(results) != len(valid_data_list):
            self.logger.warning(
                "Result count mismatch",
                input_count=len(valid_data_list),
                output_count=len(results),
            )
            # Pad or truncate results
            if len(results) < len(valid_data_list):
                results.extend([{}] * (len(valid_data_list) - len(results)))
            else:
                results = results[: len(valid_data_list)]

        return results, valid_messages

    def _produce_results(
        self,
        results: List[dict],
        original_data: List[Any],
    ) -> None:
        """Produce results to the output topic.

        Args:
            results: Processed results from API
            original_data: Original input data for key extraction (can be dict or list)
        """
        for i, result in enumerate(results):
            # Skip None results
            if result is None:
                continue

            # Extract source info from original data
            source_obj = original_data[i] if i < len(original_data) else None
            source_info = {}
            if isinstance(source_obj, dict):
                source_info = {
                    "objectId": source_obj.get("objectId"),
                    "candid": source_obj.get("candid"),
                }

            # Enrich result with original data reference
            enriched = {
                "result": result,
                "source": source_info,
                "bridge": self.config.bridge_name,
            }

            key = self._extract_key(source_obj) if source_obj is not None else None

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
