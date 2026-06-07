"""Fetch an AVRO schema from a Kafka schema topic.

The schema topic is produced by spark_ztf_inference_feed.py (and
spark_ztf_transfer.py): each message has the Avro schema JSON string as its
*key*. This module reads the first available message and returns the parsed
schema dict.
"""

import json
import time

from confluent_kafka import Consumer, KafkaError


def fetch_schema_from_topic(
    schema_topic: str,
    kafka_config: dict,
    timeout: float = 30.0,
) -> dict:
    """Return the Avro schema dict stored in the key of a schema topic.

    Parameters
    ----------
    schema_topic : str
        Kafka topic that contains schema messages (e.g. finference_in_xxx_schema).
    kafka_config : dict
        Confluent Kafka base config (bootstrap.servers, security settings, …).
        group.id and auto.offset.reset are overridden internally.
    timeout : float
        Maximum seconds to wait for a message.

    Returns
    -------
    dict
        Parsed Avro schema dict ready for fastavro.parse_schema().

    Raises
    ------
    RuntimeError
        If no schema message is received within *timeout* seconds.
    """
    consumer_config = {
        **kafka_config,
        "group.id": f"schema-fetcher-{schema_topic}-{time.time_ns()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([schema_topic])

    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = consumer.poll(timeout=min(1.0, deadline - time.time()))
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(
                    f"Kafka error reading schema topic '{schema_topic}': {msg.error()}"
                )
            schema_json = msg.key().decode("utf-8")
            return json.loads(schema_json)
    finally:
        consumer.close()

    raise RuntimeError(f"No schema message received from topic '{schema_topic}' within {timeout}s")
