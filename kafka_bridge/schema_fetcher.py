"""Fetch an AVRO schema from a Kafka schema topic.

The schema topic is produced by spark_ztf_inference_feed.py (and
spark_ztf_transfer.py): each message has the Avro schema JSON string as its
*key*. This module reads the first available message and returns the parsed
schema dict.
"""

import copy
import json
import time

from confluent_kafka import Consumer, KafkaError


def _fetch_raw_schema_from_topic(
    schema_topic: str,
    kafka_config: dict,
    timeout: float = 30.0,
) -> dict:
    """Return the raw Avro schema dict stored in the key of a schema topic."""
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
                if msg.error().code() in (
                    KafkaError._PARTITION_EOF,
                    KafkaError.UNKNOWN_TOPIC_OR_PART,
                ):
                    # Topic metadata can take a few seconds to propagate right
                    # after creation — keep polling until the deadline instead
                    # of failing on the first attempt.
                    continue
                raise RuntimeError(
                    f"Kafka error reading schema topic '{schema_topic}': {msg.error()}"
                )
            schema_json = msg.key().decode("utf-8")
            return json.loads(schema_json)
    finally:
        consumer.close()

    raise RuntimeError(f"No schema message received from topic '{schema_topic}' within {timeout}s")


def _fix_spark_schema(schema: dict) -> dict:
    """Wrap top-level plain-record fields in a ``[record, null]`` union.

    NOT USED. Was written under the assumption that Spark's Avro wire bytes
    always carry a union discriminator byte for nested nullable records (e.g.
    ``candidate``), even when the schema published to the schema topic
    declares them as a plain record. That assumption no longer holds: for the
    schemas currently published by spark_ztf_transfer.py /
    spark_ztf_inference_feed.py, the raw schema already matches the wire
    bytes, and applying this wrapping corrupts every message (fastavro raises
    ``IndexError: list index out of range`` on the union discriminator it
    expects but that isn't there). Verified against live topic data before
    removing the call in fetch_schema_from_topic. Kept here in case a future
    Spark/schema-converter version reintroduces the mismatch it was meant to
    fix.
    """
    fixed = copy.deepcopy(schema)
    for field in fixed.get("fields", []):
        t = field.get("type")
        if isinstance(t, dict) and t.get("type") == "record":
            field["type"] = [t, "null"]
    return fixed


def fetch_schema_from_topic(
    schema_topic: str,
    kafka_config: dict,
    timeout: float = 30.0,
) -> dict:
    """Return the Avro schema dict stored in the key of a schema topic.

    Parameters
    ----------
    schema_topic : str
        Kafka topic that contains schema messages (e.g. fink_ai_feed_xxx_schema).
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
    return _fetch_raw_schema_from_topic(schema_topic, kafka_config, timeout)
