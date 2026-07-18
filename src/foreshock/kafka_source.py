"""Kafka boundary: DataHub's Metadata Change Log topic to typed Foreshock events.

DataHub already writes every aspect mutation to Kafka as an Avro-encoded
``MetadataChangeLog`` record. Foreshock does not add a shipping mechanism; it
subscribes to the one DataHub ships and gives agents a typed stream instead of a
GraphQL polling loop.

This module owns all Kafka and Schema Registry concerns. It decodes a record and
hands it to :func:`foreshock.mcl_event.from_mcl_record`, so the projection stays
pure and unit-testable without a broker.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Mapping

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from foreshock.mcl_event import MclEvent, from_mcl_record

# The versioned log carries entity aspects (schemas, ownership, deprecation).
# The timeseries topic carries profiling/usage stats, which cannot invalidate a
# downstream consumer, so Foreshock does not subscribe to it.
MCL_VERSIONED_TOPIC = "MetadataChangeLog_Versioned_v1"

# DataHub Core serves a Confluent-compatible registry from GMS itself
# (SCHEMA_REGISTRY_TYPE=INTERNAL), rather than running a separate container.
DEFAULT_SCHEMA_REGISTRY_PATH = "/schema-registry/api/"


@dataclass(frozen=True)
class SourceConfig:
    """Connection settings for one MCL subscription."""

    bootstrap_servers: str = "127.0.0.1:9092"
    schema_registry_url: str = "http://127.0.0.1:8080/schema-registry/api/"
    group_id: str = "foreshock"
    topic: str = MCL_VERSIONED_TOPIC
    # "earliest" replays the estate's whole history, which is what a cold-start
    # blast-radius index wants; "latest" is right for a warm subscriber.
    auto_offset_reset: str = "earliest"
    # Podman publishes container ports on IPv4 only, while the broker advertises
    # itself as "localhost". On a dual-stack host librdkafka resolves that to
    # ::1 and every connection fails after bootstrap succeeds. Pinning the
    # address family keeps the advertised name usable.
    broker_address_family: str = "v4"

    def consumer_settings(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "broker.address.family": self.broker_address_family,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            # Foreshock commits only after a subscriber has handled an event, so
            # a crash mid-walk replays rather than silently drops.
            "enable.auto.commit": False,
        }


class MclSource:
    """A subscription to DataHub's metadata change log.

    Iterating yields :class:`MclEvent` values. Records the projection cannot use
    (no entity urn, no aspect name) are skipped rather than raised, because a
    single unusable record must not stall an agent's stream.
    """

    def __init__(self, config: SourceConfig | None = None) -> None:
        self.config = config or SourceConfig()
        registry = SchemaRegistryClient({"url": self.config.schema_registry_url})
        subject = f"{self.config.topic}-value"
        schema = registry.get_latest_version(subject).schema
        self._deserializer = AvroDeserializer(registry, schema.schema_str)
        self._consumer = Consumer(self.config.consumer_settings())
        self._consumer.subscribe([self.config.topic])

    def poll(self, timeout: float = 1.0) -> MclEvent | None:
        """Return the next usable event, or ``None`` if the timeout expired."""
        message = self._consumer.poll(timeout)
        if message is None:
            return None
        error = message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(error)

        record = self._deserializer(
            message.value(),
            SerializationContext(message.topic(), MessageField.VALUE),
        )
        if not isinstance(record, Mapping):
            return None
        return from_mcl_record(record)

    def commit(self) -> None:
        """Acknowledge everything handled so far."""
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()

    def __iter__(self) -> Iterator[MclEvent]:
        while True:
            event = self.poll()
            if event is not None:
                yield event


@contextmanager
def open_source(config: SourceConfig | None = None) -> Iterator[MclSource]:
    """Open an :class:`MclSource` and guarantee the consumer is closed."""
    source = MclSource(config)
    try:
        yield source
    finally:
        source.close()
