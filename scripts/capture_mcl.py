"""Capture a real MCL record for a breaking schema change, as a test fixture.

Drops a column from a seeded table and records the exact envelope DataHub puts
on Kafka in response. The captured record is what the projection's regression
test replays, so the contract is pinned to observed DataHub behaviour rather
than to an assumption about it.

Usage:
    python scripts/capture_mcl.py [--table raw.transactions] [--column device_fingerprint]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from datahub.emitter.rest_emitter import DatahubRestEmitter

from foreshock.estate import drop_column
from foreshock.kafka_source import MCL_VERSIONED_TOPIC, SourceConfig

POLL_BUDGET_SECONDS = 60.0


def _jsonable(value: Any) -> Any:
    """Make an Avro-decoded record JSON-serialisable without losing content.

    Aspect payloads arrive as raw bytes holding JSON. Decoding them to text
    keeps the fixture readable and diffable; the test re-encodes before feeding
    the projection so the bytes path stays covered.
    """
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="raw.transactions")
    parser.add_argument("--column", default="device_fingerprint")
    parser.add_argument("--gms", default="http://127.0.0.1:8080")
    parser.add_argument(
        "--out",
        default="tests/fixtures/real_mcl_schema_drop.json",
        help="Where to write the captured envelope.",
    )
    args = parser.parse_args()

    config = SourceConfig(group_id="foreshock-capture")
    registry = SchemaRegistryClient({"url": config.schema_registry_url})
    schema = registry.get_latest_version(f"{MCL_VERSIONED_TOPIC}-value").schema
    deserializer = AvroDeserializer(registry, schema.schema_str)

    consumer = Consumer(config.consumer_settings())

    # Pin the read to the current end of the log, so the only records seen are
    # the ones this run causes.
    metadata = consumer.list_topics(MCL_VERSIONED_TOPIC, timeout=20)
    partitions = list(metadata.topics[MCL_VERSIONED_TOPIC].partitions)
    starts = []
    for partition in partitions:
        _, high = consumer.get_watermark_offsets(
            TopicPartition(MCL_VERSIONED_TOPIC, partition), timeout=20
        )
        starts.append(TopicPartition(MCL_VERSIONED_TOPIC, partition, high))
    consumer.assign(starts)
    print(f"watching from offsets: {[(p.partition, p.offset) for p in starts]}")

    emitter = DatahubRestEmitter(gms_server=args.gms)
    urn = drop_column(emitter, args.table, args.column)
    print(f"dropped {args.column} from {args.table}")
    print(f"waiting for schemaMetadata MCL on {urn}")

    captured: dict[str, Any] | None = None
    spent = 0.0
    while spent < POLL_BUDGET_SECONDS and captured is None:
        message = consumer.poll(1.0)
        spent += 1.0
        if message is None or message.error():
            continue
        record = deserializer(
            message.value(),
            SerializationContext(message.topic(), MessageField.VALUE),
        )
        if record.get("entityUrn") == urn and record.get("aspectName") == "schemaMetadata":
            captured = record

    consumer.close()

    if captured is None:
        print("no matching MCL record observed", file=sys.stderr)
        return 1

    payload = _jsonable(captured)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(f"captured -> {out}")
    print(f"  changeType          : {payload.get('changeType')}")
    print(f"  previousAspectValue : {'PRESENT' if payload.get('previousAspectValue') else 'ABSENT'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
