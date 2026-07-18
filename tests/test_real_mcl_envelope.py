"""Regression test against a real DataHub MCL envelope.

Every other test in this suite builds records by hand from the Avro schema,
which proves the projection is self-consistent but not that it matches what
DataHub actually emits. The fixture replayed here was captured off
``MetadataChangeLog_Versioned_v1`` on DataHub Core v1.5.0.6 by dropping a column
from a seeded table (``scripts/capture_mcl.py``).

It exists to pin the two assumptions Foreshock is built on:

1. A breaking change is visible in the event itself — the envelope carries
   ``previousAspectValue``, so a column drop can be diffed without calling back
   into GMS. If DataHub ever stops populating it, this test fails and the
   architecture needs revisiting rather than a patch.
2. Real envelopes contain shapes the hand-written records do not, notably a null
   ``headers`` field and aspect payloads delivered as raw bytes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from foreshock.mcl_event import from_mcl_record

FIXTURE = Path(__file__).parent / "fixtures" / "real_mcl_schema_drop.json"

DROPPED_COLUMN = "device_fingerprint"
CAPTURED_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)"


def _as_wire_bytes(value: Any) -> Any:
    """Restore the bytes payloads that the capture decoded for readability."""
    if isinstance(value, dict) and isinstance(value.get("value"), str):
        return {**value, "value": value["value"].encode("utf-8")}
    return value


@pytest.fixture(scope="module")
def real_record() -> dict[str, Any]:
    record = json.loads(FIXTURE.read_text(encoding="utf-8"))
    record["aspect"] = _as_wire_bytes(record["aspect"])
    record["previousAspectValue"] = _as_wire_bytes(record["previousAspectValue"])
    return record


def test_real_envelope_projects(real_record: dict[str, Any]) -> None:
    event = from_mcl_record(real_record)
    assert event is not None
    assert event.entity_urn == CAPTURED_URN
    assert event.entity_type == "dataset"
    assert event.aspect_name == "schemaMetadata"
    assert event.change_type == "UPSERT"
    assert event.occurred_at.year == 2026


def test_real_envelope_carries_the_previous_aspect(real_record: dict[str, Any]) -> None:
    """The load-bearing assumption: no GMS round-trip is needed to diff."""
    event = from_mcl_record(real_record)
    assert event is not None
    assert event.previous_aspect is not None
    previous_columns = {f["fieldPath"] for f in event.previous_aspect["fields"]}
    assert DROPPED_COLUMN in previous_columns


def test_real_column_drop_is_detected_as_breaking(real_record: dict[str, Any]) -> None:
    event = from_mcl_record(real_record)
    assert event is not None
    diff = event.schema_field_diff()
    assert diff.removed == (DROPPED_COLUMN,)
    assert diff.added == ()
    assert diff.retyped == ()
    assert diff.is_breaking
    assert event.could_break_consumers


def test_real_envelope_has_null_headers(real_record: dict[str, Any]) -> None:
    """DataHub sends headers as null, not as an empty map."""
    assert real_record["headers"] is None
    event = from_mcl_record(real_record)
    assert event is not None
    assert event.headers == {}


def test_real_envelope_records_the_acting_system(real_record: dict[str, Any]) -> None:
    event = from_mcl_record(real_record)
    assert event is not None
    assert event.actor_urn is not None
    assert event.actor_urn.startswith("urn:li:corpuser:")
