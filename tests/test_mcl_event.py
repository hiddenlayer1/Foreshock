"""Contract tests for the MCL envelope projection.

Records here are shaped after the bundled Avro schema
``datahub/metadata/schemas/MetadataChangeLog.avsc`` in acryl-datahub 1.6.0.15.
They lock the behaviour the blast-radius agent depends on: that a breaking
column change is detectable from the event alone, and that everything else is
cheap to ignore.
"""

from __future__ import annotations

import json
from typing import Any

from foreshock.mcl_event import from_mcl_record

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,demo.orders,PROD)"


def _aspect(fields: list[dict[str, str]]) -> dict[str, Any]:
    """Wrap a schemaMetadata value the way GenericAspect carries it."""
    return {
        "value": json.dumps({"fields": fields}),
        "contentType": "application/json",
    }


def _record(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "entityUrn": DATASET_URN,
        "entityType": "dataset",
        "aspectName": "schemaMetadata",
        "changeType": "UPSERT",
        "created": {"time": 1784500000000, "actor": "urn:li:corpuser:alice"},
        "previousAspectValue": _aspect(
            [
                {"fieldPath": "order_id", "nativeDataType": "NUMBER"},
                {"fieldPath": "customer_email", "nativeDataType": "VARCHAR"},
            ]
        ),
        "aspect": _aspect([{"fieldPath": "order_id", "nativeDataType": "NUMBER"}]),
    }
    base.update(overrides)
    return base


def test_projects_core_envelope_fields() -> None:
    event = from_mcl_record(_record())
    assert event is not None
    assert event.entity_urn == DATASET_URN
    assert event.entity_type == "dataset"
    assert event.aspect_name == "schemaMetadata"
    assert event.actor_urn == "urn:li:corpuser:alice"
    assert event.occurred_at.year == 2026


def test_dropped_column_is_breaking() -> None:
    event = from_mcl_record(_record())
    assert event is not None
    diff = event.schema_field_diff()
    assert diff.removed == ("customer_email",)
    assert diff.added == ()
    assert diff.is_breaking
    assert event.could_break_consumers


def test_added_column_alone_is_not_breaking() -> None:
    """Additive schema evolution must not spend a lineage walk."""
    event = from_mcl_record(
        _record(
            aspect=_aspect(
                [
                    {"fieldPath": "order_id", "nativeDataType": "NUMBER"},
                    {"fieldPath": "customer_email", "nativeDataType": "VARCHAR"},
                    {"fieldPath": "currency", "nativeDataType": "VARCHAR"},
                ]
            )
        )
    )
    assert event is not None
    diff = event.schema_field_diff()
    assert diff.added == ("currency",)
    assert diff.removed == ()
    assert not diff.is_breaking
    assert not event.could_break_consumers


def test_retyped_column_is_breaking() -> None:
    event = from_mcl_record(
        _record(
            aspect=_aspect(
                [
                    {"fieldPath": "order_id", "nativeDataType": "NUMBER"},
                    {"fieldPath": "customer_email", "nativeDataType": "NUMBER"},
                ]
            )
        )
    )
    assert event is not None
    diff = event.schema_field_diff()
    assert diff.retyped == (("customer_email", "VARCHAR", "NUMBER"),)
    assert diff.is_breaking


def test_unrelated_aspect_is_ignored_without_diffing() -> None:
    event = from_mcl_record(
        _record(aspectName="globalTags", aspect=None, previousAspectValue=None)
    )
    assert event is not None
    assert not event.could_break_consumers
    assert event.schema_field_diff().is_empty


def test_delete_change_type_is_breaking_even_without_a_diff() -> None:
    event = from_mcl_record(
        _record(changeType="DELETE", aspect=None, previousAspectValue=None)
    )
    assert event is not None
    assert event.is_removal
    assert event.could_break_consumers


def test_creation_has_no_previous_value_and_does_not_crash() -> None:
    event = from_mcl_record(_record(previousAspectValue=None))
    assert event is not None
    assert event.schema_field_diff().is_empty
    assert not event.could_break_consumers


def test_record_without_urn_or_aspect_name_is_dropped() -> None:
    assert from_mcl_record({"entityType": "dataset"}) is None
    assert from_mcl_record(_record(entityUrn=None)) is None
    assert from_mcl_record(_record(aspectName=None)) is None


def test_malformed_aspect_payload_does_not_crash() -> None:
    event = from_mcl_record(
        _record(aspect={"value": "not-json{", "contentType": "application/json"})
    )
    assert event is not None
    assert event.aspect is None
    assert event.schema_field_diff().is_empty


def test_missing_audit_stamp_falls_back_to_now() -> None:
    event = from_mcl_record(_record(created=None))
    assert event is not None
    assert event.actor_urn is None
    assert event.occurred_at is not None
