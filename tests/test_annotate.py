"""Tests for write-back planning.

Planning is pure, so what gets written is decided and assertable before any
mutation reaches DataHub. The apply step is a thin wrapper over MCP calls and is
covered by the end-to-end run rather than mocked here.
"""

from __future__ import annotations

import json
from typing import Any

from foreshock.annotate import AT_RISK_TAG_URN, plan_annotations
from foreshock.blast_radius import AffectedColumn, BlastRadius, ImpactedAsset
from foreshock.mcl_event import from_mcl_record

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)"
DERIVED_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,features.txn,PROD)"


def _event() -> Any:
    aspect = lambda fields: {  # noqa: E731
        "value": json.dumps(
            {"fields": [{"fieldPath": f, "nativeDataType": "VARCHAR"} for f in fields]}
        ),
        "contentType": "application/json",
    }
    return from_mcl_record(
        {
            "entityUrn": DATASET_URN,
            "entityType": "dataset",
            "aspectName": "schemaMetadata",
            "changeType": "UPSERT",
            "created": {"time": 1784500000000, "actor": "urn:li:corpuser:alice"},
            "previousAspectValue": aspect(["a", "device_fingerprint"]),
            "aspect": aspect(["a"]),
        }
    )


def _radius(**overrides: Any) -> BlastRadius:
    base: dict[str, Any] = {
        "event": _event(),
        "removed_columns": ("device_fingerprint",),
        "retyped_columns": (),
        "impacted": (
            ImpactedAsset("urn:model:fraud", "MLMODEL", "fraud_detector", 2),
            ImpactedAsset("urn:feature:dfe", "MLFEATURE", "device_fingerprint_entropy", 1),
            ImpactedAsset(DERIVED_URN, "DATASET", "features.txn", 1),
        ),
        "affected_columns": (
            AffectedColumn(
                source_column="device_fingerprint",
                dataset_urn=DERIVED_URN,
                dataset_name="features.txn",
                column="device_fingerprint_entropy",
            ),
        ),
        "column_precise": True,
    }
    base.update(overrides)
    return BlastRadius(**base)


def test_models_and_features_are_tagged() -> None:
    plan = plan_annotations(_radius())
    assert plan.tag_urn == AT_RISK_TAG_URN
    assert "urn:model:fraud" in plan.entity_urns
    assert "urn:feature:dfe" in plan.entity_urns


def test_intermediate_tables_are_not_tagged() -> None:
    """Tagging every hop would turn the estate into noise within a few changes."""
    plan = plan_annotations(_radius())
    assert DERIVED_URN not in plan.entity_urns


def test_affected_columns_are_tagged_at_column_level() -> None:
    plan = plan_annotations(_radius())
    assert plan.column_targets == ((DERIVED_URN, "device_fingerprint_entropy"),)


def test_non_actionable_radius_writes_nothing() -> None:
    """A table-only impact must not put warning tags on the estate."""
    plan = plan_annotations(
        _radius(
            impacted=(ImpactedAsset(DERIVED_URN, "DATASET", "features.txn", 1),),
            affected_columns=(),
        )
    )
    assert plan.is_empty


def test_empty_radius_writes_nothing() -> None:
    plan = plan_annotations(_radius(impacted=(), affected_columns=()))
    assert plan.is_empty


def test_plan_is_deterministic_so_replays_are_idempotent() -> None:
    """The same event must resolve to the same writes, so a replay is a no-op."""
    first = plan_annotations(_radius())
    second = plan_annotations(_radius())
    assert first == second


def test_describe_mentions_what_would_be_written() -> None:
    described = plan_annotations(_radius()).describe()
    assert AT_RISK_TAG_URN in described
    assert "entity" in described
