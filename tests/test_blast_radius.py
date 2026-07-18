"""Tests for blast-radius assessment.

Lineage payloads here mirror the shape the DataHub MCP ``get_lineage`` tool
returns, observed against DataHub Core v1.5.0.6. Assessment is pure, so none of
this needs a broker or a running instance.
"""

from __future__ import annotations

import json
from typing import Any

from foreshock.blast_radius import assess, impacts_from_lineage
from foreshock.mcl_event import from_mcl_record

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)"


def _aspect(fields: list[str]) -> dict[str, Any]:
    return {
        "value": json.dumps(
            {"fields": [{"fieldPath": f, "nativeDataType": "VARCHAR"} for f in fields]}
        ),
        "contentType": "application/json",
    }


def _event(before: list[str], after: list[str]) -> Any:
    return from_mcl_record(
        {
            "entityUrn": DATASET_URN,
            "entityType": "dataset",
            "aspectName": "schemaMetadata",
            "changeType": "UPSERT",
            "created": {"time": 1784500000000, "actor": "urn:li:corpuser:alice"},
            "previousAspectValue": _aspect(before),
            "aspect": _aspect(after),
        }
    )


def _lineage(entries: list[tuple[str, str, int]], has_more: bool = False) -> dict[str, Any]:
    return {
        "downstreams": {
            "total": len(entries),
            "hasMore": has_more,
            "searchResults": [
                {
                    "entity": {
                        "urn": f"urn:li:{kind.lower()}:{name}",
                        "type": kind,
                        "name": name,
                        "properties": {"name": name, "description": f"{name} desc"},
                    },
                    "degree": degree,
                }
                for kind, name, degree in entries
            ],
        }
    }


def test_reaching_a_model_is_critical() -> None:
    event = _event(["a", "device_fingerprint"], ["a"])
    radius = assess(
        event,
        _lineage([("DATASET", "features.txn", 1), ("MLMODEL", "fraud_detector", 2)]),
    )
    assert radius.severity == "critical"
    assert radius.removed_columns == ("device_fingerprint",)
    assert [m.name for m in radius.models] == ["fraud_detector"]
    assert radius.is_actionable


def test_reaching_only_features_is_high() -> None:
    radius = assess(
        _event(["a", "b"], ["a"]),
        _lineage([("MLFEATURE", "amount_zscore", 2)]),
    )
    assert radius.severity == "high"
    assert radius.is_actionable


def test_reaching_only_tables_is_medium() -> None:
    """A table-only impact is what existing tooling already surfaces."""
    radius = assess(_event(["a", "b"], ["a"]), _lineage([("DATASET", "features.txn", 1)]))
    assert radius.severity == "medium"
    assert not radius.is_actionable


def test_no_downstream_is_not_actionable() -> None:
    radius = assess(_event(["a", "b"], ["a"]), _lineage([]))
    assert radius.severity == "none"
    assert not radius.is_actionable
    assert "nothing downstream" in radius.summary()


def test_models_are_ranked_before_features_and_nearest_first() -> None:
    radius = assess(
        _event(["a", "b"], ["a"]),
        _lineage(
            [
                ("MLFEATURE", "far_feature", 3),
                ("MLMODEL", "far_model", 3),
                ("MLMODEL", "near_model", 1),
                ("DATASET", "some_table", 1),
            ]
        ),
    )
    assert [a.name for a in radius.impacted][:3] == [
        "near_model",
        "far_model",
        "far_feature",
    ]


def test_truncated_graph_is_flagged() -> None:
    """A partial walk that finds no models must not read as safe."""
    radius = assess(_event(["a", "b"], ["a"]), _lineage([], has_more=True))
    assert radius.truncated


def test_malformed_lineage_payload_yields_nothing() -> None:
    assets, truncated = impacts_from_lineage({"unexpected": True})
    assert assets == ()
    assert not truncated
    assets, _ = impacts_from_lineage(
        {"downstreams": {"searchResults": [{"entity": "not-a-mapping"}, {}]}}
    )
    assert assets == ()


def test_summary_names_the_change_and_the_models() -> None:
    radius = assess(
        _event(["a", "device_fingerprint"], ["a"]),
        _lineage([("MLMODEL", "fraud_detector", 2)]),
    )
    summary = radius.summary()
    assert "device_fingerprint" in summary
    assert "fraud_detector" in summary
