"""Behaviour when DataHub is slow, partly broken, or lying.

A judge running this on unknown hardware is the realistic worst case, so the
question each test asks is: does the agent still say something useful, and does
it say so honestly?
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from foreshock.blast_radius import analyze
from foreshock.mcl_event import from_mcl_record

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,raw.transactions,PROD)"


def _aspect(fields: list[str]) -> dict[str, Any]:
    return {
        "value": json.dumps(
            {"fields": [{"fieldPath": f, "nativeDataType": "VARCHAR"} for f in fields]}
        ),
        "contentType": "application/json",
    }


def _event() -> Any:
    return from_mcl_record(
        {
            "entityUrn": DATASET_URN,
            "entityType": "dataset",
            "aspectName": "schemaMetadata",
            "changeType": "UPSERT",
            "created": {"time": 1784500000000, "actor": "urn:li:corpuser:alice"},
            "previousAspectValue": _aspect(["a", "device_fingerprint"]),
            "aspect": _aspect(["a"]),
        }
    )


TABLE_LINEAGE = {
    "downstreams": {
        "total": 1,
        "searchResults": [
            {
                "entity": {
                    "urn": "urn:li:mlModel:fraud",
                    "type": "MLMODEL",
                    "name": "fraud_detector",
                },
                "degree": 2,
            }
        ],
    }
}


class _StubTools:
    """Stands in for DataHubTools with scripted failures."""

    def __init__(self, *, column_error: Exception | None = None,
                 table_error: Exception | None = None) -> None:
        self.column_error = column_error
        self.table_error = table_error
        self.calls: list[str | None] = []

    async def downstream_lineage(self, urn: str, *, column: str | None = None, **_: Any) -> Any:
        self.calls.append(column)
        if column is None:
            if self.table_error:
                raise self.table_error
            return TABLE_LINEAGE
        if self.column_error:
            raise self.column_error
        return {"downstreams": {"searchResults": []}}


@pytest.mark.asyncio
async def test_column_walk_timeout_degrades_to_table_scope() -> None:
    """Losing precision beats losing the warning."""
    tools = _StubTools(column_error=TimeoutError("get_lineage timed out"))
    radius = await analyze(tools, _event())  # type: ignore[arg-type]

    assert radius is not None
    assert [m.name for m in radius.models] == ["fraud_detector"]
    # And it must not claim a precision it did not achieve.
    assert radius.column_precise is False


@pytest.mark.asyncio
async def test_column_walk_error_degrades_to_table_scope() -> None:
    tools = _StubTools(column_error=RuntimeError("get_lineage failed: boom"))
    radius = await analyze(tools, _event())  # type: ignore[arg-type]

    assert radius is not None
    assert radius.column_precise is False
    assert radius.is_actionable


@pytest.mark.asyncio
async def test_table_walk_failure_propagates_to_the_caller() -> None:
    """The caller must be able to skip the commit, so this one must not be swallowed."""
    tools = _StubTools(table_error=TimeoutError("get_lineage timed out"))

    with pytest.raises(TimeoutError):
        await analyze(tools, _event())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_non_breaking_change_never_touches_datahub() -> None:
    """Cost must scale with risk, not with total metadata write volume."""
    tools = _StubTools()
    event = from_mcl_record(
        {
            "entityUrn": DATASET_URN,
            "entityType": "dataset",
            "aspectName": "globalTags",
            "changeType": "UPSERT",
            "created": {"time": 1784500000000, "actor": "urn:li:corpuser:alice"},
        }
    )
    assert await analyze(tools, event) is None  # type: ignore[arg-type]
    assert tools.calls == []
