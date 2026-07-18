"""Typed projection of a DataHub Metadata Change Log (MCL) record.

DataHub emits an MCL event to Kafka every time an entity's aspect changes. The
envelope is defined by ``com.linkedin.pegasus2avro.mxe.MetadataChangeLog`` and
carries both the new aspect value and the previous one, which means a change can
be diffed at consume time without calling back into GMS.

This module owns the boundary between that raw Avro record and the typed event
the rest of Foreshock subscribes to. Nothing downstream should touch raw MCL
fields directly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

# Aspect names that can invalidate something downstream. Used as a cheap
# pre-filter so the blast-radius walk only runs on changes that can actually
# break a consumer, rather than on every metadata write.
BREAKING_ASPECT_NAMES: frozenset[str] = frozenset(
    {
        "schemaMetadata",
        "editableSchemaMetadata",
        "status",
        "deprecation",
        "datasetProperties",
    }
)

# ChangeType values that represent a removal rather than an edit.
REMOVAL_CHANGE_TYPES: frozenset[str] = frozenset({"DELETE"})


@dataclass(frozen=True)
class SchemaFieldDiff:
    """Column-level delta between two schemaMetadata aspect values."""

    removed: tuple[str, ...] = ()
    added: tuple[str, ...] = ()
    retyped: tuple[tuple[str, str, str], ...] = ()

    @property
    def is_empty(self) -> bool:
        return not (self.removed or self.added or self.retyped)

    @property
    def is_breaking(self) -> bool:
        """Removals and type changes can break a consumer; additions cannot."""
        return bool(self.removed or self.retyped)


@dataclass(frozen=True)
class MclEvent:
    """One decoded metadata change, ready for a subscriber to reason about."""

    entity_urn: str
    entity_type: str
    aspect_name: str
    change_type: str
    occurred_at: datetime
    actor_urn: str | None = None
    aspect: Mapping[str, Any] | None = None
    previous_aspect: Mapping[str, Any] | None = None
    headers: Mapping[str, str] = field(default_factory=dict)

    @property
    def is_removal(self) -> bool:
        return self.change_type in REMOVAL_CHANGE_TYPES

    @property
    def could_break_consumers(self) -> bool:
        """Whether this change is worth spending a lineage walk on."""
        if self.aspect_name not in BREAKING_ASPECT_NAMES:
            return False
        if self.is_removal:
            return True
        return self.schema_field_diff().is_breaking

    def schema_field_diff(self) -> SchemaFieldDiff:
        """Diff the column set across the aspect boundary.

        Returns an empty diff for non-schema aspects, or when either side of the
        change is absent (a create has no previous value, a delete has no new
        one) — callers distinguish those cases via ``is_removal``.
        """
        if self.aspect_name not in ("schemaMetadata", "editableSchemaMetadata"):
            return SchemaFieldDiff()
        if self.aspect is None or self.previous_aspect is None:
            return SchemaFieldDiff()

        before = _fields_by_path(self.previous_aspect)
        after = _fields_by_path(self.aspect)

        removed = tuple(sorted(before.keys() - after.keys()))
        added = tuple(sorted(after.keys() - before.keys()))
        retyped = tuple(
            sorted(
                (path, before[path], after[path])
                for path in before.keys() & after.keys()
                if before[path] != after[path]
            )
        )
        return SchemaFieldDiff(removed=removed, added=added, retyped=retyped)


def _fields_by_path(aspect: Mapping[str, Any]) -> dict[str, str]:
    """Map fieldPath -> native type string for a schemaMetadata aspect value."""
    out: dict[str, str] = {}
    for entry in aspect.get("fields") or ():
        if not isinstance(entry, Mapping):
            continue
        path = entry.get("fieldPath")
        if not isinstance(path, str):
            continue
        out[path] = str(entry.get("nativeDataType") or "")
    return out


def _decode_aspect(raw: Any) -> Mapping[str, Any] | None:
    """Decode a GenericAspect payload into a mapping.

    GenericAspect wraps the real value in a ``value`` byte string alongside a
    ``contentType``. In practice DataHub writes JSON there.
    """
    if raw is None:
        return None
    if isinstance(raw, Mapping) and "value" not in raw:
        return raw

    payload = raw.get("value") if isinstance(raw, Mapping) else raw
    if payload is None:
        return None
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            return None
        return decoded if isinstance(decoded, Mapping) else None
    return payload if isinstance(payload, Mapping) else None


def _decode_timestamp(created: Any) -> datetime:
    """Read the AuditStamp time, falling back to now when absent."""
    if isinstance(created, Mapping):
        millis = created.get("time")
        if isinstance(millis, (int, float)):
            return datetime.fromtimestamp(millis / 1000, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def _decode_actor(created: Any) -> str | None:
    if isinstance(created, Mapping):
        actor = created.get("actor")
        if isinstance(actor, str):
            return actor
    return None


def from_mcl_record(record: Mapping[str, Any]) -> MclEvent | None:
    """Project a raw MCL Avro record into an ``MclEvent``.

    Returns ``None`` for records that carry no entity urn or aspect name, which
    the envelope permits but which no subscriber can act on.
    """
    entity_urn = record.get("entityUrn")
    aspect_name = record.get("aspectName")
    if not isinstance(entity_urn, str) or not isinstance(aspect_name, str):
        return None

    created = record.get("created")
    raw_headers = record.get("headers")

    return MclEvent(
        entity_urn=entity_urn,
        entity_type=str(record.get("entityType") or ""),
        aspect_name=aspect_name,
        change_type=str(record.get("changeType") or ""),
        occurred_at=_decode_timestamp(created),
        actor_urn=_decode_actor(created),
        aspect=_decode_aspect(record.get("aspect")),
        previous_aspect=_decode_aspect(record.get("previousAspectValue")),
        headers={
            str(k): str(v) for k, v in (raw_headers or {}).items()
        }
        if isinstance(raw_headers, Mapping)
        else {},
    )
