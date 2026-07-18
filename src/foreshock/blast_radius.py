"""Turn a breaking metadata change into the list of ML assets it endangers.

The reason this is worth automating is distance. A column drop is reviewed by
someone looking at one table; the model that column feeds is two or three hops
away, owned by another team, and invisible from the change itself. By the time
the damage shows up it looks like a model regression, not a schema edit.

Assessment is a pure function of the change plus a lineage payload, so the
ranking logic is testable without a broker or a DataHub instance. Only
:func:`analyze` performs I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from foreshock.datahub_tools import DataHubTools
from foreshock.mcl_event import MclEvent

MODEL_TYPE = "MLMODEL"
FEATURE_TYPE = "MLFEATURE"
DATASET_TYPE = "DATASET"

# Ordered worst-first; the first matching rule wins.
SEVERITY_CRITICAL = "critical"
SEVERITY_HIGH = "high"
SEVERITY_MEDIUM = "medium"
SEVERITY_NONE = "none"


@dataclass(frozen=True)
class ImpactedAsset:
    """One entity downstream of the change."""

    urn: str
    entity_type: str
    name: str
    degree: int
    description: str | None = None

    @property
    def is_model(self) -> bool:
        return self.entity_type == MODEL_TYPE

    @property
    def is_feature(self) -> bool:
        return self.entity_type == FEATURE_TYPE


@dataclass(frozen=True)
class BlastRadius:
    """What one change puts at risk."""

    event: MclEvent
    removed_columns: tuple[str, ...]
    retyped_columns: tuple[str, ...]
    impacted: tuple[ImpactedAsset, ...]
    truncated: bool = False

    @property
    def models(self) -> tuple[ImpactedAsset, ...]:
        return tuple(a for a in self.impacted if a.is_model)

    @property
    def features(self) -> tuple[ImpactedAsset, ...]:
        return tuple(a for a in self.impacted if a.is_feature)

    @property
    def severity(self) -> str:
        """Severity is driven by how far the damage reaches, not by hop count.

        A change that reaches a served model is the case Foreshock exists for;
        one that stops at a table is a normal downstream-dependency problem that
        existing tooling already surfaces.
        """
        if not self.impacted:
            return SEVERITY_NONE
        if self.models:
            return SEVERITY_CRITICAL
        if self.features:
            return SEVERITY_HIGH
        return SEVERITY_MEDIUM

    @property
    def is_actionable(self) -> bool:
        return self.severity in (SEVERITY_CRITICAL, SEVERITY_HIGH)

    def summary(self) -> str:
        """One line an on-call engineer can read without opening anything."""
        change = _describe_change(self.removed_columns, self.retyped_columns)
        if not self.impacted:
            return f"{change} on {_short(self.event.entity_urn)}; nothing downstream."
        model_names = ", ".join(a.name for a in self.models) or "no models"
        return (
            f"{change} on {_short(self.event.entity_urn)} reaches "
            f"{len(self.impacted)} downstream asset(s); models at risk: {model_names}."
        )


def _short(urn: str) -> str:
    """Trim a urn to the readable name inside it."""
    if "," in urn and urn.endswith(")"):
        parts = urn.rstrip(")").split(",")
        if len(parts) >= 2:
            return parts[-2] if parts[-1] in ("PROD", "DEV", "QA") else parts[-1]
    return urn


def _describe_change(removed: tuple[str, ...], retyped: tuple[str, ...]) -> str:
    pieces = []
    if removed:
        pieces.append(f"dropped {', '.join(removed)}")
    if retyped:
        pieces.append(f"retyped {', '.join(retyped)}")
    return "; ".join(pieces) or "breaking change"


def impacts_from_lineage(payload: Mapping[str, Any]) -> tuple[tuple[ImpactedAsset, ...], bool]:
    """Read the MCP ``get_lineage`` payload into ranked assets.

    Returns the assets plus whether the graph was truncated, because a partial
    walk that happens to find no models must not be reported as "safe".
    """
    downstreams = payload.get("downstreams")
    if not isinstance(downstreams, Mapping):
        return (), False

    assets: list[ImpactedAsset] = []
    for result in downstreams.get("searchResults") or ():
        if not isinstance(result, Mapping):
            continue
        entity = result.get("entity")
        if not isinstance(entity, Mapping):
            continue
        urn = entity.get("urn")
        if not isinstance(urn, str):
            continue
        properties = entity.get("properties")
        description = (
            properties.get("description") if isinstance(properties, Mapping) else None
        )
        assets.append(
            ImpactedAsset(
                urn=urn,
                entity_type=str(entity.get("type") or ""),
                name=str(entity.get("name") or _short(urn)),
                degree=int(result.get("degree") or 0),
                description=description,
            )
        )

    # Models first, then features, then everything else; nearest hop first
    # within a tier, since a closer dependency is the likelier true break.
    tier = {MODEL_TYPE: 0, FEATURE_TYPE: 1}
    assets.sort(key=lambda a: (tier.get(a.entity_type, 2), a.degree, a.name))
    return tuple(assets), bool(downstreams.get("hasMore"))


def assess(event: MclEvent, lineage: Mapping[str, Any]) -> BlastRadius:
    """Combine a change with its downstream graph. Pure; no I/O."""
    diff = event.schema_field_diff()
    assets, truncated = impacts_from_lineage(lineage)
    return BlastRadius(
        event=event,
        removed_columns=diff.removed,
        retyped_columns=tuple(path for path, _, _ in diff.retyped),
        impacted=assets,
        truncated=truncated,
    )


async def analyze(tools: DataHubTools, event: MclEvent) -> BlastRadius | None:
    """Walk lineage for a change, or return ``None`` if it cannot break anything.

    The pre-filter matters at estate scale: most metadata traffic is ownership
    and tag edits, and walking the graph for those would make the agent cost
    scale with total write volume rather than with risk.
    """
    if not event.could_break_consumers:
        return None
    lineage = await tools.downstream_lineage(event.entity_urn)
    if not isinstance(lineage, Mapping):
        return None
    return assess(event, lineage)
