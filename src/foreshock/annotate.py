"""Write blast-radius findings back into DataHub.

A warning that only exists in Foreshock's console is a warning nobody sees. The
people who need it are looking at the model page in DataHub, so the finding
belongs there.

Writes go through DataHub's MCP mutation tools, which are themselves gated
behind ``TOOLS_IS_MUTATION_ENABLED``. Tagging is used rather than description
rewriting because applying the same tag twice is a no-op, so a replayed event
cannot corrupt anything a human wrote.

There is no proposal or approval workflow in the OSS tool set — that is DataHub
Cloud's surface — so Foreshock writes directly and keeps the footprint to a
single, removable tag.
"""

from __future__ import annotations

from dataclasses import dataclass

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import TagPropertiesClass

from foreshock.blast_radius import BlastRadius
from foreshock.datahub_tools import DataHubTools

AT_RISK_TAG_URN = "urn:li:tag:foreshock_at_risk"
AT_RISK_TAG_NAME = "foreshock_at_risk"
AT_RISK_TAG_DESCRIPTION = (
    "Flagged by Foreshock: an upstream metadata change may have invalidated "
    "this asset. Check the change that triggered it before trusting downstream "
    "output."
)


@dataclass(frozen=True)
class AnnotationPlan:
    """What would be written, resolved before anything is."""

    tag_urn: str
    entity_urns: tuple[str, ...]
    column_targets: tuple[tuple[str, str], ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.entity_urns and not self.column_targets

    def describe(self) -> str:
        parts = [f"tag {self.tag_urn} ->"]
        if self.entity_urns:
            parts.append(f"{len(self.entity_urns)} entity(ies)")
        if self.column_targets:
            parts.append(f"{len(self.column_targets)} column(s)")
        return " ".join(parts)


def ensure_tag(emitter: DatahubRestEmitter) -> str:
    """Create the Foreshock tag if it is not already there.

    ``add_tags`` rejects a tag urn that does not resolve, and the OSS MCP tool
    set has no tag-creation tool, so this one write uses the SDK directly.
    Emitting the same properties again is an upsert, so it is safe to repeat.
    """
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=AT_RISK_TAG_URN,
            aspect=TagPropertiesClass(
                name=AT_RISK_TAG_NAME,
                description=AT_RISK_TAG_DESCRIPTION,
            ),
        )
    )
    return AT_RISK_TAG_URN


def plan_annotations(radius: BlastRadius) -> AnnotationPlan:
    """Decide what to mark, without writing. Pure.

    Only models and features are tagged. Intermediate tables are on the path but
    are not themselves the thing at risk, and tagging every hop would turn the
    estate into noise after a handful of changes.
    """
    if not radius.is_actionable:
        return AnnotationPlan(tag_urn=AT_RISK_TAG_URN, entity_urns=())

    entities = tuple(a.urn for a in radius.models) + tuple(
        a.urn for a in radius.features
    )
    columns = tuple(
        (affected.dataset_urn, affected.column) for affected in radius.affected_columns
    )
    return AnnotationPlan(
        tag_urn=AT_RISK_TAG_URN,
        entity_urns=entities,
        column_targets=columns,
    )


async def apply_annotations(tools: DataHubTools, plan: AnnotationPlan) -> int:
    """Apply a plan through the MCP mutation tools. Returns writes performed."""
    if plan.is_empty:
        return 0

    writes = 0
    if plan.entity_urns:
        await tools.add_tags([plan.tag_urn], list(plan.entity_urns))
        writes += 1
    if plan.column_targets:
        # add_tags pairs entity_urns with column_paths positionally.
        await tools.call(
            "add_tags",
            {
                "tag_urns": [plan.tag_urn],
                "entity_urns": [urn for urn, _ in plan.column_targets],
                "column_paths": [column for _, column in plan.column_targets],
            },
        )
        writes += 1
    return writes
