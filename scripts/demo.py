"""The whole argument, in one command.

Runs two scenarios back to back against a live DataHub, both of them real
changes producing real Kafka events:

  1. Drop ``raw.transactions.device_fingerprint`` -> the fraud model is flagged,
     the churn model is not.
  2. Drop ``raw.transactions.amount``            -> both models are flagged.

The second is the control. It is the reason the first result is precision
rather than a walk that quietly stopped early: ``amount`` genuinely does feed
the churn model's ``lifetime_value``, so a correct analysis must reach it.

Usage:
    python scripts/demo.py [--annotate] [--gms http://127.0.0.1:8080]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

from foreshock.annotate import apply_annotations, ensure_tag, plan_annotations
from foreshock.blast_radius import BlastRadius, analyze
from foreshock.datahub_tools import DataHubTools, ToolsConfig, open_tools
from foreshock.estate import (
    drop_column,
    schema_metadata,
    seed_estate,
    table_by_name,
)
from foreshock.kafka_source import MclSource, SourceConfig

TABLE = "raw.transactions"
SCENARIOS = (
    (
        "device_fingerprint",
        "A column a data engineer would read as harmless.",
        "fraud_detector only",
    ),
    (
        "amount",
        "A column that genuinely feeds both models.",
        "fraud_detector AND churn_predictor",
    ),
)
EVENT_TIMEOUT_SECONDS = 90.0


def _rule(char: str = "=") -> str:
    return char * 74


def _restore(emitter: DatahubRestEmitter) -> None:
    """Put the full schema back so the demo can be run again."""
    table = table_by_name(TABLE)
    emitter.emit(
        MetadataChangeProposalWrapper(
            entityUrn=table.urn, aspect=schema_metadata(table)
        )
    )


async def _await_breaking_event(source: MclSource, urn: str) -> BlastRadius | None:
    """Poll until the change we just made shows up, or give up.

    The budget is wall-clock, deliberately. Counting poll calls instead looks
    equivalent but is not: ``poll`` returns immediately when a message is
    already buffered, so a backlog — seeding the estate produces a sizeable one
    — burns the whole budget in milliseconds without any time passing.
    """
    deadline = time.monotonic() + EVENT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        event = source.poll(1.0)
        if event is None:
            await asyncio.sleep(0)
            continue
        if event.entity_urn != urn or event.aspect_name != "schemaMetadata":
            continue
        if not event.could_break_consumers:
            continue
        return event  # type: ignore[return-value]
    return None


def _report(radius: BlastRadius, expected: str) -> None:
    print()
    print(_rule())
    print(f"  {radius.summary()}")
    print(_rule())
    print(f"  severity        : {radius.severity.upper()}")
    print(f"  column-precise  : {radius.column_precise}")
    print(f"  expected        : {expected}")
    models = ", ".join(m.name for m in radius.models) or "none"
    print(f"  models flagged  : {models}")
    if radius.features:
        print(f"  features        : {', '.join(f.name for f in radius.features)}")
    for affected in radius.affected_columns:
        print(
            f"  column path     : {affected.source_column} -> "
            f"{affected.dataset_name}.{affected.column}"
        )


async def _run_scenario(
    tools: DataHubTools,
    source: MclSource,
    emitter: DatahubRestEmitter,
    column: str,
    framing: str,
    expected: str,
    annotate: bool,
) -> bool:
    print()
    print(_rule("-"))
    print(f"SCENARIO: drop {TABLE}.{column}")
    print(f"  {framing}")
    print(_rule("-"))

    source.assign_from_end()
    urn = drop_column(emitter, TABLE, column)
    print(f"  emitted the change; waiting for DataHub to publish it...")

    event = await _await_breaking_event(source, urn)
    if event is None:
        print("  NO EVENT OBSERVED within the timeout.", file=sys.stderr)
        return False

    radius = await analyze(tools, event)
    if radius is None:
        print("  event was not classified as breaking.", file=sys.stderr)
        return False

    _report(radius, expected)

    plan = plan_annotations(radius)
    if not plan.is_empty:
        if annotate:
            writes = await apply_annotations(tools, plan)
            print(f"  wrote back      : {plan.describe()} ({writes} call(s))")
        else:
            print(f"  would write back: {plan.describe()}  (pass --annotate)")

    _restore(emitter)
    return True


async def run(args: argparse.Namespace) -> int:
    emitter = DatahubRestEmitter(gms_server=args.gms)
    emitter.test_connection()

    print("seeding the ML estate (idempotent)...")
    seed_estate(emitter)
    _restore(emitter)
    if args.annotate:
        ensure_tag(emitter)

    source = MclSource(
        SourceConfig(
            bootstrap_servers=args.bootstrap,
            schema_registry_url=args.schema_registry,
            group_id="foreshock-demo",
        ),
        subscribe=False,
    )

    ok = True
    try:
        async with open_tools(
            ToolsConfig(gms_url=args.gms, enable_mutations=args.annotate)
        ) as tools:
            for column, framing, expected in SCENARIOS:
                ok &= await _run_scenario(
                    tools, source, emitter, column, framing, expected, args.annotate
                )
    finally:
        source.close()

    print()
    print(_rule())
    if ok:
        print("  Same table, two columns, two different answers.")
        print("  The blast radius follows the column, not the table.")
    else:
        print("  Demo did not complete; see errors above.")
    print(_rule())
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gms", default="http://127.0.0.1:8080")
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument(
        "--schema-registry", default="http://127.0.0.1:8080/schema-registry/api/"
    )
    parser.add_argument(
        "--annotate", action="store_true", help="Also write findings back to DataHub."
    )
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
