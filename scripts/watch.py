"""Watch DataHub's change stream and report ML blast radius as it happens.

Run this, then change a schema in another shell:

    python scripts/watch.py
    python -c "from datahub.emitter.rest_emitter import DatahubRestEmitter; \
               from foreshock.estate import drop_column; \
               drop_column(DatahubRestEmitter('http://127.0.0.1:8080'), \
                           'raw.transactions', 'device_fingerprint')"

Nothing polls. The warning appears because DataHub emitted the change and
Foreshock was subscribed to it.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from foreshock.blast_radius import BlastRadius, analyze
from foreshock.datahub_tools import ToolsConfig, open_tools
from foreshock.kafka_source import MclSource, SourceConfig

SEVERITY_LABEL = {
    "critical": "CRITICAL",
    "high": "HIGH",
    "medium": "MEDIUM",
    "none": "clear",
}


def render(radius: BlastRadius) -> str:
    lines = [
        "",
        "=" * 72,
        f"[{SEVERITY_LABEL.get(radius.severity, radius.severity)}] {radius.summary()}",
        "=" * 72,
        f"  changed by : {radius.event.actor_urn or 'unknown'}",
        f"  at         : {radius.event.occurred_at.isoformat()}",
        f"  aspect     : {radius.event.aspect_name} ({radius.event.change_type})",
    ]
    if radius.removed_columns:
        lines.append(f"  removed    : {', '.join(radius.removed_columns)}")
    if radius.retyped_columns:
        lines.append(f"  retyped    : {', '.join(radius.retyped_columns)}")

    if radius.models:
        lines.append("")
        lines.append("  models at risk:")
        for model in radius.models:
            lines.append(f"    - {model.name}  ({model.degree} hop(s) downstream)")
            if model.description:
                lines.append(f"        {model.description}")
    if radius.features:
        lines.append("")
        lines.append("  features affected:")
        for feature in radius.features:
            lines.append(f"    - {feature.name}  ({feature.degree} hop(s))")
    if radius.truncated:
        lines.append("")
        lines.append("  NOTE: lineage graph truncated; impact may be wider.")
    return "\n".join(lines)


async def run(args: argparse.Namespace) -> int:
    source = MclSource(
        SourceConfig(
            bootstrap_servers=args.bootstrap,
            schema_registry_url=args.schema_registry,
            group_id=args.group,
            auto_offset_reset=args.offset,
        )
    )
    print(f"watching {source.config.topic} (offset={args.offset})", flush=True)
    print("waiting for metadata changes; Ctrl-C to stop\n", flush=True)

    seen = 0
    try:
        async with open_tools(ToolsConfig(gms_url=args.gms)) as tools:
            while True:
                event = source.poll(1.0)
                if event is None:
                    await asyncio.sleep(0)
                    continue
                seen += 1
                radius = await analyze(tools, event)
                if radius is None:
                    if args.verbose:
                        print(
                            f"  . {event.aspect_name} on {event.entity_urn} "
                            "-> not breaking, skipped",
                            flush=True,
                        )
                    continue
                print(render(radius), flush=True)
                source.commit()
                if args.once and radius.is_actionable:
                    return 0
    except KeyboardInterrupt:
        print(f"\nstopped after {seen} event(s)")
        return 0
    finally:
        source.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gms", default="http://127.0.0.1:8080")
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument(
        "--schema-registry", default="http://127.0.0.1:8080/schema-registry/api/"
    )
    parser.add_argument("--group", default="foreshock-watch")
    parser.add_argument("--offset", default="latest", choices=["earliest", "latest"])
    parser.add_argument("--once", action="store_true", help="Exit after one finding.")
    parser.add_argument("--verbose", action="store_true", help="Show skipped events.")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
