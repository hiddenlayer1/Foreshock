"""Ask DataHub what Foreshock wrote, rather than trusting that it wrote it.

The write-back claim is the one a reader has most reason to doubt: an agent
reporting its own success proves nothing. So this asks the metadata store
directly and prints what it says, including the negative case.

The negative case is the point. Anyone can show a tag they just applied. The
result worth showing is that ``churn_predictor`` — the model a correct analysis
must leave alone — is still clean, because that is what makes the flagged model
mean something.

Reads only, and needs no credential: the quickstart runs GMS with
METADATA_SERVICE_AUTH_ENABLED=false.

Usage:
    python scripts/verify_writeback.py [--gms http://127.0.0.1:8080]
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

from foreshock.annotate import AT_RISK_TAG_URN
from foreshock.estate import FEATURES, MODELS, TABLES

RULE_WIDTH = 74
REQUEST_TIMEOUT_SECONDS = 15.0


def _rule(char: str = "=") -> str:
    return char * RULE_WIDTH


def _aspect(gms: str, urn: str, aspect: str) -> dict | None:
    """Fetch one aspect, or None when the entity carries no such aspect."""
    quoted = urllib.parse.quote(urn, safe="")
    url = f"{gms.rstrip('/')}/aspects/{quoted}?aspect={aspect}&version=0"
    try:
        with urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            # GMS answers with a UTF-8 BOM, which json.loads rejects outright.
            return json.loads(response.read().decode("utf-8-sig"))
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def entity_tags(gms: str, urn: str) -> list[str]:
    payload = _aspect(gms, urn, "globalTags")
    if payload is None:
        return []
    body = payload.get("aspect", {}).get("com.linkedin.common.GlobalTags", {})
    return [tag["tag"] for tag in body.get("tags", [])]


def column_tags(gms: str, dataset_urn: str) -> dict[str, list[str]]:
    """Column-level tags live on the dataset, not on the schemaField entity."""
    payload = _aspect(gms, dataset_urn, "editableSchemaMetadata")
    if payload is None:
        return {}
    body = payload.get("aspect", {}).get(
        "com.linkedin.schema.EditableSchemaMetadata", {}
    )
    found: dict[str, list[str]] = {}
    for field_info in body.get("editableSchemaFieldInfo", []):
        tags = [t["tag"] for t in (field_info.get("globalTags") or {}).get("tags", [])]
        if tags:
            found[field_info["fieldPath"]] = tags
    return found


def _verdict(tags: list[str]) -> str:
    return "AT RISK" if AT_RISK_TAG_URN in tags else "clean"


def run(gms: str) -> int:
    print()
    print(_rule())
    print("  Asking DataHub what Foreshock wrote")
    print(_rule())

    flagged = 0
    clean = 0

    print("  models")
    for model in MODELS:
        verdict = _verdict(entity_tags(gms, model.urn))
        flagged += verdict == "AT RISK"
        clean += verdict == "clean"
        print(f"    {model.name:<34} {verdict}")

    print("  features")
    for feature in FEATURES:
        verdict = _verdict(entity_tags(gms, feature.urn))
        flagged += verdict == "AT RISK"
        clean += verdict == "clean"
        print(f"    {feature.name:<34} {verdict}")

    print("  columns")
    any_column = False
    for table in TABLES:
        for column, tags in column_tags(gms, table.urn).items():
            any_column = True
            # Column first, dataset underneath: the fully-qualified path is
            # wider than the name field, and padding it would push the verdict
            # out of the column the rows above established.
            print(f"    {column:<34} {_verdict(tags)}")
            print(f"      in {table.name}")
    if not any_column:
        print("    (none tagged)")

    print(_rule())
    print(f"  {flagged} flagged, {clean} left clean.")
    print("  The clean ones are the result worth checking: a walk that flagged")
    print("  everything would look identical on the flagged rows alone.")
    print(_rule())
    print()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gms", default="http://127.0.0.1:8080")
    args = parser.parse_args()
    try:
        return run(args.gms)
    except urllib.error.URLError as exc:
        print(f"cannot reach GMS at {args.gms}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
