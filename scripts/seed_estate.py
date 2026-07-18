"""Seed the synthetic ML estate into a running DataHub instance.

Usage:
    python scripts/seed_estate.py [--gms http://127.0.0.1:8080]
"""

from __future__ import annotations

import argparse
import sys

from datahub.emitter.rest_emitter import DatahubRestEmitter

from foreshock.estate import FEATURES, MODELS, TABLES, seed_estate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gms", default="http://127.0.0.1:8080")
    args = parser.parse_args()

    emitter = DatahubRestEmitter(gms_server=args.gms)
    emitter.test_connection()

    written = seed_estate(emitter)
    print(f"emitted {written} aspects to {args.gms}")
    for table in TABLES:
        print(f"  dataset  {table.urn}")
    for feature in FEATURES:
        print(f"  feature  {feature.urn}")
    for model in MODELS:
        print(f"  model    {model.urn}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
