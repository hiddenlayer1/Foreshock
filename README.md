# Foreshock

**DataHub already knows what is about to break. It just has no way to tell an agent in time.**

Foreshock turns DataHub's metadata change stream into live blast-radius warnings — so an
upstream column edit surfaces as a warning on the production ML models it is about to
break, within seconds, inside DataHub itself.

> Built for **Build with DataHub: The Agent Hackathon** (2026).

---

## The gap this closes

DataHub has no GraphQL subscriptions ([datahub#15497]), so every agent built on it must
poll. Meanwhile DataHub *already* emits Metadata Change Log (MCL) events to Kafka
internally — the signal exists, it just has no agent-facing surface.

Foreshock is that surface:

```
MCL Kafka stream
      ↓
typed subscribable agent event bus
      ↓
blast-radius agent  ──(MCP: get_lineage / get_lineage_paths_between)──▶  DataHub
      ↓                                                                     ▲
findings written back ──(MCP mutation tools)─────────────────────────────────┘
```

Any agent built on DataHub can subscribe to the same bus. Foreshock ships one consumer —
the ML blast-radius agent — as the worked example.

[datahub#15497]: https://github.com/datahub-project/datahub/issues/15497

## What it is not

This is a **reactive substrate plus an ML blast-radius consumer**: pre-emptive review at
metadata-mutation time. It is deliberately *not* runtime pipeline gating and *not* a
policy engine — those are DataHub Cloud's paid surface, and reimplementing them would
compete with the product rather than extend it.

## What it does, demonstrated

Foreshock scopes the warning to the **column** that changed, not the table. Same table,
two different columns, against a live DataHub Core instance:

| Dropped column | Downstream assets reported | Models flagged |
|---|---|---|
| `device_fingerprint` | 3 | `fraud_detector` |
| `amount` | 6 | `fraud_detector`, `churn_predictor` |

The second row is the control. `amount` really does feed `lifetime_value` in
`customer_features`, so `churn_predictor` is genuinely downstream and is correctly
reported — which is what makes the first row precision rather than a walk that stopped
early. Warning about models that are fine is how a tool like this earns a mute rule.

Where a platform emits no column-level lineage, the analysis falls back to table scope
and says so rather than reporting a narrow result it cannot support.

## Quickstart

Bring up DataHub Core (see [docs/running-datahub-on-podman.md](docs/running-datahub-on-podman.md)
if you use Podman — three things bite), then run both scenarios end to end:

```bash
pip install -e ".[dev]"
python scripts/demo.py --annotate
```

That seeds the estate, makes both changes against the live instance, reports each
blast radius from the resulting Kafka events, writes the findings back into DataHub,
and restores the schema so it can be run again. It takes about 25 seconds. Drop
`--annotate` to see what it *would* write without writing anything.

### Running the pieces separately

```bash
pip install -e ".[dev]"

# 1. Seed a synthetic ML estate: raw tables -> feature tables -> models,
#    with column-level lineage.
python scripts/seed_estate.py

# 2. Watch the change stream.
python scripts/watch.py

# 3. In another shell, break something.
python -c "from datahub.emitter.rest_emitter import DatahubRestEmitter; \
           from foreshock.estate import drop_column; \
           drop_column(DatahubRestEmitter('http://127.0.0.1:8080'), \
                       'raw.transactions', 'device_fingerprint')"
```

The warning appears in the watcher within seconds. Nothing polls — it arrives because
DataHub emitted the change and Foreshock was subscribed to it.

To capture a fresh MCL envelope as a test fixture:

```bash
python scripts/capture_mcl.py
```

## Tests

```bash
pytest
```

The suite runs without a broker or a DataHub instance. Envelope projection and
blast-radius ranking are pure functions; a checked-in fixture captured from a real
DataHub instance pins the contract to observed behaviour rather than to an assumption
about it.

## Requirements

- DataHub Core, self-hosted (no Cloud account required)
- Python 3.10+
- A container runtime for the DataHub quickstart stack

## Provenance

This repository is clean-room: created and written entirely within the hackathon
submission window, with no code carried in from any pre-existing codebase. Third-party
dependencies are installed from public package registries in the normal way.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
