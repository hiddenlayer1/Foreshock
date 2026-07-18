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

## Status

Early. Setup instructions, fixture seeding, and demo steps land as the build progresses.

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
