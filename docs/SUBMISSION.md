# Foreshock

**Submission — Build with DataHub: The Agent Hackathon**

Repository: <https://github.com/hiddenlayer1/Foreshock> · Apache-2.0

---

## The change nobody would flag

Someone drops `device_fingerprint` from `raw.transactions`. It is a hashed
string from a card terminal, nothing reads it in the warehouse, and the review
is one person looking at one table.

Two hops away, `device_fingerprint_entropy` is computed from it — and that
feature feeds `fraud_detector`, a tier-1 model sitting in the live
authorization path. A missing feature there fails open.

Nothing in the change says any of that. The table does not mention the model,
the reviewer has no reason to look, and the damage surfaces days later as a
model regression rather than as the schema edit it actually was.

DataHub already knows the whole path. It just has no way to tell an agent in
time.

## Inspiration

DataHub has no GraphQL subscriptions
([datahub#15497](https://github.com/datahub-project/datahub/issues/15497)), so
every agent built on it has to poll. Polling is the wrong shape for this
problem twice over: it is too slow to catch a change while it is still being
reviewed, and it scales with how often you ask rather than with how often
something happens.

Meanwhile DataHub *already* emits Metadata Change Log (MCL) events to Kafka
internally. The signal exists. It simply has no agent-facing surface.

Foreshock is that surface — a typed, subscribable event bus over the MCL
stream — plus one worked consumer: an ML blast-radius agent.

## What it does

An upstream column change surfaces, within seconds, as a warning on the
production ML models it is about to break, written back into DataHub itself.

```
MCL Kafka stream
      ↓
typed subscribable agent event bus
      ↓
blast-radius agent  ──(MCP: get_lineage / get_lineage_paths_between)──▶  DataHub
      ↓                                                                     ▲
findings written back ──(MCP mutation tools)─────────────────────────────────┘
```

The loop runs end to end against a live self-hosted DataHub Core v1.5.0.6:
MCL consumed off Kafka, breaking changes pre-filtered, lineage walked through
DataHub's own MCP server, findings written back as tags.

The pre-filter is load-bearing. Most metadata traffic is ownership and tag
edits; walking lineage for all of it would make agent cost scale with total
write volume instead of with risk.

## The technical claim: the warning follows the column, not the table

Warning about models that are fine is how a tool like this earns a mute rule.
So the blast radius is scoped to the column that changed.

Same table, two different columns, both against the live instance:

| Dropped column | Downstream assets reported | Models flagged |
|---|---|---|
| `device_fingerprint` | 3 | `fraud_detector` |
| `amount` | 6 | `fraud_detector`, `churn_predictor` |

The second row is the control, and it is the row that makes the first one
mean anything. `amount` genuinely does feed `lifetime_value` in
`customer_features`, so `churn_predictor` really is downstream and a correct
analysis has to reach it. Without that row, the narrow first result is
indistinguishable from a graph walk that quietly stopped early.

Where a platform emits no column-level lineage, the analysis falls back to
table scope and marks itself imprecise rather than reporting a narrow answer
it cannot support.

## How I built it

**The thesis had to be proven, not assumed.** The whole design rests on
whether a breaking change is diffable at the moment it is consumed. It is: a
real MCL envelope captured off Kafka carries `previousAspectValue` with the
pre-drop column set, so the diff needs no callback to GMS. That envelope is
checked in as a fixture and replayed by the test suite, which pins the
contract to observed behaviour instead of to an assumption about it.

**Lineage and metadata writes are not reimplemented.** DataHub ships an MCP
server exposing both, and Foreshock calls it. The only genuinely new substrate
is the typed event stream — the part that does not exist yet.

**Write-back tags rather than rewrites descriptions.** Applying a tag twice is
a no-op, so a replayed Kafka event cannot corrupt human-authored text.
Verified: three applications leave exactly one tag.

**Write-back was verified by querying DataHub, not by trusting the agent.**
After a run, `fraud_detector`, the affected feature, and the affected column
carry `urn:li:tag:foreshock_at_risk`; `churn_predictor` is clean.

**It degrades honestly.** MCP calls carry a 45s timeout. A failed column walk
falls back to table scope and self-marks imprecise. A failed table walk
propagates, so the Kafka offset is not committed and the event is not silently
lost. A truncated graph is never reported as "nothing at risk".

## What it deliberately is not

This is a **reactive substrate plus an ML blast-radius consumer** — pre-emptive
review at metadata-mutation time.

It is not runtime pipeline gating and not a policy engine. Those are DataHub
Cloud's paid surface, and rebuilding them for free would compete with the
product rather than extend it. Foreshock warns; it never blocks a change,
approves one, or arbitrates whether one is allowed.

That boundary is also the design: because lineage traversal and metadata
writes go through DataHub's own MCP server, the only thing Foreshock adds is
the missing event surface. Any agent can subscribe to the same bus. The
blast-radius agent is one consumer, shipped as the worked example.

## Challenges

**A Kafka consumer that silently delivered nothing.** Subscribing an
`MclSource` and then manually assigning partitions returns no messages —
librdkafka treats the two modes as exclusive, `assignment()` still looks
populated, and no error is raised anywhere. It only reproduces when about ten
seconds pass between the two calls, so it presented as a lineage fault rather
than a consumer fault. The code now raises instead of going quiet.

**A poll budget that expired instantly.** The loop counted `poll()` calls,
which looks equivalent to counting time but is not: `poll` returns immediately
when a message is already buffered, so a backlog burned the entire budget in
milliseconds without any time passing. It is wall-clock now.

**Column-level precision was the hard part**, not the plumbing. Reaching the
right models means resolving the changed column to specific downstream
columns, then those to the features computed from them, then those to the
models served by them — and being willing to say "imprecise" when any of it is
unavailable.

## Accomplishments

- The reactive loop works end to end against a live instance, not a mock.
- Blast radius is column-precise, with a control case proving it.
- Write-back is idempotent and independently verified in DataHub.
- One command runs the entire argument in about 25 seconds and restores state,
  so it is re-runnable: `python scripts/demo.py --annotate`.
- 75 tests pass with no broker and no DataHub instance required.

## What I learned

The most useful thing was how much DataHub already models that nothing
downstream consumes in real time. Fine-grained column lineage, ML feature
sources, model criticality — everything needed to answer "what does this
break" is expressible in aspects DataHub already ships, written through the
ordinary SDK. The missing piece was never the metadata. It was the ability to
react to it the moment it changes.

The second was that a precision claim needs a control. Reporting three assets
instead of six is only impressive if you can show the same code reports six
when six is correct.

## What's next

- More consumers on the same bus — the substrate is the reusable part.
- Broader change coverage: type changes are handled alongside drops; deprecation
  and ownership-loss signals are the natural next ones.
- Richer findings, so a warning carries which feature and which serving model
  are affected without a round trip.

## Built with

Python 3.10+ · DataHub Core (self-hosted, no Cloud account) · Apache Kafka
(MCL stream) · DataHub's MCP server · `acryl-datahub` SDK · pytest

## Try it

```bash
pip install -e ".[dev]"
python scripts/demo.py --annotate
```

Seeds a synthetic ML estate, makes both changes against a live instance,
reports each blast radius from the resulting Kafka events, writes the findings
back, and restores the schema so it can be run again.

## Provenance

Clean-room: created and written entirely within the hackathon submission
window, with no code carried in from any pre-existing codebase. The public
commit history is the record. Third-party dependencies are installed from
public package registries in the normal way.

Licensed under Apache-2.0.
