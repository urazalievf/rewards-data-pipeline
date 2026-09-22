# Architecture

## Why medallion

Three layers, each with one job, so a failure has one obvious owner:

| Layer | Contract | Rewritable? |
|---|---|---|
| **bronze** | Source fidelity. No business rules. Lineage added, nothing removed. | Append per ingest date |
| **silver** | Conformed and trustworthy. Deduped, typed, FX-normalised, validated. | Full recompute from bronze |
| **gold** | Business meaning. Marts an analyst can query without context. | Full recompute from silver |

Silver and gold are recomputed rather than incrementally merged. At this data
volume it is cheaper than maintaining merge state, and it means a logic fix is
a re-run rather than a migration. Bronze is the only layer that accumulates,
partitioned by `ingest_date`, and it is written with dynamic partition
overwrite so replaying a date replaces exactly that date.

## Data flow

```
                    ┌───────────────────────────────────────────┐
  landing/          │ bronze/                                   │
  ├─ transactions ──┤ + _batch_id, _source, _source_file,       │
  │  (JSON, daily)  │   _ingested_at, ingest_date               │
  ├─ cards          │ + corrupt rows split to _quarantine/      │
  │  (CSV snapshot) └──────────────────┬────────────────────────┘
  ├─ merchants                         │
  ├─ reward_rules      ┌───────────────▼───────────────────────┐
  └─ fx_rates          │ silver/                               │
                       │  fx_rates    one rate per ccy per day │
                       │  merchants   conformed                │
                       │  reward_rules typed, date-bounded     │
                       │  cards       SCD2 (valid_from/to)     │
                       │  transactions dedup → FX → validate   │
                       │              rejects → _quarantine/   │
                       └───────────────┬───────────────────────┘
                                       │
                       ┌───────────────▼───────────────────────┐
                       │ gold/                                 │
                       │  transaction_rewards  (as-of + caps)  │
                       │      ├─► card_roi_monthly             │
                       │      ├─► merchant_category_spend      │
                       │      └─► member_rewards_summary       │
                       └───────────────────────────────────────┘
```

Build order is a dependency order, not a convenience: FX and the dimensions
exist before transactions are converted, and `transaction_rewards` exists
before the three marts that aggregate it.

## Key decisions

**Schemas are declared, never inferred.** Inference re-reads the data, costs a
pass, and silently retypes a column when one batch happens to be all integers.
Every source has a `StructType` in `schemas.py` and a `_corrupt_record` column,
so a malformed row is captured rather than dropped.

**Rejects are quarantined, not filtered.** The silver SQL tags rows with a
`_reject_reason` instead of removing them; the job routes them to
`_quarantine/`. A bad upstream release is then a query, not an investigation.

**SQL for transformations, Python for wiring.** Every transformation is a
`.sql` file an analyst can open. Python handles configuration, ordering,
persistence and the quality gate — the parts SQL is bad at.

**Contracts are declared, and checked against reality.** `schemas/*.yml` states
what each table promises; `rewards schema check` compares that against the
schema the pipeline actually produced and fails CI on drift. This is the only
thing that makes the contract binding, because with Parquet the catalog cannot
reject a write that contradicts it.

**Table definitions are deployed, not implied.** `ddl/` holds numbered
migrations applied by `rewards migrate`, with a checksummed ledger in the
warehouse. Without this the schema is a side effect of the last write, and a
renamed column is a silent break for every consumer. The trade-off with bare
Parquet is that the catalog cannot enforce agreement between DDL and writer —
they have to change in the same PR.

**Quality gates between layers, not after.** Expectations run against what was
actually persisted, before the next layer reads it. A `fail` breach raises
`DataQualityError`, which the CLI turns into exit code 2 so an orchestrator can
distinguish a data problem from a code crash.

**Idempotency by construction.** Re-running any stage for any logical date
produces the same output. This is what makes `backfill` safe and retries
meaningless to reason about.

## Cluster portability

No job references a local path or a local master. `SPARK_MASTER_URL` selects
local vs cluster, `RP_LANDING_PATH` / `RP_WAREHOUSE_PATH` select filesystem vs
object store. The same code runs on a laptop and on a cluster.

## What a production version would add

Honest list of what this showcase leaves out:

- **Delta Lake or Iceberg** instead of bare Parquet — ACID commits, time
  travel, `MERGE` for true incremental silver, and schema evolution enforced
  by the table format. The `ddl/` migrations would shrink rather than
  disappear: `ALTER TABLE` still ships as a numbered file, but the format
  would reject a write that contradicts the declared schema instead of
  letting the catalog drift.
- **A real catalog** (Glue, Unity, Hive) so tables are discoverable rather
  than path-addressed.
- **Streaming ingest** for transactions, with the batch layer reduced to
  compaction and late-arrival reconciliation.
- **Column-level lineage** (OpenLineage) emitted from each job.
- **Alerting** on the quality reports rather than only failing the task.
- **Secrets and IAM** — this version has no credentials at all by design.
