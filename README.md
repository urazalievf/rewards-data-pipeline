# Card Rewards Data Pipeline

A production-shaped batch pipeline that turns raw card-transaction feeds into
rewards-ROI marts, built with **PySpark**, **Spark SQL** and **Airflow** on a
medallion (bronze → silver → gold) architecture.

It answers one question end to end: *for a given member and card, did the
rewards earned this month beat the card's annual fee?*

```
landing/            bronze/              silver/                gold/
├─ transactions ──► raw + lineage ────► deduped, FX-normalised ─┐
├─ cards        ──► raw + lineage ────► SCD2 versioned ─────────┤
├─ merchants    ──► raw + lineage ────► conformed ──────────────┼─► transaction_rewards
├─ reward_rules ──► raw + lineage ────► typed, date-bounded ────┤   card_roi_monthly
└─ fx_rates     ──► raw + lineage ────► one rate per day ───────┘   merchant_category_spend
                         │                      │                  member_rewards_summary
                    quarantine             quarantine                       │
                  (unparseable)        (failed validation)            data quality gate
```

---

## Quick start

Requires Python 3.10+ and **JDK 17 or 21** (Spark 3.5 does not run on newer JDKs).

```bash
make venv          # virtualenv + dependencies
make doctor        # confirms your JDK is one Spark can use
make run           # seed data, then bronze → silver → gold
make preview       # print the gold marts
```

No Java on hand? Everything also runs in a container:

```bash
make docker-run
make docker-preview
```

`make run` processes a single logical date, the way the scheduled DAG does.
To load two weeks of history in one go:

```bash
make backfill          # or: make backfill DAYS=30
```

---

## What it demonstrates

| Area | Implementation |
|---|---|
| **Medallion layering** | Three isolated stages, each independently runnable and retryable |
| **Schema contracts** | Declared schemas per source; no inference, corrupt rows preserved |
| **Deduplication** | Window-ranked by ingest time — the source re-delivers transactions |
| **SCD Type 2** | Card dimension versioned in pure SQL with hash-diff + window functions |
| **As-of joins** | Transactions join the card version that was in force that day |
| **Window analytics** | Monthly reward caps enforced with a running sum, not a post-hoc filter |
| **Data quality** | Declarative expectations in YAML, fail/warn severities, quarantine |
| **Idempotency** | Dynamic partition overwrite; re-running a date reproduces the same output |
| **Orchestration** | Airflow DAG with TaskGroups, retries with exponential backoff, backfill support |
| **Testing** | 38 tests covering the quality engine, the SQL transforms and the seed data |
| **CI** | Lint, type-check, unit tests and a full end-to-end run on generated data |

---

## The interesting logic

### SCD Type 2 without a merge

The card feed sends a full snapshot daily, so versioning is a pure window
problem: hash the tracked attributes, mark the rows where the hash changes,
running-sum those marks into a version number, then collapse each version into
one row with a validity interval. Fully recomputable from history, no state.

See [`sql/silver/cards.sql`](src/rewards_pipeline/sql/silver/cards.sql).

### Monthly caps that stop at the cap

A 3× dining bonus capped at $500/month cannot simply be switched off by the
transaction that crosses the cap — that transaction is *partly* eligible. A
running sum inside the member/card/month/rule window computes exactly how much
of each transaction still fits:

```sql
GREATEST(0.0, LEAST(amount_usd, monthly_cap_usd - (running_bonus_spend - amount_usd)))
```

See [`sql/gold/transaction_rewards.sql`](src/rewards_pipeline/sql/gold/transaction_rewards.sql).

### Quality as configuration, not code

Expectations live in [`conf/expectations.yaml`](conf/expectations.yaml), so
adding a check is a config change and the whole contract is readable in one
file:

```yaml
silver.transactions:
  - {type: unique, column: txn_id, severity: fail}
  - {type: accepted_values, column: status, values: [settled, refunded], severity: fail}
  - {type: freshness_days_max, column: txn_ts, value: 45, severity: warn}

silver.cards:
  # Exactly one current row per natural key is the whole point of the SCD2 build.
  - {type: unique, column: card_id, where: "is_current", severity: fail}
```

Ten check types are supported: `row_count_min`, `not_null`, `unique`,
`unique_composite`, `accepted_values`, `range`, `corrupt_ratio_max`,
`freshness_days_max` and `referential_integrity`, each scopeable with `where`.
A `fail` breach exits with code 2 so the orchestrator can alert on a data
problem differently from a code crash.

---

## Bad data is expected, not assumed away

The seed generator deliberately produces the mess a real feed produces —
unparseable JSON lines, re-delivered duplicates, missing amounts, currencies
the FX feed does not publish, and mid-window card repricing. Nothing is
silently dropped:

```
bronze.transactions: 2513 row(s), 3 quarantined        ← unparseable JSON lines
silver.transactions quarantined 305 row(s): missing_amount=201, unknown_currency=104
silver.transactions: 34695 row(s), 305 rejected
gold.transaction_rewards: 33319 row(s)
```

Quarantined rows keep their rejection reason under
`data/warehouse/_quarantine/`, and every run appends a manifest to
`data/warehouse/_runs/`.

---

## Environments

Environments are **configuration, not branches**. There is one long-lived
branch (`main`); `uat` and `prod` are config overlays selected by `RP_ENV`,
deep-merged over `conf/pipeline.yaml` so each file states only what differs:

```bash
RP_ENV=prod rewards config     # resolve and print, without starting Spark
```

```
local  warehouse=data/warehouse                shuffle=8    quality=fail
uat    warehouse=s3a://rewards-uat/warehouse   shuffle=64   quality=fail
prod   warehouse=s3a://rewards-prod/warehouse  shuffle=400  quality=fail
```

The same commit is promoted from uat to prod by the
[`deploy`](.github/workflows/deploy.yml) workflow, which selects the overlay
and pulls per-environment credentials from GitHub Environment secrets — so uat
and prod cannot borrow each other's. `prod` sits behind a required reviewer,
so choosing it pauses for approval before any step runs.

This is deliberate: long-lived `uat`/`prod` branches drift, turn promotion into
cherry-picking, and make "what is running in prod" a branch tip rather than a
tagged artifact.

Data locations may be local paths or URIs (`s3a://`, `hdfs://`). Location
joining never goes through `pathlib`, which would silently collapse `s3a://`
into `s3a:/`, and existence checks go through Hadoop's `FileSystem` API so
they answer correctly for local disk and object storage alike.

## Orchestration

The Airflow DAG ([`dags/rewards_medallion.py`](dags/rewards_medallion.py)) runs
the three stages as separate tasks behind a landing-zone check, so a failed
layer retries without replaying the others.

```bash
make airflow-up    # http://localhost:8080  (admin / admin)
```

> **Schedules are defined but switched off.** The DAG declares `0 6 * * *` and
> ships with `is_paused_upon_creation=True`; the GitHub Actions workflows are
> `workflow_dispatch`-only with their `push`/`schedule` triggers commented out.
> Cloning this repo starts nothing on its own. Unpause the DAG, or uncomment a
> trigger block, when you want it to run.

---

## Layout

```
conf/           pipeline.yaml + per-environment overlays, and expectations.yaml (the DQ contract)
dags/           Airflow DAG
scripts/        find-jdk.sh, used by the Makefile to locate a Spark-compatible JDK
docker/         Pinned JDK 17 images for the pipeline and for Airflow
src/rewards_pipeline/
  config.py     YAML + RP_* env overlay, single source of truth for paths
  session.py    SparkSession construction, local/cluster switch, logging
  schemas.py    Declared source schemas and natural keys
  io.py         Readers, idempotent writers, SQL loader, run manifests
  quality.py    The expectation engine
  cli.py        rewards {seed,bronze,silver,gold,run-all,backfill,preview}
  jobs/         One module per layer
  sql/          Every transformation, as plain .sql
tests/          38 tests (pytest); -m "not slow" skips the JVM-backed ones
```

## Commands

```bash
make help        # every target
make seed        # regenerate the synthetic landing zone
make bronze DATE=2026-09-22
make silver DATE=2026-09-22
make gold   DATE=2026-09-22
make backfill DAYS=30  # ingest a range, then rebuild silver and gold once
make quality     # print the latest data-quality report
RP_ENV=uat .venv/bin/rewards config   # resolved config for an environment
make test        # full suite
make test-fast   # skip the JVM-backed tests
make lint        # ruff + mypy
make cluster-up  # standalone Spark master + 2 workers
make clean
```

## Running against a real cluster

Nothing in the job code assumes local mode. Point it at a master and it
submits there instead:

```bash
export SPARK_MASTER_URL=spark://spark-master:7077
export RP_ENV=prod                    # or set the locations explicitly:
export RP_LANDING_PATH=s3a://your-bucket/landing
export RP_WAREHOUSE_PATH=s3a://your-bucket/warehouse
export RP_REPORTS_PATH=/var/log/rewards-pipeline
.venv/bin/rewards run-all
```
