"""Airflow DAG for the card-rewards medallion pipeline.

Scheduling note
---------------
The schedule is *defined* but the DAG ships paused (`is_paused_upon_creation=
True`, reinforced by AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION in the compose
file). Cloning this repo and starting Airflow will not fire anything; unpause
it in the UI, or trigger a single run manually, when you actually want it to
execute. Catchup is off for the same reason — turning the DAG on should not
kick off two weeks of backfill.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = Path(os.getenv("RP_PROJECT_ROOT", "/opt/pipeline"))
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}

DOC_MD = """
### Card rewards medallion pipeline

`landing -> bronze -> silver -> gold`, one Spark job per layer, with a
declarative data-quality gate between layers.

* **bronze** — raw ingest with lineage columns; unparseable rows quarantined.
* **silver** — dedupe, FX conversion to USD, SCD2 card dimension, validation.
* **gold** — rewards fact plus the ROI, category and member marts.

A `fail`-severity expectation breach exits the task with code 2 and stops the
downstream layers. Quarantined and rejected rows are written under
`warehouse/_quarantine/` for inspection.

**This DAG is paused on creation by design.**
"""


with DAG(
    dag_id="rewards_medallion",
    description="Card transactions to rewards-ROI marts (bronze/silver/gold)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 9, 1),
    schedule="0 6 * * *",  # 06:00 UTC daily — inert while paused
    catchup=False,
    is_paused_upon_creation=True,  # nothing runs until a human unpauses it
    max_active_runs=1,
    tags=["rewards", "spark", "medallion", "batch"],
    doc_md=DOC_MD,
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task(task_id="check_landing_zone")
    def check_landing_zone(ds: str) -> dict:
        """Fail fast if the upstream drop never arrived for this logical date."""
        from rewards_pipeline.config import load_config

        config = load_config()
        partition = config.landing_path("transactions") / f"ingest_date={ds}"
        if not partition.exists():
            raise FileNotFoundError(
                f"no transaction drop for {ds} at {partition}; "
                "run `make seed` or point RP_LANDING_PATH at the real feed"
            )
        files = sorted(partition.glob("*.json"))
        return {"logical_date": ds, "files": len(files)}

    @task(task_id="ingest")
    def run_bronze(ds: str) -> dict:
        from rewards_pipeline.jobs import bronze_ingest

        return bronze_ingest.run(ds)

    @task(task_id="transform")
    def run_silver(ds: str) -> dict:
        from rewards_pipeline.jobs import silver_transform

        return silver_transform.run(ds)

    @task(task_id="build_marts")
    def run_gold(ds: str) -> dict:
        from rewards_pipeline.jobs import gold_marts

        return gold_marts.run(ds)

    @task(task_id="publish_run_report")
    def publish_run_report(bronze: dict, silver: dict, gold: dict, ds: str) -> dict:
        """Collapse the three stage summaries into one line for the run log."""
        from rewards_pipeline.config import load_config
        from rewards_pipeline.io import write_run_manifest

        config = load_config()
        report = {
            "stage": "run_report",
            "logical_date": ds,
            "bronze_rows": bronze["total_rows"],
            "bronze_quarantined": bronze["total_quarantined"],
            "silver_rows": silver["total_rows"],
            "silver_rejected": silver["total_rejected"],
            "gold_rows": gold["total_rows"],
            "gold_tables": {item["table"]: item["rows"] for item in gold["tables"]},
        }
        write_run_manifest(config, report)
        return report

    with TaskGroup(group_id="bronze") as bronze_group:
        bronze_result = run_bronze(ds="{{ ds }}")

    with TaskGroup(group_id="silver") as silver_group:
        silver_result = run_silver(ds="{{ ds }}")

    with TaskGroup(group_id="gold") as gold_group:
        gold_result = run_gold(ds="{{ ds }}")

    landing_check = check_landing_zone(ds="{{ ds }}")
    report = publish_run_report(bronze_result, silver_result, gold_result, ds="{{ ds }}")

    start >> landing_check >> bronze_group >> silver_group >> gold_group >> report >> finish
