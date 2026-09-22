"""Gold layer — business marts, expressed entirely in SQL.

Gold is where analysts read, so the logic lives in .sql files they can open in
any editor. Python's only responsibility is ordering, persistence and the
quality gate.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from ..config import Config, load_config
from ..io import read_table, register_views, run_sql, write_run_manifest, write_table
from ..quality import run_checks
from ..session import configure_logging, get_logger, spark_session

log = get_logger("gold")

# transaction_rewards feeds the other three, so it is built first.
BUILD_ORDER = [
    "transaction_rewards",
    "card_roi_monthly",
    "merchant_category_spend",
    "member_rewards_summary",
]

PARTITIONED = {"transaction_rewards": ["month"]}


def build_mart(spark: SparkSession, config: Config, table: str) -> dict:
    df = run_sql(spark, "gold", table)
    write_table(df, config, "gold", table, partition_by=PARTITIONED.get(table))

    persisted = read_table(spark, config, "gold", table)
    persisted.createOrReplaceTempView(f"gold_{table}")
    run_checks(persisted, config, "gold", table, spark=spark)

    count = persisted.count()
    log.info("gold.%s: %d row(s)", table, count)
    return {"table": table, "rows": count}


def run(logical_date: str, config: Config | None = None) -> dict:
    config = config or load_config()
    stats = []

    with spark_session(config, "gold") as spark:
        register_views(spark, config, "silver")
        for table in BUILD_ORDER:
            stats.append(build_mart(spark, config, table))

    summary = {
        "stage": "gold",
        "logical_date": logical_date,
        "tables": stats,
        "total_rows": sum(item["rows"] for item in stats),
    }
    write_run_manifest(config, summary)
    return summary


def main(logical_date: str) -> None:
    configure_logging()
    run(logical_date)


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1])
