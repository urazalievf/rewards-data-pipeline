"""Silver layer — conform, deduplicate, version and validate.

Each table is one SQL file; this module only wires inputs to outputs and
decides what happens to rows the SQL flagged as untrustworthy. Build order
matters: FX and the dimensions exist before transactions are converted.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Config, load_config
from ..io import register_views, run_sql, write_run_manifest, write_table
from ..quality import run_checks
from ..session import configure_logging, get_logger, spark_session

log = get_logger("silver")

# Dependency-ordered: later tables read views registered by earlier ones.
BUILD_ORDER = ["fx_rates", "merchants", "reward_rules", "cards", "transactions"]

PARTITIONED = {"transactions": ["ingest_date"]}


def _quarantine_rejects(df: DataFrame, config: Config, table: str) -> int:
    """Park invalid rows with their reason instead of dropping them silently."""
    rejects = df.filter(~F.col("_is_valid"))
    count = rejects.count()
    if count:
        path = config.quarantine_path(f"silver_{table}")
        rejects.write.mode("append").parquet(str(path))
        breakdown = rejects.groupBy("_reject_reason").count().orderBy(F.desc("count")).collect()
        log.warning(
            "silver.%s quarantined %d row(s): %s",
            table,
            count,
            ", ".join(f"{row['_reject_reason']}={row['count']}" for row in breakdown),
        )
    return count


def build_table(spark: SparkSession, config: Config, table: str) -> dict:
    df = run_sql(spark, "silver", table)

    rejected = 0
    if "_is_valid" in df.columns:
        df.cache()
        rejected = _quarantine_rejects(df, config, table)
        df = df.filter(F.col("_is_valid")).drop("_is_valid", "_reject_reason")

    write_table(df, config, "silver", table, partition_by=PARTITIONED.get(table))

    # Re-read so the view (and the checks) see exactly what was persisted.
    from ..io import read_table

    persisted = read_table(spark, config, "silver", table)
    persisted.createOrReplaceTempView(f"silver_{table}")
    run_checks(persisted, config, "silver", table, spark=spark)

    count = persisted.count()
    log.info("silver.%s: %d row(s), %d rejected", table, count, rejected)
    return {"table": table, "rows": count, "rejected": rejected}


def run(logical_date: str, config: Config | None = None) -> dict:
    config = config or load_config()
    stats = []

    with spark_session(config, "silver") as spark:
        register_views(spark, config, "bronze")
        for table in BUILD_ORDER:
            stats.append(build_table(spark, config, table))

    summary = {
        "stage": "silver",
        "logical_date": logical_date,
        "tables": stats,
        "total_rows": sum(item["rows"] for item in stats),
        "total_rejected": sum(item["rejected"] for item in stats),
    }
    write_run_manifest(config, summary)
    return summary


def main(logical_date: str) -> None:
    configure_logging()
    run(logical_date)


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1])
