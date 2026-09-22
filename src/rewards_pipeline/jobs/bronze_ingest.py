"""Bronze layer — land the raw feeds as-is, with lineage.

Bronze keeps source fidelity: no business rules, no filtering, no renaming.
The only additions are lineage columns and a split of unparseable rows into a
quarantine table, so a bad upstream release is inspectable rather than silently
dropped.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Config, load_config
from ..io import (
    make_batch_id,
    read_landing,
    with_ingest_metadata,
    write_run_manifest,
    write_table,
)
from ..quality import run_checks
from ..schemas import CORRUPT_COLUMN
from ..session import configure_logging, get_logger, spark_session

log = get_logger("bronze")

# Sources partitioned by ingest date; the rest are full daily snapshots.
INCREMENTAL_SOURCES = {"transactions"}


def _split_corrupt(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate parseable rows from rows the reader could not decode."""
    if CORRUPT_COLUMN not in df.columns:
        return df, None  # type: ignore[return-value]
    clean = df.filter(F.col(CORRUPT_COLUMN).isNull())
    corrupt = df.filter(F.col(CORRUPT_COLUMN).isNotNull())
    return clean, corrupt


def ingest_source(
    spark: SparkSession,
    config: Config,
    source: str,
    logical_date: str,
    batch_id: str,
) -> dict:
    incremental = source in INCREMENTAL_SOURCES
    raw = read_landing(spark, config, source, logical_date if incremental else None)
    raw = with_ingest_metadata(raw, batch_id, source)

    if incremental:
        raw = raw.withColumn("ingest_date", F.lit(logical_date).cast("date"))

    # Spark refuses to plan a query whose only referenced source column is
    # _corrupt_record, so the frame is cached and materialised once here. Every
    # narrow query below then reads the in-memory relation instead of the file.
    raw = raw.cache()
    landed = raw.count()

    clean, corrupt = _split_corrupt(raw)

    quarantined = 0
    if corrupt is not None:
        quarantined = corrupt.count()
        if quarantined:
            path = config.quarantine_path(f"bronze_{source}")
            corrupt.select(CORRUPT_COLUMN, "_batch_id", "_source_file", "_ingested_at").write.mode(
                "append"
            ).parquet(path)
            log.warning("quarantined %d unparseable row(s) from %s", quarantined, source)

    write_table(
        clean,
        config,
        "bronze",
        source,
        mode="overwrite",
        partition_by=["ingest_date"] if incremental else None,
    )

    # Quality runs against everything that landed, corrupt rows included, so
    # the corrupt-ratio expectation has a denominator.
    run_checks(raw, config, "bronze", source, spark=spark)

    row_count = landed - quarantined
    raw.unpersist()
    log.info("bronze.%s: %d row(s), %d quarantined", source, row_count, quarantined)
    return {"source": source, "rows": row_count, "quarantined": quarantined}


def run(logical_date: str, config: Config | None = None) -> dict:
    config = config or load_config()
    batch_id = make_batch_id(logical_date)
    stats = []

    with spark_session(config, "bronze") as spark:
        for source in config.tables("bronze"):
            stats.append(ingest_source(spark, config, source, logical_date, batch_id))

    summary = {
        "stage": "bronze",
        "logical_date": logical_date,
        "batch_id": batch_id,
        "tables": stats,
        "total_rows": sum(item["rows"] for item in stats),
        "total_quarantined": sum(item["quarantined"] for item in stats),
    }
    write_run_manifest(config, summary)
    return summary


def main(logical_date: str) -> None:
    configure_logging()
    run(logical_date)


if __name__ == "__main__":  # pragma: no cover
    import sys

    main(sys.argv[1])
