"""Read/write helpers shared by every job.

Two rules the whole pipeline relies on:

1. Reads are explicit — declared schema, PERMISSIVE mode, corrupt rows kept.
2. Writes are idempotent — a partition is overwritten, never appended twice,
   so a re-run of the same logical date produces the same table.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .config import Config
from .schemas import CORRUPT_COLUMN, SOURCE_SCHEMAS
from .session import get_logger

log = get_logger("io")

SQL_DIR = Path(__file__).resolve().parent / "sql"


# --------------------------------------------------------------------------
# Landing zone
# --------------------------------------------------------------------------
def read_landing(
    spark: SparkSession,
    config: Config,
    source: str,
    logical_date: str | None = None,
) -> DataFrame:
    """Read one landing source with its declared schema.

    When the source is partitioned by ingest date and a logical date is given,
    only that day's files are read — this is what makes the DAG backfillable.
    """
    spec = config.sources[source]
    schema: StructType = SOURCE_SCHEMAS[source]
    base = config.landing_path(source)

    if spec.get("partition_by_ingest_date") and logical_date:
        path = base / f"ingest_date={logical_date}"
    else:
        path = base

    reader = spark.read.schema(schema).option("mode", "PERMISSIVE")
    if CORRUPT_COLUMN in schema.fieldNames():
        reader = reader.option("columnNameOfCorruptRecord", CORRUPT_COLUMN)

    fmt = spec["format"]
    if fmt == "csv":
        reader = reader.option("header", "true").option("escape", '"')
    elif fmt == "json":
        reader = reader.option("multiLine", "false")

    log.info("reading %s source=%s path=%s", fmt, source, path)
    return reader.format(fmt).load(str(path))


# --------------------------------------------------------------------------
# Warehouse
# --------------------------------------------------------------------------
def write_table(
    df: DataFrame,
    config: Config,
    layer: str,
    table: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> Path:
    """Write a layer table as Parquet and return its path."""
    path = config.layer_path(layer, table)
    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))
    log.info("wrote %s.%s -> %s", layer, table, path)
    return path


def read_table(spark: SparkSession, config: Config, layer: str, table: str) -> DataFrame:
    path = config.layer_path(layer, table)
    return spark.read.parquet(str(path))


def table_exists(config: Config, layer: str, table: str) -> bool:
    path = config.layer_path(layer, table)
    return path.exists() and any(path.iterdir())


def register_views(
    spark: SparkSession, config: Config, layer: str, tables: list[str] | None = None
) -> list[str]:
    """Expose warehouse tables to Spark SQL as `<layer>_<table>` views.

    The SQL files are written against these names, which keeps the .sql files
    free of absolute paths and lets tests register in-memory fixtures instead.
    """
    registered = []
    for table in tables or config.tables(layer):
        if not table_exists(config, layer, table):
            continue
        view = f"{layer}_{table}"
        read_table(spark, config, layer, table).createOrReplaceTempView(view)
        registered.append(view)
    log.info("registered views: %s", ", ".join(registered) or "(none)")
    return registered


# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------
def load_sql(layer: str, name: str, **params: Any) -> str:
    """Load a .sql file and substitute `{param}` placeholders."""
    path = SQL_DIR / layer / f"{name}.sql"
    statement = path.read_text()
    return statement.format(**params) if params else statement


def run_sql(spark: SparkSession, layer: str, name: str, **params: Any) -> DataFrame:
    log.info("executing sql %s/%s.sql", layer, name)
    return spark.sql(load_sql(layer, name, **params))


# --------------------------------------------------------------------------
# Run metadata
# --------------------------------------------------------------------------
def with_ingest_metadata(df: DataFrame, batch_id: str, source: str) -> DataFrame:
    """Stamp lineage columns onto a bronze DataFrame."""
    return (
        df.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source", F.lit(source))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )


def make_batch_id(logical_date: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%H%M%S")
    return f"{logical_date.replace('-', '')}-{stamp}"


def write_run_manifest(config: Config, payload: dict[str, Any]) -> Path:
    """Append a JSON line describing the run — the pipeline's own audit log."""
    directory = config.warehouse / "_runs"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{payload.get('logical_date', 'unknown')}.jsonl"
    payload = {"recorded_at": datetime.now(timezone.utc).isoformat(), **payload}
    with open(path, "a") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")
    return path
