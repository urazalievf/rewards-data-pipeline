"""Versioned DDL migrations.

Table definitions are deployed, not implied. Without this the warehouse schema
is simply whatever the last write happened to produce, which gives downstream
consumers nothing to depend on and turns a renamed column into a silent break.

The model is the familiar Flyway one, kept deliberately small:

* every change is a numbered file in `ddl/`, applied in version order;
* each applied file is recorded in a ledger **in the warehouse**, so the
  record lives alongside the data it describes and is shared by everyone
  pointed at that environment;
* each file is checksummed, so editing a migration that has already been
  applied is rejected rather than quietly ignored - you write a new one;
* applying is idempotent: re-running applies only what is pending.

`{database}` and `{warehouse}` are substituted at apply time, which is what
lets the same DDL target local, uat and prod.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

from .config import REPO_ROOT, Config
from .session import get_logger

log = get_logger("migrations")

DDL_DIR = REPO_ROOT / "ddl"
# V001__create_gold_marts.sql -> version "001", name "create gold marts"
FILENAME_PATTERN = re.compile(r"^V(?P<version>\d+)__(?P<name>[a-z0-9_]+)\.sql$")
LEDGER_TABLE = "_migrations"


class MigrationError(RuntimeError):
    """Raised when the migration history and the files on disk disagree."""


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    path: Path
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()[:16]

    def statements(self, config: Config) -> list[str]:
        """Render the file and split it into executable statements."""
        rendered = self.sql.format(database=config.catalog["database"], warehouse=config.warehouse)
        return split_statements(rendered)


def split_statements(sql: str) -> list[str]:
    """Split SQL on statement boundaries, respecting quoting.

    Splitting naively on ";" is wrong the moment a COMMENT contains one -
    `COMMENT 'SCD2 dimension; filter is_current'` would be torn into two
    invalid fragments. This walks the text tracking whether it is inside a
    single-quoted literal (with '' escapes), and drops -- line comments only
    when they are not inside one.
    """
    statements: list[str] = []
    buffer: list[str] = []
    in_string = False
    index = 0

    while index < len(sql):
        char = sql[index]

        if in_string:
            buffer.append(char)
            if char == "'":
                if sql[index + 1 : index + 2] == "'":  # escaped quote
                    buffer.append("'")
                    index += 2
                    continue
                in_string = False
            index += 1
            continue

        if char == "'":
            in_string = True
            buffer.append(char)
        elif sql[index : index + 2] == "--":
            newline = sql.find("\n", index)
            index = len(sql) if newline == -1 else newline
            continue
        elif char == ";":
            statements.append("".join(buffer))
            buffer = []
        else:
            buffer.append(char)
        index += 1

    statements.append("".join(buffer))
    return [statement.strip() for statement in statements if statement.strip()]


def discover(ddl_dir: Path | None = None) -> list[Migration]:
    """Load every migration file, ordered by version."""
    directory = ddl_dir or DDL_DIR
    if not directory.exists():
        return []

    migrations: list[Migration] = []
    seen: dict[str, Path] = {}
    for path in sorted(directory.glob("*.sql")):
        match = FILENAME_PATTERN.match(path.name)
        if not match:
            raise MigrationError(f"{path.name} does not match V<version>__<name>.sql")
        version = match.group("version")
        if version in seen:
            raise MigrationError(
                f"duplicate migration version {version}: {seen[version].name} and {path.name}"
            )
        seen[version] = path
        migrations.append(
            Migration(
                version=version,
                name=match.group("name").replace("_", " "),
                path=path,
                sql=path.read_text(),
            )
        )
    return migrations


def _ledger_location(config: Config) -> str:
    return config.join(config.warehouse, LEDGER_TABLE)


def applied(spark: SparkSession, config: Config) -> dict[str, str]:
    """Return {version: checksum} for migrations already applied here."""
    from .io import location_exists

    location = _ledger_location(config)
    if not location_exists(spark, location):
        return {}
    rows = spark.read.parquet(location).select("version", "checksum").collect()
    return {row["version"]: row["checksum"] for row in rows}


def _record(spark: SparkSession, config: Config, migration: Migration, count: int) -> None:
    row = [
        (
            migration.version,
            migration.name,
            migration.checksum,
            config.env,
            count,
            datetime.now(timezone.utc),
        )
    ]
    columns = ["version", "name", "checksum", "env", "statements", "applied_at"]
    spark.createDataFrame(row, columns).write.mode("append").parquet(_ledger_location(config))


def plan(spark: SparkSession, config: Config, ddl_dir: Path | None = None) -> list[Migration]:
    """Work out what is pending, refusing to run if history has been rewritten."""
    history = applied(spark, config)
    pending: list[Migration] = []

    for migration in discover(ddl_dir):
        recorded = history.get(migration.version)
        if recorded is None:
            pending.append(migration)
        elif recorded != migration.checksum:
            raise MigrationError(
                f"V{migration.version} has already been applied to '{config.env}' but its "
                f"contents changed (recorded {recorded}, found {migration.checksum}). "
                "Migrations are immutable once applied - add a new one instead."
            )
    return pending


def migrate(
    spark: SparkSession,
    config: Config,
    dry_run: bool = False,
    ddl_dir: Path | None = None,
) -> list[dict]:
    """Apply every pending migration in version order."""
    pending = plan(spark, config, ddl_dir)
    if not pending:
        log.info("catalog '%s' is up to date; nothing pending", config.catalog["database"])
        return []

    results = []
    for migration in pending:
        statements = migration.statements(config)
        log.info(
            "%s V%s %s (%d statement(s))",
            "would apply" if dry_run else "applying",
            migration.version,
            migration.name,
            len(statements),
        )
        if not dry_run:
            for statement in statements:
                spark.sql(statement)
            _record(spark, config, migration, len(statements))
        results.append(
            {
                "version": migration.version,
                "name": migration.name,
                "statements": len(statements),
                "applied": not dry_run,
            }
        )
    return results


def repair(spark: SparkSession, config: Config) -> list[str]:
    """Re-discover partitions for every registered table.

    External Parquet tables do not learn about partitions the pipeline wrote
    after the table was created, so this runs once the data is in place.
    """
    database = config.catalog["database"]
    if not spark.catalog.databaseExists(database):
        log.warning("database %s does not exist yet; run migrate first", database)
        return []

    repaired = []
    for table in spark.catalog.listTables(database):
        qualified = f"{database}.{table.name}"
        try:
            spark.sql(f"MSCK REPAIR TABLE {qualified}")
            repaired.append(qualified)
        except Exception as exc:  # noqa: BLE001 - unpartitioned tables are expected
            log.debug("skipping %s: %s", qualified, exc)
    log.info("recovered partitions for %d table(s)", len(repaired))
    return repaired
