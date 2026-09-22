"""Tests for versioned DDL migrations."""

from __future__ import annotations

from dataclasses import replace

import pytest

from rewards_pipeline import migrations
from rewards_pipeline.migrations import MigrationError, split_statements


# --------------------------------------------------------------------------
# Statement splitting (no JVM needed)
# --------------------------------------------------------------------------
def test_split_respects_semicolons_inside_literals():
    sql = "CREATE TABLE a (x INT) COMMENT 'SCD2; filter is_current'; CREATE TABLE b (y INT)"
    statements = split_statements(sql)
    assert len(statements) == 2
    assert "SCD2; filter is_current" in statements[0]


def test_split_strips_line_comments():
    sql = "-- a comment; with a semicolon\nCREATE TABLE a (x INT)"
    assert split_statements(sql) == ["CREATE TABLE a (x INT)"]


def test_split_keeps_double_quote_escapes():
    sql = "CREATE TABLE a (x INT) COMMENT 'it''s fine'"
    assert split_statements(sql) == ["CREATE TABLE a (x INT) COMMENT 'it''s fine'"]


def test_split_ignores_trailing_semicolon():
    assert len(split_statements("SELECT 1; SELECT 2;")) == 2


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------
def _write(directory, name, body="CREATE DATABASE IF NOT EXISTS {database}"):
    path = directory / name
    path.write_text(body)
    return path


def test_discovery_orders_by_version(tmp_path):
    _write(tmp_path, "V002__second.sql")
    _write(tmp_path, "V001__first.sql")
    assert [m.version for m in migrations.discover(tmp_path)] == ["001", "002"]


def test_badly_named_file_is_rejected(tmp_path):
    _write(tmp_path, "create_stuff.sql")
    with pytest.raises(MigrationError, match="does not match"):
        migrations.discover(tmp_path)


def test_duplicate_version_is_rejected(tmp_path):
    _write(tmp_path, "V001__first.sql")
    _write(tmp_path, "V001__also_first.sql")
    with pytest.raises(MigrationError, match="duplicate migration version"):
        migrations.discover(tmp_path)


def test_repo_ddl_is_discoverable_and_well_formed():
    """The migrations actually shipped must parse."""
    found = migrations.discover()
    assert found, "expected migrations in ddl/"
    assert [m.version for m in found] == sorted(m.version for m in found)


# --------------------------------------------------------------------------
# Applying (JVM-backed)
# --------------------------------------------------------------------------
@pytest.fixture
def migration_config(config, request):
    database = f"t_{abs(hash(request.node.name)) % 10**8}"
    return replace(config, catalog={"enabled": True, "database": database})


@pytest.fixture
def ddl(tmp_path):
    directory = tmp_path / "ddl"
    directory.mkdir()
    _write(directory, "V001__create_db.sql", "CREATE DATABASE IF NOT EXISTS {database}")
    return directory


@pytest.mark.slow
def test_dry_run_changes_nothing(spark, migration_config, ddl):
    result = migrations.migrate(spark, migration_config, dry_run=True, ddl_dir=ddl)
    assert [item["applied"] for item in result] == [False]
    assert migrations.applied(spark, migration_config) == {}


@pytest.mark.slow
def test_apply_then_reapply_is_idempotent(spark, migration_config, ddl):
    first = migrations.migrate(spark, migration_config, ddl_dir=ddl)
    assert [item["version"] for item in first] == ["001"]
    assert list(migrations.applied(spark, migration_config)) == ["001"]

    second = migrations.migrate(spark, migration_config, ddl_dir=ddl)
    assert second == [], "an applied migration must not run twice"


@pytest.mark.slow
def test_only_pending_migrations_are_applied(spark, migration_config, ddl):
    migrations.migrate(spark, migration_config, ddl_dir=ddl)
    _write(ddl, "V002__second.sql", "CREATE DATABASE IF NOT EXISTS {database}")
    result = migrations.migrate(spark, migration_config, ddl_dir=ddl)
    assert [item["version"] for item in result] == ["002"]


@pytest.mark.slow
def test_editing_an_applied_migration_is_rejected(spark, migration_config, ddl):
    """The check that stops history being rewritten under a live warehouse."""
    migrations.migrate(spark, migration_config, ddl_dir=ddl)
    (ddl / "V001__create_db.sql").write_text(
        "CREATE DATABASE IF NOT EXISTS {database} COMMENT 'edited after the fact'"
    )
    with pytest.raises(MigrationError, match="contents changed"):
        migrations.plan(spark, migration_config, ddl_dir=ddl)


@pytest.mark.slow
def test_ledger_records_environment_and_checksum(spark, migration_config, ddl):
    migrations.migrate(spark, migration_config, ddl_dir=ddl)
    ledger = spark.read.parquet(
        migration_config.join(migration_config.warehouse, "_migrations")
    ).collect()
    assert len(ledger) == 1
    assert ledger[0]["env"] == "test"
    assert len(ledger[0]["checksum"]) == 16
