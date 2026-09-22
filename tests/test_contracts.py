"""Tests for the declared table contracts."""

from __future__ import annotations

import textwrap
from dataclasses import replace

import pytest

from rewards_pipeline import contracts
from rewards_pipeline.contracts import ContractError


# --------------------------------------------------------------------------
# The contracts actually shipped
# --------------------------------------------------------------------------
def test_every_shipped_contract_parses():
    declared = contracts.load_all()
    assert declared, "expected contracts under schemas/"
    assert {c.qualified for c in declared} >= {
        "gold.card_roi_monthly",
        "gold.transaction_rewards",
        "silver.cards",
    }


def test_every_gold_table_has_a_contract(config):
    declared = {c.table for c in contracts.load_all() if c.layer == "gold"}
    assert set(config.tables("gold")) <= declared


def test_contracts_declare_a_grain_and_an_owner():
    for contract in contracts.load_all():
        assert contract.grain, f"{contract.qualified} has no grain"
        assert contract.owner != "unowned", f"{contract.qualified} has no owner"


def test_member_id_is_marked_pii_everywhere_it_appears():
    """A PII tag that is applied inconsistently is worse than none."""
    for contract in contracts.load_all():
        column = contract.column("member_id")
        if column is not None:
            assert column.pii, f"{contract.qualified}.member_id is not tagged pii"


# --------------------------------------------------------------------------
# Validation
# --------------------------------------------------------------------------
def _contract(**overrides):
    payload = {
        "table": "t",
        "layer": "gold",
        "owner": "team",
        "grain": ["a"],
        "columns": [{"name": "a", "type": "string"}],
    }
    payload.update(overrides)
    return payload


def test_missing_required_key_is_rejected():
    with pytest.raises(ContractError, match="missing"):
        contracts._parse({"table": "t"})


def test_duplicate_column_is_rejected():
    payload = _contract(columns=[{"name": "a", "type": "string"}, {"name": "a", "type": "int"}])
    with pytest.raises(ContractError, match="twice"):
        contracts._parse(payload)


def test_grain_on_undeclared_column_is_rejected():
    with pytest.raises(ContractError, match="grain on undeclared"):
        contracts._parse(_contract(grain=["nope"]))


def test_partition_on_undeclared_column_is_rejected():
    with pytest.raises(ContractError, match="partitions on undeclared"):
        contracts._parse(_contract(partitioned_by=["nope"]))


# --------------------------------------------------------------------------
# DDL rendering
# --------------------------------------------------------------------------
def test_render_keeps_placeholders_so_the_ddl_is_portable(config):
    ddl = contracts.render_ddl(contracts.load("gold.card_roi_monthly"), config)
    assert "{database}.card_roi_monthly" in ddl
    assert "LOCATION '{warehouse}/gold/card_roi_monthly'" in ddl
    assert str(config.warehouse) not in ddl, "a resolved path must not be committed"


def test_render_resolved_substitutes(config):
    ddl = contracts.render_ddl(contracts.load("gold.card_roi_monthly"), config, resolved=True)
    assert "{warehouse}" not in ddl
    assert config.warehouse in ddl


def test_apostrophes_use_backslash_not_doubled_quotes():
    """Spark parses 'month''s' as "months", silently losing the apostrophe."""
    assert contracts._escape("month's") == r"month\'s"
    assert contracts._escape(r"back\slash") == r"back\\slash"


def test_silver_tables_are_prefixed_in_the_catalog(config):
    ddl = contracts.render_ddl(contracts.load("silver.cards"), config)
    assert "{database}.silver_cards" in ddl


def test_not_null_and_partitioning_reach_the_ddl(config):
    ddl = contracts.render_ddl(contracts.load("gold.transaction_rewards"), config)
    assert "txn_id STRING NOT NULL" in ddl
    assert "PARTITIONED BY (month)" in ddl
    assert "'pii_columns' = 'member_id'" in ddl
    assert "'owner'" not in ddl, "owner is reserved by Spark; must be owner_team"


# --------------------------------------------------------------------------
# Drift detection
# --------------------------------------------------------------------------
@pytest.fixture
def one_contract(tmp_path):
    directory = tmp_path / "schemas" / "gold"
    directory.mkdir(parents=True)
    (directory / "demo.yml").write_text(
        textwrap.dedent(
            """
            table: demo
            layer: gold
            owner: team
            grain: [id]
            columns:
              - {name: id, type: string}
              - {name: amount, type: double}
            """
        )
    )
    return tmp_path / "schemas"


@pytest.fixture
def drift_config(config):
    layers = {**config.layers, "gold": {**config.layers["gold"], "tables": ["demo"]}}
    return replace(config, layers=layers)


@pytest.mark.slow
def test_unwritten_table_is_skipped_not_failed(spark, drift_config, one_contract):
    results = contracts.check(spark, drift_config, one_contract)
    assert len(results) == 1
    assert results[0].checked is False
    assert results[0].clean is True


@pytest.mark.slow
def test_matching_table_is_clean(spark, drift_config, one_contract):
    spark.createDataFrame([("a", 1.0)], ["id", "amount"]).write.parquet(
        drift_config.layer_path("gold", "demo")
    )
    results = contracts.check(spark, drift_config, one_contract)
    assert results[0].clean, results[0].render()


@pytest.mark.slow
def test_renamed_column_is_caught(spark, drift_config, one_contract):
    """The exact failure this exists to prevent."""
    spark.createDataFrame([("a", 1.0)], ["id", "gross_amount"]).write.parquet(
        drift_config.layer_path("gold", "demo")
    )
    drift = contracts.check(spark, drift_config, one_contract)[0]
    assert drift.missing == ["amount"]
    assert drift.undeclared == ["gross_amount"]
    assert not drift.clean


@pytest.mark.slow
def test_type_change_is_caught(spark, drift_config, one_contract):
    spark.createDataFrame([("a", 1)], ["id", "amount"]).write.parquet(
        drift_config.layer_path("gold", "demo")
    )
    drift = contracts.check(spark, drift_config, one_contract)[0]
    assert drift.type_mismatches == [("amount", "double", "bigint")]
