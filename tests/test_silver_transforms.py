"""Tests for the silver SQL: dedup + FX conversion, and the SCD2 build."""

from __future__ import annotations

from datetime import date, datetime

import pytest

pytestmark = pytest.mark.slow

FX_ROWS = [
    (date(2026, 9, 1), "USD", 1.0),
    (date(2026, 9, 1), "EUR", 1.10),
]
FX_COLUMNS = ["rate_date", "currency", "usd_rate"]


def _bronze_txn(spark, rows):
    columns = [
        "txn_id",
        "member_id",
        "card_id",
        "merchant_id",
        "mcc",
        "amount",
        "currency",
        "txn_ts",
        "status",
        "is_refund",
        "channel",
        "source_system",
        "ingest_date",
        "_batch_id",
        "_source_file",
        "_ingested_at",
    ]
    return spark.createDataFrame(rows, columns)


@pytest.fixture
def silver_transactions(spark, sql_runner):
    spark.createDataFrame(FX_ROWS, FX_COLUMNS).createOrReplaceTempView("silver_fx_rates")

    stamp = datetime(2026, 9, 1, 12, 0, 0)
    rows = [
        # Same txn delivered twice; the later ingest must win and only one row survives.
        (
            "T1",
            "M1",
            "C1",
            "R1",
            "5812",
            100.0,
            "USD",
            "2026-09-01T10:00:00",
            "settled",
            False,
            "online",
            "core_auth",
            date(2026, 9, 1),
            "b1",
            "f1",
            stamp,
        ),
        (
            "T1",
            "M1",
            "C1",
            "R1",
            "5812",
            100.0,
            "USD",
            "2026-09-01T10:00:00",
            "settled",
            False,
            "online",
            "core_auth",
            date(2026, 9, 1),
            "b2",
            "f2",
            datetime(2026, 9, 1, 13, 0, 0),
        ),
        # EUR converts at 1.10.
        (
            "T2",
            "M1",
            "C1",
            "R2",
            "3000",
            50.0,
            "EUR",
            "2026-09-01T11:00:00",
            "settled",
            False,
            "online",
            "core_auth",
            date(2026, 9, 1),
            "b1",
            "f1",
            stamp,
        ),
        # Refund carries a negative USD amount.
        (
            "T3",
            "M2",
            "C1",
            "R1",
            "5812",
            20.0,
            "USD",
            "2026-09-01T12:00:00",
            "refunded",
            True,
            "in_store",
            "batch_settle",
            date(2026, 9, 1),
            "b1",
            "f1",
            stamp,
        ),
        # No FX rate published for XBT -> rejected.
        (
            "T4",
            "M2",
            "C1",
            "R1",
            "5812",
            10.0,
            "XBT",
            "2026-09-01T13:00:00",
            "settled",
            False,
            "online",
            "core_auth",
            date(2026, 9, 1),
            "b1",
            "f1",
            stamp,
        ),
        # Missing amount -> rejected.
        (
            "T5",
            "M2",
            "C1",
            "R1",
            "5812",
            None,
            "USD",
            "2026-09-01T14:00:00",
            "settled",
            False,
            "online",
            "core_auth",
            date(2026, 9, 1),
            "b1",
            "f1",
            stamp,
        ),
    ]
    _bronze_txn(spark, rows).createOrReplaceTempView("bronze_transactions")
    return sql_runner("silver", "transactions").cache()


def test_duplicate_transactions_are_collapsed(silver_transactions):
    assert silver_transactions.filter("txn_id = 'T1'").count() == 1


def test_latest_ingest_wins(silver_transactions):
    row = silver_transactions.filter("txn_id = 'T1'").collect()[0]
    assert row["_batch_id"] == "b2"


def test_foreign_currency_converted_to_usd(silver_transactions):
    row = silver_transactions.filter("txn_id = 'T2'").collect()[0]
    assert row["amount_usd"] == pytest.approx(55.0)


def test_refunds_are_signed_negative(silver_transactions):
    row = silver_transactions.filter("txn_id = 'T3'").collect()[0]
    assert row["amount_usd"] == pytest.approx(-20.0)


def test_unknown_currency_is_rejected(silver_transactions):
    row = silver_transactions.filter("txn_id = 'T4'").collect()[0]
    assert row["_is_valid"] is False
    assert row["_reject_reason"] == "unknown_currency"


def test_missing_amount_is_rejected(silver_transactions):
    row = silver_transactions.filter("txn_id = 'T5'").collect()[0]
    assert row["_reject_reason"] == "missing_amount"


def test_valid_rows_are_flagged(silver_transactions):
    valid = {row["txn_id"] for row in silver_transactions.filter("_is_valid").collect()}
    assert valid == {"T1", "T2", "T3"}


# --------------------------------------------------------------------------
# SCD2
# --------------------------------------------------------------------------
CARD_COLUMNS = [
    "card_id",
    "product_name",
    "issuer",
    "network",
    "annual_fee_usd",
    "base_earn_rate",
    "point_value_cents",
    "snapshot_date",
    "_ingested_at",
]


@pytest.fixture
def silver_cards(spark, sql_runner):
    stamp = datetime(2026, 9, 5, 0, 0, 0)
    rows = [
        # Unchanged for two days, then the fee and earn rate move on the 3rd.
        ("C1", "Summit Reserve", "Harbor Trust", "visa", 250.0, 2.0, 1.5, date(2026, 9, 1), stamp),
        ("C1", "Summit Reserve", "Harbor Trust", "visa", 250.0, 2.0, 1.5, date(2026, 9, 2), stamp),
        ("C1", "Summit Reserve", "Harbor Trust", "visa", 300.0, 2.25, 1.5, date(2026, 9, 3), stamp),
        # Never changes -> exactly one version.
        ("C2", "Everyday Cash", "Northwind Bank", "visa", 0.0, 1.0, 1.0, date(2026, 9, 1), stamp),
        ("C2", "Everyday Cash", "Northwind Bank", "visa", 0.0, 1.0, 1.0, date(2026, 9, 3), stamp),
    ]
    spark.createDataFrame(rows, CARD_COLUMNS).createOrReplaceTempView("bronze_cards")
    return sql_runner("silver", "cards").cache()


def test_scd2_opens_a_version_per_change(silver_cards):
    assert silver_cards.filter("card_id = 'C1'").count() == 2
    assert silver_cards.filter("card_id = 'C2'").count() == 1


def test_scd2_has_exactly_one_current_row_per_card(silver_cards):
    current = silver_cards.filter("is_current").collect()
    assert sorted(row["card_id"] for row in current) == ["C1", "C2"]


def test_scd2_intervals_are_contiguous_and_closed(silver_cards):
    versions = sorted(
        silver_cards.filter("card_id = 'C1'").collect(), key=lambda row: row["version_no"]
    )
    first, second = versions
    assert first["valid_from"] == date(2026, 9, 1)
    assert first["valid_to"] == date(2026, 9, 2)  # day before the change
    assert second["valid_from"] == date(2026, 9, 3)
    assert second["valid_to"] == date(9999, 12, 31)
    assert first["is_current"] is False


def test_scd2_surrogate_keys_are_unique(silver_cards):
    keys = [row["card_sk"] for row in silver_cards.collect()]
    assert len(keys) == len(set(keys))


def test_scd2_captures_the_new_attribute_values(silver_cards):
    current = silver_cards.filter("card_id = 'C1' AND is_current").collect()[0]
    assert current["annual_fee_usd"] == pytest.approx(300.0)
    assert current["base_earn_rate"] == pytest.approx(2.25)
