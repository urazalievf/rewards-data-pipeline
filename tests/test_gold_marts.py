"""Tests for the gold SQL: as-of card join, best-rule selection, monthly caps."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

pytestmark = pytest.mark.slow

TXN_COLUMNS = [
    "txn_id",
    "member_id",
    "card_id",
    "merchant_id",
    "mcc",
    "amount",
    "currency",
    "usd_rate",
    "amount_usd",
    "txn_ts",
    "txn_date",
    "status",
    "is_refund",
    "channel",
    "source_system",
    "ingest_date",
    "_batch_id",
    "_ingested_at",
]
CARD_COLUMNS = [
    "card_sk",
    "card_id",
    "product_name",
    "issuer",
    "network",
    "annual_fee_usd",
    "base_earn_rate",
    "point_value_cents",
    "version_no",
    "attr_hash",
    "valid_from",
    "valid_to",
    "is_current",
    "last_seen_date",
]
MERCHANT_COLUMNS = ["merchant_id", "merchant_name", "mcc", "category", "country"]
# An explicit schema, because `effective_to` is entirely NULL in this fixture
# and Spark cannot infer a type from an all-NULL column.
RULE_SCHEMA = StructType(
    [
        StructField("rule_id", StringType()),
        StructField("card_id", StringType()),
        StructField("category", StringType()),
        StructField("multiplier", DoubleType()),
        StructField("monthly_cap_usd", DoubleType()),
        StructField("effective_from", DateType()),
        StructField("effective_to", DateType()),
    ]
)


def _txn(txn_id, member, card, merchant, amount_usd, day, hour=12, status="settled"):
    """Build a silver-shaped row. Silver signs refunds negative, so this
    helper does too - a positive refund would not exist downstream."""
    stamp = datetime(2026, 9, day, hour, 0, 0)
    if status == "refunded":
        amount_usd = -abs(amount_usd)
    return (
        txn_id,
        member,
        card,
        merchant,
        "5812",
        amount_usd,
        "USD",
        1.0,
        amount_usd,
        stamp,
        date(2026, 9, day),
        status,
        status == "refunded",
        "online",
        "core_auth",
        date(2026, 9, day),
        "b1",
        datetime(2026, 9, 20, 0, 0, 0),
    )


@pytest.fixture
def gold_rewards(spark, sql_runner):
    transactions = [
        _txn("T1", "M1", "C1", "R_DINING", 100.0, 1),
        _txn("T2", "M1", "C1", "R_DINING", 400.0, 2),
        _txn("T3", "M1", "C1", "R_DINING", 200.0, 3),  # crosses the 500 cap
        _txn("T4", "M1", "C1", "R_GAS", 100.0, 4),  # no bonus rule -> base earn
        _txn("T5", "M1", "C1", "R_DINING", 100.0, 20),  # after the fee change
        _txn("T6", "M1", "C1", "R_DINING", 50.0, 5, status="refunded"),
    ]
    spark.createDataFrame(transactions, TXN_COLUMNS).createOrReplaceTempView("silver_transactions")

    cards = [
        (
            "sk1",
            "C1",
            "Summit Reserve",
            "Harbor Trust",
            "visa",
            120.0,
            1.0,
            1.0,
            1,
            "h1",
            date(2026, 9, 1),
            date(2026, 9, 15),
            False,
            date(2026, 9, 15),
        ),
        (
            "sk2",
            "C1",
            "Summit Reserve",
            "Harbor Trust",
            "visa",
            240.0,
            2.0,
            1.0,
            2,
            "h2",
            date(2026, 9, 16),
            date(9999, 12, 31),
            True,
            date(2026, 9, 30),
        ),
    ]
    spark.createDataFrame(cards, CARD_COLUMNS).createOrReplaceTempView("silver_cards")

    merchants = [
        ("R_DINING", "Blue Cafe", "5812", "dining", "US"),
        ("R_GAS", "Iron Fuel", "5541", "gas", "US"),
    ]
    spark.createDataFrame(merchants, MERCHANT_COLUMNS).createOrReplaceTempView("silver_merchants")

    rules = [
        ("RULE1", "C1", "dining", 3.0, 500.0, date(2026, 1, 1), None),
        # A weaker overlapping rule must lose to RULE1.
        ("RULE2", "C1", "dining", 2.0, None, date(2026, 1, 1), None),
    ]
    spark.createDataFrame(rules, RULE_SCHEMA).createOrReplaceTempView("silver_reward_rules")

    return sql_runner("gold", "transaction_rewards").cache()


def _row(df, txn_id):
    return df.filter(f"txn_id = '{txn_id}'").collect()[0]


def test_refunds_do_not_earn_points(gold_rewards):
    assert gold_rewards.filter("txn_id = 'T6'").count() == 0


def test_one_row_per_transaction(gold_rewards):
    assert gold_rewards.count() == gold_rewards.select("txn_id").distinct().count()


def test_best_rule_wins_when_several_match(gold_rewards):
    assert _row(gold_rewards, "T1")["rule_id"] == "RULE1"
    assert _row(gold_rewards, "T1")["applied_multiplier"] == pytest.approx(3.0)


def test_base_earn_applies_without_a_bonus_rule(gold_rewards):
    row = _row(gold_rewards, "T4")
    assert row["rule_id"] == "BASE_EARN"
    assert row["bonus_eligible_usd"] == pytest.approx(0.0)
    assert row["points_earned"] == pytest.approx(100.0)  # 100 * base 1.0


def test_points_use_base_plus_bonus_uplift(gold_rewards):
    # 100 USD, base 1x, bonus 3x, fully under the cap -> 100 * 3 = 300.
    assert _row(gold_rewards, "T1")["points_earned"] == pytest.approx(300.0)


def test_monthly_cap_limits_the_bonus_portion(gold_rewards):
    # T1 (100) + T2 (400) exhaust the 500 cap, so T3 earns base rate only.
    assert _row(gold_rewards, "T3")["bonus_eligible_usd"] == pytest.approx(0.0)
    assert _row(gold_rewards, "T3")["points_earned"] == pytest.approx(200.0)


def test_transaction_at_the_cap_boundary_is_fully_eligible(gold_rewards):
    assert _row(gold_rewards, "T2")["bonus_eligible_usd"] == pytest.approx(400.0)


def test_card_is_joined_as_of_the_transaction_date(gold_rewards):
    # Before the change: base 1.0, fee 120. After: base 2.0, fee 240.
    assert _row(gold_rewards, "T1")["base_earn_rate"] == pytest.approx(1.0)
    assert _row(gold_rewards, "T1")["annual_fee_usd"] == pytest.approx(120.0)
    assert _row(gold_rewards, "T5")["base_earn_rate"] == pytest.approx(2.0)
    assert _row(gold_rewards, "T5")["annual_fee_usd"] == pytest.approx(240.0)


def test_reward_value_uses_point_value(gold_rewards):
    row = _row(gold_rewards, "T1")
    assert row["reward_value_usd"] == pytest.approx(row["points_earned"] * 1.0 / 100.0)


# --------------------------------------------------------------------------
# ROI mart
# --------------------------------------------------------------------------
@pytest.fixture
def roi(spark, sql_runner, gold_rewards):
    gold_rewards.createOrReplaceTempView("gold_transaction_rewards")
    return sql_runner("gold", "card_roi_monthly").cache()


def test_roi_has_one_row_per_member_card_month(roi):
    assert roi.count() == 1
    assert roi.collect()[0]["month"] == "2026-09"


def test_roi_nets_refunds_out_of_spend(roi):
    row = roi.collect()[0]
    assert row["spend_usd"] == pytest.approx(900.0)  # settled only
    assert row["refund_usd"] == pytest.approx(-50.0)
    assert row["net_spend_usd"] == pytest.approx(850.0)


def test_roi_amortises_the_annual_fee(roi):
    row = roi.collect()[0]
    assert row["monthly_fee_usd"] == pytest.approx(round(240.0 / 12.0, 2))
    assert row["net_value_usd"] == pytest.approx(
        round(row["reward_value_usd"] - row["monthly_fee_usd"], 2)
    )


def test_roi_emits_a_fee_verdict(roi):
    assert roi.collect()[0]["fee_verdict"] in {"keep", "review"}
