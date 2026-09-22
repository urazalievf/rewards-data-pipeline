"""Explicit source schemas.

Schema inference is convenient and wrong: it re-reads the data, drifts between
runs and silently retypes columns when a batch happens to be all-integers.
Every landing source is read with a declared schema plus a `_corrupt_record`
column so malformed rows are captured instead of dropped.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CORRUPT_COLUMN = "_corrupt_record"

TRANSACTIONS = StructType(
    [
        StructField("txn_id", StringType(), nullable=False),
        StructField("member_id", StringType(), nullable=False),
        StructField("card_id", StringType(), nullable=False),
        StructField("merchant_id", StringType(), nullable=True),
        StructField("mcc", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("txn_ts", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("is_refund", BooleanType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
        StructField("source_system", StringType(), nullable=True),
        StructField(CORRUPT_COLUMN, StringType(), nullable=True),
    ]
)

CARDS = StructType(
    [
        StructField("card_id", StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=True),
        StructField("issuer", StringType(), nullable=True),
        StructField("network", StringType(), nullable=True),
        StructField("annual_fee_usd", DoubleType(), nullable=True),
        StructField("base_earn_rate", DoubleType(), nullable=True),
        StructField("point_value_cents", DoubleType(), nullable=True),
        StructField("snapshot_date", DateType(), nullable=True),
    ]
)

MERCHANTS = StructType(
    [
        StructField("merchant_id", StringType(), nullable=False),
        StructField("merchant_name", StringType(), nullable=True),
        StructField("mcc", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
    ]
)

REWARD_RULES = StructType(
    [
        StructField("rule_id", StringType(), nullable=False),
        StructField("card_id", StringType(), nullable=False),
        StructField("category", StringType(), nullable=True),
        StructField("multiplier", DoubleType(), nullable=True),
        StructField("monthly_cap_usd", DoubleType(), nullable=True),
        StructField("effective_from", StringType(), nullable=True),
        StructField("effective_to", StringType(), nullable=True),
        StructField(CORRUPT_COLUMN, StringType(), nullable=True),
    ]
)

FX_RATES = StructType(
    [
        StructField("rate_date", DateType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("usd_rate", DoubleType(), nullable=True),
    ]
)

SOURCE_SCHEMAS: dict[str, StructType] = {
    "transactions": TRANSACTIONS,
    "cards": CARDS,
    "merchants": MERCHANTS,
    "reward_rules": REWARD_RULES,
    "fx_rates": FX_RATES,
}

# Natural keys used for deduplication and SCD2 change detection.
NATURAL_KEYS: dict[str, list[str]] = {
    "transactions": ["txn_id"],
    "cards": ["card_id"],
    "merchants": ["merchant_id"],
    "reward_rules": ["rule_id"],
    "fx_rates": ["rate_date", "currency"],
}

# Columns that, when changed, open a new SCD2 version of a card.
SCD2_TRACKED_COLUMNS = [
    "product_name",
    "issuer",
    "network",
    "annual_fee_usd",
    "base_earn_rate",
    "point_value_cents",
]

INTEGER_FIELDS = (IntegerType, TimestampType)  # re-exported for tests
