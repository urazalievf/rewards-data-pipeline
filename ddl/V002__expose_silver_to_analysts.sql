-- Expose the conformed silver layer to analysts.
--
-- GENERATED from schemas/silver/*.yml by `rewards schema render`.
--
-- The SCD2 card dimension answers "what was this card's fee in March", which
-- the gold marts deliberately flatten away. Silver tables are prefixed rather
-- than placed in their own database so a single GRANT on {database} covers
-- everything an analyst may read.

CREATE TABLE IF NOT EXISTS {database}.silver_transactions (
    txn_id STRING NOT NULL,
    member_id STRING NOT NULL,
    card_id STRING NOT NULL,
    merchant_id STRING,
    mcc STRING COMMENT 'Four-digit merchant category code, zero padded',
    amount DOUBLE COMMENT 'Amount in the original currency',
    currency STRING COMMENT 'ISO currency code, uppercased',
    usd_rate DOUBLE COMMENT 'FX rate used, as published for the transaction date',
    amount_usd DOUBLE COMMENT 'Signed USD amount; negative for refunds',
    txn_ts TIMESTAMP,
    txn_date DATE,
    status STRING,
    is_refund BOOLEAN,
    channel STRING,
    source_system STRING COMMENT 'Which upstream system delivered the row',
    _batch_id STRING COMMENT 'Lineage - ingest batch that produced this row',
    _ingested_at TIMESTAMP COMMENT 'Lineage - when bronze received it',
    ingest_date DATE NOT NULL COMMENT 'Partition column'
)
USING parquet
PARTITIONED BY (ingest_date)
LOCATION '{warehouse}/silver/transactions'
COMMENT 'Deduplicated, USD-normalised transactions. Refunds carry a negative amount_usd so any SUM() nets out correctly without a CASE.'
TBLPROPERTIES ('owner_team' = 'data-engineering', 'grain' = 'txn_id', 'pii_columns' = 'member_id');

CREATE TABLE IF NOT EXISTS {database}.silver_cards (
    card_sk STRING NOT NULL COMMENT 'Surrogate key, md5(card_id, valid_from)',
    card_id STRING NOT NULL COMMENT 'Natural key',
    product_name STRING,
    issuer STRING,
    network STRING,
    annual_fee_usd DOUBLE,
    base_earn_rate DOUBLE COMMENT 'Points per USD before any bonus rule',
    point_value_cents DOUBLE COMMENT 'Assumed redemption value of one point',
    version_no BIGINT COMMENT '1-based version of this card',
    attr_hash STRING COMMENT 'Hash of tracked attributes; a change opens a version',
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL COMMENT '9999-12-31 while current',
    is_current BOOLEAN NOT NULL COMMENT 'Exactly one true row per card_id',
    last_seen_date DATE COMMENT 'Most recent snapshot carrying this version'
)
USING parquet
LOCATION '{warehouse}/silver/cards'
COMMENT 'Slowly changing dimension (type 2) over daily card snapshots. Filter is_current for the live view, or join on valid_from/valid_to to see the card as it was on a given date.'
TBLPROPERTIES ('owner_team' = 'data-engineering', 'grain' = 'card_sk');

