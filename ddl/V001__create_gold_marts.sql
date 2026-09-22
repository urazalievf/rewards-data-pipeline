-- Register the gold marts as external tables over the warehouse.
--
-- GENERATED from schemas/gold/*.yml by `rewards schema render` and committed.
-- Do not hand-edit: change the contract, render a NEW migration. An applied
-- migration is immutable and its checksum is recorded in the warehouse.
--
-- External, not managed: the pipeline owns the files and the catalog owns the
-- contract, so dropping a table here never deletes data. {database} and
-- {warehouse} are substituted at apply time, so one file targets every
-- environment.

CREATE DATABASE IF NOT EXISTS {database}
    COMMENT 'Card rewards analytics marts';

CREATE TABLE IF NOT EXISTS {database}.transaction_rewards (
    txn_id STRING NOT NULL COMMENT 'Source transaction identifier',
    member_id STRING NOT NULL,
    card_id STRING NOT NULL,
    card_sk STRING COMMENT 'Surrogate key of the SCD2 card version applied',
    product_name STRING,
    issuer STRING,
    network STRING,
    merchant_id STRING,
    merchant_name STRING,
    category STRING COMMENT 'Merchant category driving rule matching',
    mcc STRING COMMENT 'Four-digit merchant category code',
    country STRING,
    channel STRING,
    txn_ts TIMESTAMP COMMENT 'Transaction time, UTC',
    txn_date DATE,
    amount_usd DOUBLE COMMENT 'Amount converted at the transaction-date FX rate',
    rule_id STRING COMMENT 'Winning earn rule, or BASE_EARN when none matched',
    base_earn_rate DOUBLE,
    applied_multiplier DOUBLE COMMENT 'Multiplier actually applied',
    bonus_eligible_usd DOUBLE COMMENT 'Portion of the amount still under the monthly cap',
    points_earned DOUBLE,
    point_value_cents DOUBLE,
    reward_value_usd DOUBLE,
    annual_fee_usd DOUBLE,
    month STRING NOT NULL COMMENT 'Partition column, yyyy-MM'
)
USING parquet
PARTITIONED BY (month)
LOCATION '{warehouse}/gold/transaction_rewards'
COMMENT 'Points earned per settled transaction, after joining the card version in force on the transaction date and applying monthly bonus caps.'
TBLPROPERTIES ('owner_team' = 'data-engineering', 'grain' = 'txn_id', 'pii_columns' = 'member_id');

CREATE TABLE IF NOT EXISTS {database}.card_roi_monthly (
    member_id STRING NOT NULL COMMENT 'Cardholder identifier',
    card_id STRING NOT NULL COMMENT 'Natural key of the card product',
    product_name STRING COMMENT 'Card product name as of the transactions in scope',
    issuer STRING COMMENT 'Issuing bank',
    month STRING NOT NULL COMMENT 'Calendar month, yyyy-MM',
    txn_count BIGINT COMMENT 'Settled transactions in the month',
    merchant_count BIGINT COMMENT 'Distinct merchants transacted with',
    spend_usd DOUBLE COMMENT 'Settled spend in USD, refunds excluded',
    refund_usd DOUBLE COMMENT 'Refunds in USD, always negative or zero',
    net_spend_usd DOUBLE COMMENT 'spend_usd + refund_usd',
    bonus_category_spend_usd DOUBLE COMMENT 'Spend that matched a bonus earn rule',
    points_earned DOUBLE COMMENT 'Points earned, base plus bonus uplift',
    reward_value_usd DOUBLE COMMENT 'Points valued at the card\'s point_value_cents',
    annual_fee_usd DOUBLE COMMENT 'Annual fee of the current card version',
    monthly_fee_usd DOUBLE COMMENT 'annual_fee_usd amortised over twelve months',
    net_value_usd DOUBLE COMMENT 'reward_value_usd - monthly_fee_usd',
    roi_ratio DOUBLE COMMENT 'Rewards per dollar of fee; null for no-fee cards',
    effective_return_pct DOUBLE COMMENT 'Reward value as a percentage of spend',
    bonus_spend_share_pct DOUBLE COMMENT 'Share of spend in bonus categories',
    fee_verdict STRING COMMENT 'Whether the fee paid for itself',
    month_value_rank INT COMMENT 'Rank by net value within the month'
)
USING parquet
LOCATION '{warehouse}/gold/card_roi_monthly'
COMMENT 'Per member, card and month: spend, rewards earned, the amortised annual fee and the resulting net value. The mart the product question is asked of.'
TBLPROPERTIES ('owner_team' = 'data-engineering', 'grain' = 'member_id, card_id, month', 'pii_columns' = 'member_id');

CREATE TABLE IF NOT EXISTS {database}.merchant_category_spend (
    month STRING NOT NULL COMMENT 'Calendar month, yyyy-MM',
    category STRING NOT NULL,
    txn_count BIGINT,
    member_count BIGINT COMMENT 'Distinct members transacting in the category',
    merchant_count BIGINT,
    spend_usd DOUBLE,
    avg_ticket_usd DOUBLE,
    points_earned DOUBLE,
    reward_value_usd DOUBLE,
    effective_return_pct DOUBLE,
    pct_of_month_spend DOUBLE COMMENT 'Share of the month\'s total spend',
    spend_rank INT COMMENT 'Rank by spend within the month'
)
USING parquet
LOCATION '{warehouse}/gold/merchant_category_spend'
COMMENT 'Category spend and effective return by month, with each category\'s share of the month.'
TBLPROPERTIES ('owner_team' = 'data-engineering', 'grain' = 'month, category');

CREATE TABLE IF NOT EXISTS {database}.member_rewards_summary (
    member_id STRING NOT NULL,
    txn_count BIGINT,
    card_count BIGINT COMMENT 'Distinct cards the member transacted on',
    active_months BIGINT,
    lifetime_spend_usd DOUBLE,
    lifetime_points DOUBLE,
    lifetime_reward_usd DOUBLE,
    first_txn_date DATE,
    last_txn_date DATE,
    effective_return_pct DOUBLE,
    top_category STRING,
    top_category_spend_usd DOUBLE,
    best_card_id STRING,
    best_card_product STRING,
    best_card_net_value_usd DOUBLE,
    value_tier STRING
)
USING parquet
LOCATION '{warehouse}/gold/member_rewards_summary'
COMMENT 'One row per member - lifetime spend, rewards, top category, best card and value tier.'
TBLPROPERTIES ('owner_team' = 'data-engineering', 'grain' = 'member_id', 'pii_columns' = 'member_id');

