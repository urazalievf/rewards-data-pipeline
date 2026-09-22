-- Register the gold marts as external tables over the warehouse.
--
-- External, not managed: the pipeline owns the files and the catalog owns the
-- contract. Dropping a table here never deletes data.
--
-- {database} and {warehouse} are substituted at apply time, so the same file
-- targets local, uat and prod.

CREATE DATABASE IF NOT EXISTS {database}
    COMMENT 'Card rewards analytics marts';

CREATE TABLE IF NOT EXISTS {database}.transaction_rewards
USING parquet
LOCATION '{warehouse}/gold/transaction_rewards'
COMMENT 'Points earned per settled transaction, after as-of card join and monthly cap';

CREATE TABLE IF NOT EXISTS {database}.card_roi_monthly
USING parquet
LOCATION '{warehouse}/gold/card_roi_monthly'
COMMENT 'Per member, card and month: spend, rewards, amortised fee and net value';

CREATE TABLE IF NOT EXISTS {database}.merchant_category_spend
USING parquet
LOCATION '{warehouse}/gold/merchant_category_spend'
COMMENT 'Category spend and effective return by month, with share of month';

CREATE TABLE IF NOT EXISTS {database}.member_rewards_summary
USING parquet
LOCATION '{warehouse}/gold/member_rewards_summary'
COMMENT 'One row per member: lifetime spend, rewards, best card and value tier';
