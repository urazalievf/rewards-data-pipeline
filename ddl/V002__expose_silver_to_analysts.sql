-- Analysts asked for the conformed layer as well as the marts: the SCD2 card
-- dimension answers "what was this card's fee in March", which the gold marts
-- deliberately flatten away.
--
-- Silver tables are prefixed rather than put in their own database so a single
-- GRANT on {database} covers everything an analyst is allowed to read.

CREATE TABLE IF NOT EXISTS {database}.silver_transactions
USING parquet
LOCATION '{warehouse}/silver/transactions'
COMMENT 'Deduplicated, USD-normalised transactions; refunds carry a negative amount';

CREATE TABLE IF NOT EXISTS {database}.silver_cards
USING parquet
LOCATION '{warehouse}/silver/cards'
COMMENT 'SCD type 2 card dimension; filter is_current for the live view';
