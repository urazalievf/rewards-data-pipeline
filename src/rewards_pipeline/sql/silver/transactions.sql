-- Silver: conformed transaction fact.
--
-- Three jobs: collapse the duplicates the source system re-delivers, convert
-- every amount to USD at the transaction-date rate, and tag rows that cannot
-- be trusted downstream. Rejects are flagged rather than deleted here; the job
-- routes them to quarantine so they stay auditable.
WITH deduped AS (
    SELECT *
    FROM (
        SELECT
            t.*,
            ROW_NUMBER() OVER (
                PARTITION BY t.txn_id
                ORDER BY t._ingested_at DESC, t._source_file DESC
            ) AS _rank
        FROM bronze_transactions t
    )
    WHERE _rank = 1
),
typed AS (
    SELECT
        txn_id,
        member_id,
        card_id,
        merchant_id,
        LPAD(TRIM(mcc), 4, '0')        AS mcc,
        CAST(amount AS DOUBLE)         AS amount,
        UPPER(TRIM(currency))          AS currency,
        CAST(txn_ts AS TIMESTAMP)      AS txn_ts,
        LOWER(TRIM(status))            AS status,
        COALESCE(is_refund, FALSE)     AS is_refund,
        LOWER(TRIM(channel))           AS channel,
        source_system,
        ingest_date,
        _batch_id,
        _ingested_at
    FROM deduped
),
converted AS (
    SELECT
        t.*,
        CAST(t.txn_ts AS DATE) AS txn_date,
        fx.usd_rate,
        -- Refunds carry a negative sign so any SUM() nets out correctly.
        ROUND(t.amount * fx.usd_rate * CASE WHEN t.is_refund THEN -1 ELSE 1 END, 2) AS amount_usd
    FROM typed t
    LEFT JOIN silver_fx_rates fx
        ON fx.currency = t.currency
       AND fx.rate_date = CAST(t.txn_ts AS DATE)
),
validated AS (
    SELECT
        c.*,
        CASE
            WHEN c.txn_id IS NULL                       THEN 'missing_txn_id'
            WHEN c.member_id IS NULL OR c.card_id IS NULL THEN 'missing_entity_key'
            WHEN c.amount IS NULL                       THEN 'missing_amount'
            WHEN c.txn_ts IS NULL                       THEN 'unparseable_timestamp'
            WHEN c.usd_rate IS NULL                     THEN 'unknown_currency'
            WHEN c.status NOT IN ('settled', 'refunded') THEN 'unknown_status'
            ELSE NULL
        END AS _reject_reason
    FROM converted c
)
SELECT
    txn_id, member_id, card_id, merchant_id, mcc,
    amount, currency, usd_rate, amount_usd,
    txn_ts, txn_date, status, is_refund, channel, source_system,
    ingest_date, _batch_id, _ingested_at,
    _reject_reason,
    (_reject_reason IS NULL) AS _is_valid
FROM validated
