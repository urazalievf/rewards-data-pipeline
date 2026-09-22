-- Silver: slowly changing dimension (type 2) over daily card snapshots.
--
-- The feed sends a full snapshot every day, so versioning is a pure window
-- problem: hash the tracked attributes, mark the rows where the hash changes,
-- running-sum those marks into a version number, then collapse each version to
-- one row with a validity interval. No merge, no statefulness, fully
-- recomputable from history.
WITH snapshots AS (
    SELECT
        card_id,
        TRIM(product_name)            AS product_name,
        TRIM(issuer)                  AS issuer,
        LOWER(TRIM(network))          AS network,
        CAST(annual_fee_usd AS DOUBLE)    AS annual_fee_usd,
        CAST(base_earn_rate AS DOUBLE)    AS base_earn_rate,
        CAST(point_value_cents AS DOUBLE) AS point_value_cents,
        snapshot_date
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.card_id, b.snapshot_date ORDER BY b._ingested_at DESC
            ) AS _rank
        FROM bronze_cards b
        WHERE b.card_id IS NOT NULL AND b.snapshot_date IS NOT NULL
    )
    WHERE _rank = 1
),
hashed AS (
    SELECT
        s.*,
        MD5(CONCAT_WS('||',
            COALESCE(product_name, ''),
            COALESCE(issuer, ''),
            COALESCE(network, ''),
            COALESCE(CAST(annual_fee_usd AS STRING), ''),
            COALESCE(CAST(base_earn_rate AS STRING), ''),
            COALESCE(CAST(point_value_cents AS STRING), '')
        )) AS attr_hash
    FROM snapshots s
),
flagged AS (
    SELECT
        h.*,
        CASE
            WHEN LAG(attr_hash) OVER (PARTITION BY card_id ORDER BY snapshot_date) IS NULL THEN 1
            WHEN LAG(attr_hash) OVER (PARTITION BY card_id ORDER BY snapshot_date) <> attr_hash THEN 1
            ELSE 0
        END AS is_change
    FROM hashed h
),
versioned AS (
    SELECT
        f.*,
        SUM(is_change) OVER (
            PARTITION BY card_id ORDER BY snapshot_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS version_no
    FROM flagged f
),
collapsed AS (
    -- Tracked attributes are constant inside a version, so MAX() just picks it.
    SELECT
        card_id,
        version_no,
        MAX(product_name)      AS product_name,
        MAX(issuer)            AS issuer,
        MAX(network)           AS network,
        MAX(annual_fee_usd)    AS annual_fee_usd,
        MAX(base_earn_rate)    AS base_earn_rate,
        MAX(point_value_cents) AS point_value_cents,
        MAX(attr_hash)         AS attr_hash,
        MIN(snapshot_date)     AS valid_from,
        MAX(snapshot_date)     AS last_seen_date
    FROM versioned
    GROUP BY card_id, version_no
),
bounded AS (
    SELECT
        c.*,
        LEAD(valid_from) OVER (PARTITION BY card_id ORDER BY version_no) AS next_valid_from
    FROM collapsed c
)
SELECT
    MD5(CONCAT_WS('||', card_id, CAST(valid_from AS STRING))) AS card_sk,
    card_id,
    product_name,
    issuer,
    network,
    annual_fee_usd,
    base_earn_rate,
    point_value_cents,
    version_no,
    attr_hash,
    valid_from,
    COALESCE(DATE_SUB(next_valid_from, 1), DATE('9999-12-31')) AS valid_to,
    (next_valid_from IS NULL) AS is_current,
    last_seen_date
FROM bounded
