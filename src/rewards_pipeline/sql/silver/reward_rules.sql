-- Silver: earn rules, typed and bounded. Open-ended rules keep a NULL end date
-- so the as-of join in gold can treat them as "still in force".
SELECT
    rule_id,
    card_id,
    LOWER(TRIM(category)) AS category,
    CAST(multiplier AS DOUBLE) AS multiplier,
    CAST(monthly_cap_usd AS DOUBLE) AS monthly_cap_usd,
    CAST(effective_from AS DATE) AS effective_from,
    CAST(effective_to AS DATE) AS effective_to
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.rule_id ORDER BY b._ingested_at DESC) AS _rank
    FROM bronze_reward_rules b
    WHERE b.rule_id IS NOT NULL AND b.card_id IS NOT NULL
)
WHERE _rank = 1
