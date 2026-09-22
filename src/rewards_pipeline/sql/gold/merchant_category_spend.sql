-- Gold: where the money goes, month over month. The window aggregate over a
-- grouped aggregate gives each category its share of the month without a
-- second pass over the fact table.
SELECT
    month,
    category,
    COUNT(*)                           AS txn_count,
    COUNT(DISTINCT member_id)          AS member_count,
    COUNT(DISTINCT merchant_id)        AS merchant_count,
    ROUND(SUM(amount_usd), 2)          AS spend_usd,
    ROUND(AVG(amount_usd), 2)          AS avg_ticket_usd,
    ROUND(SUM(points_earned), 2)       AS points_earned,
    ROUND(SUM(reward_value_usd), 2)    AS reward_value_usd,
    ROUND(100.0 * SUM(reward_value_usd) / NULLIF(SUM(amount_usd), 0), 3) AS effective_return_pct,
    ROUND(
        100.0 * SUM(amount_usd) / SUM(SUM(amount_usd)) OVER (PARTITION BY month), 2
    ) AS pct_of_month_spend,
    RANK() OVER (PARTITION BY month ORDER BY SUM(amount_usd) DESC) AS spend_rank
FROM gold_transaction_rewards
WHERE category IS NOT NULL
GROUP BY month, category
