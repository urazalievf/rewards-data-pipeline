-- Gold: one row per member — the serving table behind a member-facing view.
WITH per_member AS (
    SELECT
        member_id,
        COUNT(*)                        AS txn_count,
        COUNT(DISTINCT card_id)         AS card_count,
        COUNT(DISTINCT month)           AS active_months,
        ROUND(SUM(amount_usd), 2)       AS lifetime_spend_usd,
        ROUND(SUM(points_earned), 2)    AS lifetime_points,
        ROUND(SUM(reward_value_usd), 2) AS lifetime_reward_usd,
        MIN(txn_date)                   AS first_txn_date,
        MAX(txn_date)                   AS last_txn_date
    FROM gold_transaction_rewards
    GROUP BY member_id
),
top_category AS (
    SELECT member_id, category AS top_category, spend_usd AS top_category_spend_usd
    FROM (
        SELECT
            member_id,
            category,
            SUM(amount_usd) AS spend_usd,
            ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY SUM(amount_usd) DESC) AS _rank
        FROM gold_transaction_rewards
        WHERE category IS NOT NULL
        GROUP BY member_id, category
    )
    WHERE _rank = 1
),
best_card AS (
    SELECT member_id, card_id AS best_card_id, product_name AS best_card_product, net_value_usd
    FROM (
        SELECT
            member_id,
            card_id,
            product_name,
            SUM(net_value_usd) AS net_value_usd,
            ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY SUM(net_value_usd) DESC) AS _rank
        FROM gold_card_roi_monthly
        GROUP BY member_id, card_id, product_name
    )
    WHERE _rank = 1
)
SELECT
    p.*,
    ROUND(100.0 * p.lifetime_reward_usd / NULLIF(p.lifetime_spend_usd, 0), 3) AS effective_return_pct,
    t.top_category,
    ROUND(t.top_category_spend_usd, 2) AS top_category_spend_usd,
    b.best_card_id,
    b.best_card_product,
    ROUND(b.net_value_usd, 2) AS best_card_net_value_usd,
    CASE
        WHEN p.lifetime_spend_usd >= 20000 THEN 'platinum'
        WHEN p.lifetime_spend_usd >= 8000  THEN 'gold'
        WHEN p.lifetime_spend_usd >= 2000  THEN 'silver'
        ELSE 'bronze'
    END AS value_tier
FROM per_member p
LEFT JOIN top_category t ON t.member_id = p.member_id
LEFT JOIN best_card b    ON b.member_id = p.member_id
