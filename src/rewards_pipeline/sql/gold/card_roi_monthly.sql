-- Gold: the mart the product actually asks for — is this card worth its fee
-- for this member this month? Fees are amortised monthly so a 695/yr card is
-- compared against the rewards it produced in the same period.
WITH earned AS (
    SELECT
        member_id,
        card_id,
        month,
        MAX(product_name)        AS product_name,
        MAX(issuer)              AS issuer,
        MAX(point_value_cents)   AS point_value_cents,
        MAX(annual_fee_usd)      AS annual_fee_usd,
        COUNT(*)                 AS txn_count,
        COUNT(DISTINCT merchant_id) AS merchant_count,
        SUM(amount_usd)          AS spend_usd,
        SUM(points_earned)       AS points_earned,
        SUM(reward_value_usd)    AS reward_value_usd,
        SUM(CASE WHEN rule_id <> 'BASE_EARN' THEN amount_usd ELSE 0 END) AS bonus_category_spend_usd
    FROM gold_transaction_rewards
    GROUP BY member_id, card_id, month
),
refunded AS (
    SELECT
        member_id,
        card_id,
        DATE_FORMAT(txn_ts, 'yyyy-MM') AS month,
        SUM(amount_usd) AS refund_usd          -- already negative in silver
    FROM silver_transactions
    WHERE status = 'refunded'
    GROUP BY member_id, card_id, DATE_FORMAT(txn_ts, 'yyyy-MM')
),
combined AS (
    SELECT
        e.member_id,
        e.card_id,
        e.product_name,
        e.issuer,
        e.month,
        e.txn_count,
        e.merchant_count,
        ROUND(e.spend_usd, 2)                              AS spend_usd,
        ROUND(COALESCE(r.refund_usd, 0.0), 2)              AS refund_usd,
        ROUND(e.spend_usd + COALESCE(r.refund_usd, 0.0), 2) AS net_spend_usd,
        ROUND(e.bonus_category_spend_usd, 2)               AS bonus_category_spend_usd,
        ROUND(e.points_earned, 2)                          AS points_earned,
        ROUND(e.reward_value_usd, 2)                       AS reward_value_usd,
        COALESCE(e.annual_fee_usd, 0.0)                    AS annual_fee_usd,
        ROUND(COALESCE(e.annual_fee_usd, 0.0) / 12.0, 2)   AS monthly_fee_usd
    FROM earned e
    LEFT JOIN refunded r
        ON r.member_id = e.member_id AND r.card_id = e.card_id AND r.month = e.month
)
SELECT
    c.*,
    ROUND(c.reward_value_usd - c.monthly_fee_usd, 2) AS net_value_usd,
    CASE
        WHEN c.monthly_fee_usd > 0 THEN ROUND(c.reward_value_usd / c.monthly_fee_usd, 3)
    END AS roi_ratio,
    ROUND(100.0 * c.reward_value_usd / NULLIF(c.spend_usd, 0), 3) AS effective_return_pct,
    ROUND(100.0 * c.bonus_category_spend_usd / NULLIF(c.spend_usd, 0), 2) AS bonus_spend_share_pct,
    CASE
        WHEN c.reward_value_usd - c.monthly_fee_usd >= 0 THEN 'keep'
        WHEN c.monthly_fee_usd = 0 THEN 'keep'
        ELSE 'review'
    END AS fee_verdict,
    RANK() OVER (
        PARTITION BY c.month ORDER BY c.reward_value_usd - c.monthly_fee_usd DESC
    ) AS month_value_rank
FROM combined c
