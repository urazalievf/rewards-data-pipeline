-- Gold: points earned per transaction.
--
-- The interesting parts are the two joins and the cap:
--   * cards are joined as-of the transaction date against the SCD2 interval,
--     so a fee change mid-month does not retroactively rewrite history;
--   * when several bonus rules match, the richest one wins (rule_rank);
--   * monthly caps are enforced with a running sum inside the
--     member/card/month/rule window, so the bonus multiplier stops applying
--     exactly at the cap instead of at the transaction that crosses it.
WITH txn AS (
    SELECT
        t.*,
        DATE_FORMAT(t.txn_ts, 'yyyy-MM') AS month
    FROM silver_transactions t
    WHERE t.status = 'settled'      -- refunds reverse spend, they do not earn
),
enriched AS (
    SELECT
        txn.*,
        m.merchant_name,
        m.category,
        m.country,
        c.card_sk,
        c.product_name,
        c.issuer,
        c.network,
        c.annual_fee_usd,
        COALESCE(c.base_earn_rate, 1.0)    AS base_earn_rate,
        COALESCE(c.point_value_cents, 1.0) AS point_value_cents
    FROM txn
    LEFT JOIN silver_merchants m
        ON m.merchant_id = txn.merchant_id
    LEFT JOIN silver_cards c
        ON c.card_id = txn.card_id
       AND txn.txn_date BETWEEN c.valid_from AND c.valid_to
),
rule_match AS (
    SELECT
        e.*,
        r.rule_id,
        r.multiplier,
        r.monthly_cap_usd,
        ROW_NUMBER() OVER (
            PARTITION BY e.txn_id
            ORDER BY r.multiplier DESC NULLS LAST, r.rule_id
        ) AS rule_rank
    FROM enriched e
    LEFT JOIN silver_reward_rules r
        ON r.card_id = e.card_id
       AND r.category = e.category
       AND e.txn_date >= r.effective_from
       AND (r.effective_to IS NULL OR e.txn_date <= r.effective_to)
),
best_rule AS (
    SELECT * FROM rule_match WHERE rule_rank = 1
),
running AS (
    SELECT
        b.*,
        SUM(b.amount_usd) OVER (
            PARTITION BY b.member_id, b.card_id, b.month, b.rule_id
            ORDER BY b.txn_ts, b.txn_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_bonus_spend
    FROM best_rule b
),
capped AS (
    SELECT
        r.*,
        CASE
            WHEN r.rule_id IS NULL THEN 0.0
            WHEN r.monthly_cap_usd IS NULL THEN r.amount_usd
            ELSE GREATEST(
                0.0,
                LEAST(r.amount_usd, r.monthly_cap_usd - (r.running_bonus_spend - r.amount_usd))
            )
        END AS bonus_eligible_usd
    FROM running r
),
scored AS (
    SELECT
        c.*,
        COALESCE(c.multiplier, c.base_earn_rate) AS applied_multiplier,
        ROUND(
            c.amount_usd * c.base_earn_rate
            + c.bonus_eligible_usd * (COALESCE(c.multiplier, c.base_earn_rate) - c.base_earn_rate),
            2
        ) AS points_earned
    FROM capped c
)
SELECT
    txn_id,
    member_id,
    card_id,
    card_sk,
    product_name,
    issuer,
    network,
    merchant_id,
    merchant_name,
    category,
    mcc,
    country,
    channel,
    txn_ts,
    txn_date,
    month,
    amount_usd,
    COALESCE(rule_id, 'BASE_EARN') AS rule_id,
    base_earn_rate,
    applied_multiplier,
    ROUND(bonus_eligible_usd, 2)   AS bonus_eligible_usd,
    points_earned,
    point_value_cents,
    ROUND(points_earned * point_value_cents / 100.0, 2) AS reward_value_usd,
    annual_fee_usd
FROM scored
