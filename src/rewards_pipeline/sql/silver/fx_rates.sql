-- Silver: FX reference data, one rate per currency per day.
SELECT
    rate_date,
    UPPER(TRIM(currency)) AS currency,
    CAST(usd_rate AS DOUBLE) AS usd_rate
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY b.rate_date, UPPER(TRIM(b.currency))
            ORDER BY b._ingested_at DESC
        ) AS _rank
    FROM bronze_fx_rates b
    WHERE b.rate_date IS NOT NULL AND b.currency IS NOT NULL
)
WHERE _rank = 1
