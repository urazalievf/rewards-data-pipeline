-- Silver: merchant dimension. Latest record per merchant wins.
SELECT
    merchant_id,
    INITCAP(TRIM(merchant_name)) AS merchant_name,
    LPAD(TRIM(mcc), 4, '0') AS mcc,
    LOWER(TRIM(category)) AS category,
    UPPER(TRIM(country)) AS country
FROM (
    SELECT
        b.*,
        ROW_NUMBER() OVER (PARTITION BY b.merchant_id ORDER BY b._ingested_at DESC) AS _rank
    FROM bronze_merchants b
    WHERE b.merchant_id IS NOT NULL
)
WHERE _rank = 1
