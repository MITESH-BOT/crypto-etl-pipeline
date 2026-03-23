SELECT
    coin_id,
    symbol,
    DATE(fetched_at)                                    AS price_date,
    ROUND(AVG(current_price_usd), 4)                   AS avg_price_usd,
    ROUND(MIN(current_price_usd), 4)                   AS min_price_usd,
    ROUND(MAX(current_price_usd), 4)                   AS max_price_usd,
    ROUND(AVG(price_change_pct_24h), 4)                AS avg_change_pct,
    SUM(total_volume_usd)                              AS total_volume_usd,
    COUNT(*)                                           AS record_count
FROM {{ ref('stg_crypto') }}
GROUP BY 1, 2, 3