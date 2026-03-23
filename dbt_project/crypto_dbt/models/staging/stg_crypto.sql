SELECT
    coin_id,
    symbol,
    name,
    current_price_usd,
    market_cap_usd,
    total_volume_usd,
    high_24h_usd,
    low_24h_usd,
    price_change_24h,
    price_change_pct_24h,
    circulating_supply,
    fetched_at,
    ingested_at,
    ROW_NUMBER() OVER (
        PARTITION BY coin_id, fetched_at
        ORDER BY ingested_at DESC
    ) AS row_num
FROM {{ source('raw', 'raw_crypto') }}
QUALIFY row_num = 1