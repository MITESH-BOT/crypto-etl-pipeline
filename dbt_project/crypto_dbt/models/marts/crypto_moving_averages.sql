SELECT
    coin_id,
    symbol,
    fetched_at,
    current_price_usd,
    ROUND(AVG(current_price_usd) OVER (
        PARTITION BY coin_id
        ORDER BY fetched_at
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 4)                                              AS ma_5,
    ROUND(AVG(current_price_usd) OVER (
        PARTITION BY coin_id
        ORDER BY fetched_at
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ), 4)                                              AS ma_10
FROM {{ ref('stg_crypto') }}