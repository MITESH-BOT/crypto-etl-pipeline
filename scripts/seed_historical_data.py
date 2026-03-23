import snowflake.connector
from dotenv import load_dotenv
import os
import random
from datetime import datetime, timedelta

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database="CRYPTO_DB",
    schema="RAW",
    warehouse="COMPUTE_WH",
)

cursor = conn.cursor()


def generate_price_series(start_price, days=30, volatility=0.02):
    prices = []
    price = start_price
    for _ in range(days * 24):
        change = random.gauss(0, volatility)
        price = price * (1 + change)
        prices.append(round(price, 4))
    return prices


btc_prices = generate_price_series(65000, volatility=0.015)
eth_prices = generate_price_series(2000, volatility=0.018)

start_time = datetime.utcnow() - timedelta(days=30)
records = []

for i, (btc, eth) in enumerate(zip(btc_prices, eth_prices)):
    timestamp = start_time + timedelta(hours=i)
    for coin, price, mcap, vol in [
        ("bitcoin", btc, btc * 19600000, btc * 15000),
        ("ethereum", eth, eth * 120000000, eth * 80000),
    ]:
        records.append(
            (
                coin,
                "BTC" if coin == "bitcoin" else "ETH",
                "Bitcoin" if coin == "bitcoin" else "Ethereum",
                price,
                mcap,
                vol,
                round(price * 1.02, 4),
                round(price * 0.98, 4),
                round(price * random.uniform(-0.02, 0.02), 4),
                round(random.uniform(-2, 2), 4),
                19600000 if coin == "bitcoin" else 120000000,
                timestamp.isoformat(),
                timestamp.isoformat(),
            )
        )

cursor.executemany(
    """
    INSERT INTO RAW_CRYPTO (
        coin_id, symbol, name, current_price_usd,
        market_cap_usd, total_volume_usd, high_24h_usd,
        low_24h_usd, price_change_24h, price_change_pct_24h,
        circulating_supply, fetched_at, ingested_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
""",
    records,
)

conn.commit()
print(f"Inserted {len(records)} historical records successfully!")
cursor.close()
conn.close()
