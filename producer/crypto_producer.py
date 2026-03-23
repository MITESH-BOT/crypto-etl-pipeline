import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("COINGECKO_API_KEY")
BASE_URL = "https://api.coingecko.com/api/v3"
COINS = ["bitcoin", "ethereum"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_crypto(coin_id: str) -> dict:
    url = f"{BASE_URL}/coins/{coin_id}"
    headers = {"x-cg-demo-api-key": API_KEY}
    params = {
        "localization": "false",
        "tickers": "false",
        "community_data": "false",
        "developer_data": "false"
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    market = data["market_data"]
    return {
        "coin_id": data["id"],
        "symbol": data["symbol"].upper(),
        "name": data["name"],
        "current_price_usd": market["current_price"]["usd"],
        "market_cap_usd": market["market_cap"]["usd"],
        "total_volume_usd": market["total_volume"]["usd"],
        "high_24h_usd": market["high_24h"]["usd"],
        "low_24h_usd": market["low_24h"]["usd"],
        "price_change_24h": market["price_change_24h"],
        "price_change_pct_24h": market["price_change_percentage_24h"],
        "circulating_supply": market["circulating_supply"],
        "fetched_at": datetime.now(datetime.UTC).isoformat()
    }

def run_producer(interval_seconds: int = 60):
    print("Crypto producer started...")
    while True:
        for coin in COINS:
            try:
                data = fetch_crypto(coin)
                producer.send("crypto-raw", value=data)
                print(f"Sent: {data['symbol']} | price=${data['current_price_usd']:,.2f}")
            except Exception as e:
                print(f"Error fetching {coin}: {e}")
        producer.flush()
        print(f"Batch complete. Sleeping {interval_seconds}s...")
        time.sleep(interval_seconds)

if __name__ == "__main__":
    run_producer()