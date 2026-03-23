from flask import Flask, jsonify
from flask_cors import CORS
import snowflake.connector
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
app = Flask(__name__)
CORS(app)

cache = {"data": None, "timestamp": None}
CACHE_SECONDS = 3600

def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="CRYPTO_DB",
        schema="STAGING",
        warehouse="COMPUTE_WH"
    )

def is_cache_valid():
    if not cache["timestamp"]:
        return False
    diff = (datetime.utcnow() - cache["timestamp"]).seconds
    return diff < CACHE_SECONDS

@app.route("/api/prices")
def get_prices():
    if is_cache_valid():
        return jsonify(cache["data"])
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT coin_id, symbol, current_price_usd,
               price_change_pct_24h, fetched_at
        FROM stg_crypto
        ORDER BY fetched_at DESC
        LIMIT 2
    """)
    rows = cursor.fetchall()
    data = [{"coin": r[0], "symbol": r[1], "price": r[2],
             "change_pct": r[3], "timestamp": str(r[4])} for r in rows]
    cache["data"] = data
    cache["timestamp"] = datetime.utcnow()
    cursor.close()
    conn.close()
    return jsonify(data)

@app.route("/api/history/<coin>")
def get_history(coin):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT price_date, avg_price_usd, min_price_usd,
               max_price_usd, total_volume_usd
        FROM daily_crypto_summary
        WHERE coin_id = %s
        ORDER BY price_date DESC
        LIMIT 30
    """, (coin,))
    rows = cursor.fetchall()
    data = [{"date": str(r[0]), "avg": r[1], "min": r[2],
             "max": r[3], "volume": r[4]} for r in rows]
    cursor.close()
    conn.close()
    return jsonify(list(reversed(data)))

@app.route("/api/moving-averages/<coin>")
def get_moving_averages(coin):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT fetched_at, current_price_usd, ma_5, ma_10
        FROM crypto_moving_averages
        WHERE coin_id = %s
        ORDER BY fetched_at DESC
        LIMIT 100
    """, (coin,))
    rows = cursor.fetchall()
    data = [{"timestamp": str(r[0]), "price": r[1],
             "ma5": r[2], "ma10": r[3]} for r in rows]
    cursor.close()
    conn.close()
    return jsonify(list(reversed(data)))

if __name__ == "__main__":
    app.run(debug=True, port=5000)