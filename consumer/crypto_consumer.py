import json
from kafka import KafkaConsumer
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transform.transformations import transform_crypto, validate_crypto
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    "crypto-raw",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="crypto-consumer-group",
    enable_auto_commit=True,
)


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="CRYPTO_DB",
        schema="RAW",
        warehouse="COMPUTE_WH",
    )


def insert_record(conn, record: dict):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO RAW_CRYPTO (
            coin_id, symbol, name, current_price_usd,
            market_cap_usd, total_volume_usd, high_24h_usd,
            low_24h_usd, price_change_24h, price_change_pct_24h,
            circulating_supply, fetched_at, ingested_at
        ) VALUES (
            %(coin_id)s, %(symbol)s, %(name)s,
            %(current_price_usd)s, %(market_cap_usd)s,
            %(total_volume_usd)s, %(high_24h_usd)s,
            %(low_24h_usd)s, %(price_change_24h)s,
            %(price_change_pct_24h)s, %(circulating_supply)s,
            %(fetched_at)s, %(ingested_at)s
        )
    """,
        record,
    )
    cursor.close()


def run_consumer():
    conn = get_snowflake_conn()
    dead_letter = []
    print("Crypto consumer started...")
    for message in consumer:
        raw = message.value
        try:
            transformed = transform_crypto(raw)
            if validate_crypto(transformed):
                insert_record(conn, transformed)
                print(
                    f"Loaded: {transformed['symbol']} | ${transformed['current_price_usd']:,.2f}"
                )
            else:
                dead_letter.append(raw)
                print(f"Validation failed — DLQ: {raw.get('coin_id')}")
        except Exception as e:
            dead_letter.append(raw)
            print(f"Consumer error: {e}")


if __name__ == "__main__":
    run_consumer()
