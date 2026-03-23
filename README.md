# Crypto ETL Pipeline

Real-time cryptocurrency data pipeline that streams live Bitcoin and 
Ethereum prices from CoinGecko into Snowflake using Apache Kafka,
transforms and models the data with dbt, and validates everything
with automated CI/CD via GitHub Actions.

## Architecture

CoinGecko API → Kafka Producer → Kafka Broker → Kafka Consumer
→ Snowflake (RAW) → dbt Models (STAGING) → Analytics

## Tech Stack

- **Data Source:** CoinGecko API (free Demo tier)
- **Streaming:** Apache Kafka (Docker)
- **Warehouse:** Snowflake
- **Transformation:** dbt (staging, daily summary, moving averages)
- **Testing:** pytest (9 unit tests)
- **CI/CD:** GitHub Actions
- **Language:** Python 3.13

## Project Structure
```
crypto-etl-pipeline/
├── producer/          # Fetches prices from CoinGecko → Kafka
├── consumer/          # Reads Kafka → validates → loads Snowflake
├── transform/         # Transformation and validation logic
├── dbt_project/       # dbt models and tests
├── tests/             # pytest unit tests
├── dags/              # Airflow DAG (orchestration)
└── docker-compose.yml # Kafka + Zookeeper setup
```

## Setup

### Prerequisites
- Python 3.11+
- Docker Desktop
- Snowflake account (free trial)
- CoinGecko API key (free Demo)

### Installation
```bash
# Clone the repo
git clone https://github.com/MITESH-BOT/crypto-etl-pipeline.git
cd crypto-etl-pipeline

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Create .env file with your credentials
cp .env.example .env
```

### Running the Pipeline
```bash
# Start Kafka
docker compose up -d

# Terminal 1 - Start producer
python producer/crypto_producer.py

# Terminal 2 - Start consumer
python consumer/crypto_consumer.py

# Run dbt models
dbt run
dbt test
```

### Running Tests
```bash
pytest tests/ -v
```

## dbt Models

| Model | Description |
|---|---|
| `stg_crypto` | Deduplicated staging layer |
| `daily_crypto_summary` | Daily OHLCV aggregations |
| `crypto_moving_averages` | 5 and 10 period moving averages |

## Design Decisions

- **Kafka over simple polling** — decouples ingestion from loading.
  If Snowflake is unavailable, Kafka retains messages so no data is lost.
- **Dead letter queue** — invalid records are caught before hitting
  the warehouse and can be replayed after fixing validation logic.
- **dbt for transformation** — version-controlled, testable SQL with
  auto-generated documentation that analysts can trust.
- **Validation checks** — high must be >= low, price must be positive,
  market cap must be positive — mirrors real financial data quality rules.
