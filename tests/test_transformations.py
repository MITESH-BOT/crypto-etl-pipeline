import pytest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from transform.transformations import transform_crypto, validate_crypto

SAMPLE_RAW = {
    "coin_id": "bitcoin",
    "symbol": "BTC",
    "name": "Bitcoin",
    "current_price_usd": 67811.00,
    "market_cap_usd": 1330000000000,
    "total_volume_usd": 25000000000,
    "high_24h_usd": 68500.00,
    "low_24h_usd": 66900.00,
    "price_change_24h": 450.00,
    "price_change_pct_24h": 0.67,
    "circulating_supply": 19600000,
    "fetched_at": "2024-11-14T14:30:00"
}

def test_transform_returns_all_fields():
    result = transform_crypto(SAMPLE_RAW)
    expected_keys = [
        "coin_id", "symbol", "name", "current_price_usd",
        "market_cap_usd", "total_volume_usd", "high_24h_usd",
        "low_24h_usd", "price_change_24h", "price_change_pct_24h",
        "circulating_supply", "fetched_at", "ingested_at"
    ]
    for key in expected_keys:
        assert key in result, f"Missing field: {key}"

def test_transform_rounds_price_correctly():
    result = transform_crypto(SAMPLE_RAW)
    assert result["current_price_usd"] == round(67811.00, 4)

def test_validate_passes_good_record():
    result = transform_crypto(SAMPLE_RAW)
    assert validate_crypto(result) is True

def test_validate_fails_zero_price():
    result = transform_crypto(SAMPLE_RAW)
    result["current_price_usd"] = 0
    assert validate_crypto(result) is False

def test_validate_fails_negative_price():
    result = transform_crypto(SAMPLE_RAW)
    result["current_price_usd"] = -100
    assert validate_crypto(result) is False

def test_validate_fails_high_below_low():
    result = transform_crypto(SAMPLE_RAW)
    result["high_24h_usd"] = 60000.00
    result["low_24h_usd"] = 65000.00
    assert validate_crypto(result) is False

def test_validate_fails_zero_market_cap():
    result = transform_crypto(SAMPLE_RAW)
    result["market_cap_usd"] = 0
    assert validate_crypto(result) is False

def test_validate_fails_unknown_coin():
    result = transform_crypto(SAMPLE_RAW)
    result["coin_id"] = "dogecoin"
    assert validate_crypto(result) is False

def test_ethereum_passes_validation():
    eth_raw = SAMPLE_RAW.copy()
    eth_raw["coin_id"] = "ethereum"
    eth_raw["symbol"] = "ETH"
    eth_raw["current_price_usd"] = 2041.27
    eth_raw["high_24h_usd"] = 2100.00
    eth_raw["low_24h_usd"] = 2000.00
    result = transform_crypto(eth_raw)
    assert validate_crypto(result) is True