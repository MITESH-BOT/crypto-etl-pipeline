from datetime import datetime


def transform_crypto(raw: dict) -> dict:
    return {
        "coin_id": raw["coin_id"],
        "symbol": raw["symbol"],
        "name": raw["name"],
        "current_price_usd": round(raw["current_price_usd"], 4),
        "market_cap_usd": raw["market_cap_usd"],
        "total_volume_usd": raw["total_volume_usd"],
        "high_24h_usd": round(raw["high_24h_usd"], 4),
        "low_24h_usd": round(raw["low_24h_usd"], 4),
        "price_change_24h": round(raw["price_change_24h"], 4),
        "price_change_pct_24h": round(raw["price_change_pct_24h"], 4),
        "circulating_supply": raw["circulating_supply"],
        "fetched_at": raw["fetched_at"],
        "ingested_at": datetime.utcnow().isoformat(),
    }


def validate_crypto(record: dict) -> bool:
    if not record.get("current_price_usd") or record["current_price_usd"] <= 0:
        return False
    if record["high_24h_usd"] < record["low_24h_usd"]:
        return False
    if not record.get("market_cap_usd") or record["market_cap_usd"] <= 0:
        return False
    if record.get("coin_id") not in ["bitcoin", "ethereum"]:
        return False
    return True
