from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def transform_tick(raw: dict) -> dict:
    out = {
        "timestamp": datetime.now(IST).strftime("%H:%M:%S"),
        "instrument_token": raw.get("instrument_token"),
        "last_price": raw.get("last_price"),
        "last_traded_quantity": raw.get("last_traded_quantity"),
        "average_traded_price": raw.get("average_traded_price"),
        "volume_traded": raw.get("volume_traded"),
        "total_buy_quantity": raw.get("total_buy_quantity"),
        "total_sell_quantity": raw.get("total_sell_quantity"),
        "open": raw.get("ohlc", {}).get("open"),
        "high": raw.get("ohlc", {}).get("high"),
        "low": raw.get("ohlc", {}).get("low"),
        "close": raw.get("ohlc", {}).get("close"),
        "change": raw.get("change"),
        "oi": raw.get("oi"),
        "oi_day_high": raw.get("oi_day_high"),
        "oi_day_low": raw.get("oi_day_low"),
    }

    buy_depth = raw.get("depth", {}).get("buy", [])
    for i in range(5):
        n = i + 1
        level = buy_depth[i] if i < len(buy_depth) else {}
        out[f"buy_price_{n}"] = level.get("price")
        out[f"buy_qty_{n}"] = level.get("quantity")
        out[f"buy_orders_{n}"] = level.get("orders")

    sell_depth = raw.get("depth", {}).get("sell", [])
    for i in range(5):
        n = i + 1
        level = sell_depth[i] if i < len(sell_depth) else {}
        out[f"sell_price_{n}"] = level.get("price")
        out[f"sell_qty_{n}"] = level.get("quantity")
        out[f"sell_orders_{n}"] = level.get("orders")

    return out
