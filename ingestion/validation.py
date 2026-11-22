"""
ingestion/validation.py
Kiểm tra giá trị nghiệp vụ cho bản tin đã chuẩn hóa trước khi gửi vào Kafka.

Mỗi hàm `validate_*` trả về `None` nếu bản ghi hợp lệ, ngược lại trả về chuỗi
mô tả lý do để parser ghi log và loại bỏ bản ghi. Các hàm này KHÔNG thay đổi
schema hay nội dung bản ghi.
"""

import math
from typing import Any, Optional


def is_valid_price(value: Any) -> bool:
    """Giá phải là số hữu hạn và > 0."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and math.isfinite(value) and value > 0


def is_valid_quantity(value: Any) -> bool:
    """Khối lượng phải là số hữu hạn và >= 0."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and math.isfinite(value) and value >= 0


def is_valid_timestamp_ms(value: Any) -> bool:
    """Epoch milliseconds phải là số nguyên dương."""
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def validate_ohlcv(record: dict) -> Optional[str]:
    """Kiểm tra một bản ghi OHLCV (dùng chung cho WebSocket và REST)."""
    for field in ("open", "high", "low", "close"):
        if not is_valid_price(record.get(field)):
            return f"invalid {field}={record.get(field)!r}"
    for field in ("volume", "quote_volume", "taker_buy_base_vol", "taker_buy_quote_vol"):
        if not is_valid_quantity(record.get(field)):
            return f"invalid {field}={record.get(field)!r}"
    trade_count = record.get("trade_count")
    if not isinstance(trade_count, int) or trade_count < 0:
        return f"invalid trade_count={trade_count!r}"

    high, low = record["high"], record["low"]
    if high < low:
        return f"high ({high}) < low ({low})"
    if not (low <= record["open"] <= high and low <= record["close"] <= high):
        return f"open/close outside [low, high] range"
    return None


def validate_trade(record: dict) -> Optional[str]:
    """Kiểm tra một bản ghi trade (raw-crypto-ticks)."""
    if not is_valid_price(record.get("price")):
        return f"invalid price={record.get('price')!r}"
    quantity = record.get("quantity")
    if not is_valid_quantity(quantity) or quantity == 0:
        return f"invalid quantity={quantity!r}"
    return None


def validate_book_ticker(record: dict) -> Optional[str]:
    """Kiểm tra một bản ghi best bid/ask (raw-orderbook)."""
    for field in ("best_bid", "best_ask"):
        if not is_valid_price(record.get(field)):
            return f"invalid {field}={record.get(field)!r}"
    for field in ("best_bid_qty", "best_ask_qty"):
        if not is_valid_quantity(record.get(field)):
            return f"invalid {field}={record.get(field)!r}"
    if record["best_bid"] > record["best_ask"]:
        return f"crossed book: best_bid ({record['best_bid']}) > best_ask ({record['best_ask']})"
    return None
