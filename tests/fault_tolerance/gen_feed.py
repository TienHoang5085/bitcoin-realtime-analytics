"""Print feed chunk N as Kafka key|json lines.

One chunk = 2 minutes of 1m candles for 5 symbols, one update every 2 s
(300 messages, the size of a real 2-minute batch). Chunks continue after the
last candle of lakehouse_v3 (2026-11-25 17:02 UTC), so chunk N never overlaps
another chunk.
"""
import json
import sys
from datetime import datetime, timedelta, timezone

n = int(sys.argv[1])
start = datetime(2026, 11, 25, 17, 2, tzinfo=timezone.utc)
prices = {"BTCUSDT": 87000.0, "ETHUSDT": 2900.0, "BNBUSDT": 860.0, "SOLUSDT": 135.0, "XRPUSDT": 2.1}


def stamp(t):
    return t.strftime("%Y-%m-%dT%H:%M:%S.000Z")


out = []
for p, (symbol, price) in enumerate(prices.items()):
    for minute in range(2):
        opened = start + timedelta(minutes=n * 2 + minute)
        for second in range(2, 61, 2):
            at = opened + timedelta(seconds=second)
            value = {
                "symbol": symbol, "event_time": stamp(at), "ingest_time": stamp(at),
                "open_time": stamp(opened), "close_time": stamp(opened + timedelta(seconds=59)),
                "interval": "1m", "open": price, "high": price * 1.001, "low": price * 0.999,
                "close": price * (1 + 0.0001 * ((n + minute + p) % 5 - 2)),
                "volume": second / 6.0, "quote_volume": second / 6.0 * price, "trade_count": second,
                "taker_buy_base_vol": second / 12.0, "is_closed": second == 60, "source": "ft-test",
                "partition_date": opened.strftime("%Y-%m-%d"),
            }
            out.append(f"{symbol}|{json.dumps(value)}")
sys.stdout.write("\n".join(out) + "\n")
