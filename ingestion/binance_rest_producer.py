"""
binance_rest_producer.py
REST API producer – thu thập OHLCV lịch sử từ Binance REST API
và đẩy vào Kafka topic raw-ohlcv.

Chạy:
    python ingestion/binance_rest_producer.py \
        --symbols BTCUSDT ETHUSDT \
        --interval 1m \
        --start-date 2024-01-01 \
        --end-date 2024-01-31
"""

"""Flow
Terminal (input)
      ↓
parse_args()
      ↓
args.symbols, args.interval
      ↓
BinanceRESTProducer(...)
"""

import argparse 
import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional

import aiohttp
# aiohttp trong Python là một thư viện dùng để làm việc với HTTP theo kiểu bất đồng bộ (asynchronous)
# Tức là bạn có thể gửi nhiều request cùng lúc mà không phải chờ từng cái chạy xong.
from confluent_kafka import Producer

# Import configuration + helpers from package-local module
from ingestion.kafka_config import (
    PRODUCER_CONFIG,
    TOPICS,
    BINANCE_REST_BASE_URL,
    BINANCE_REST_ENDPOINTS,
    BINANCE_API_KEY,
    REST_REQUEST_DELAY_MS,
    REST_MAX_LIMIT,
    get_partition_for_symbol,
    SUPPORTED_SYMBOLS,
)
from ingestion.kafka_utils import produce_with_retry
from ingestion.validation import is_valid_timestamp_ms, validate_ohlcv

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("BinanceRESTProducer")


# ─────────────────────────────────────────────
# Interval → milliseconds mapping
# Bảng ánh xạ từ Khoảng thời gian sang đơn vị Mili giây
# ─────────────────────────────────────────────
INTERVAL_MS: dict = {
    "1m":  60_000,
    "3m":  180_000,
    "5m":  300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h":  3_600_000,
    "4h":  14_400_000,
    "1d":  86_400_000,
}


# ─────────────────────────────────────────────
# OHLCV Row → dict
# ─────────────────────────────────────────────
def parse_kline_row(
    row: list,
    symbol: str,
    interval: str,
    now_ms: Optional[int] = None,
) -> Optional[dict]:
    """
    Parse a single Binance klines API row into a standardised dict.
    Binance kline format:
      [open_time, open, high, low, close, volume, close_time,
       quote_volume, trade_count, taker_buy_base_vol, taker_buy_quote_vol, ignore]

    Return `None` when the row is malformed or its values are invalid.

    `is_closed` is True only when the candle's close_time is in the past:
    the klines endpoint also returns the candle that is still forming, which
    must not be flagged as closed (it would clash with the real closing
    candle sent later by the WebSocket stream).
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)

    try:
        open_time_ms  = int(row[0])
        close_time_ms = int(row[6])
        if not (is_valid_timestamp_ms(open_time_ms) and is_valid_timestamp_ms(close_time_ms)):
            raise ValueError(f"invalid open_time/close_time {row[0]!r}/{row[6]!r}")
        open_dt = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)

        record = {
            "symbol":              symbol,
            "event_time":          open_dt.isoformat(),
            "ingest_time":         datetime.now(tz=timezone.utc).isoformat(),
            "open_time":           open_dt.isoformat(),
            "close_time":          datetime.fromtimestamp(close_time_ms / 1000, tz=timezone.utc).isoformat(),
            "interval":            interval,
            "open":                float(row[1]),
            "high":                float(row[2]),
            "low":                 float(row[3]),
            "close":               float(row[4]),
            "volume":              float(row[5]),
            "quote_volume":        float(row[7]),
            "trade_count":         int(row[8]),
            "taker_buy_base_vol":  float(row[9]),
            "taker_buy_quote_vol": float(row[10]),
            "is_closed":           close_time_ms < now_ms,
            "source":              "rest",
            "partition_date":      open_dt.strftime("%Y-%m-%d"),
        }
    except (IndexError, TypeError, ValueError, OverflowError, OSError) as exc:
        logger.warning("Failed to parse kline row for %s: %s | row=%s", symbol, exc, row)
        return None

    reason = validate_ohlcv(record)
    if reason:
        logger.warning("Invalid kline row for %s (%s) | row=%s", symbol, reason, row)
        return None
    return record


# ─────────────────────────────────────────────
# REST Client (async)
# ─────────────────────────────────────────────
class BinanceRESTClient:
    """Thin async wrapper around Binance REST klines endpoint with rate-limit handling."""

    BASE_HEADERS = {"X-MBX-APIKEY": BINANCE_API_KEY} if BINANCE_API_KEY else {}

    def __init__(self, session: aiohttp.ClientSession):
        self._session = session

    async def get_klines(
        self,
        symbol:    str,
        interval:  str,
        start_ms:  int,
        end_ms:    int,
        limit:     int = REST_MAX_LIMIT,
    ) -> List[list]:
        """Fetch up to `limit` kline rows for the given time window."""
        url    = f"{BINANCE_REST_BASE_URL}{BINANCE_REST_ENDPOINTS['klines']}"
        params = {
            "symbol":    symbol,
            "interval":  interval,
            "startTime": start_ms,
            "endTime":   end_ms,
            "limit":     min(limit, REST_MAX_LIMIT),
        }

        for attempt in range(5):
            try:
                async with self._session.get(
                    url, params=params, headers=self.BASE_HEADERS, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 60))
                        logger.warning("Rate limit hit – sleeping %ds", retry_after)
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status == 418:
                        logger.error("IP banned by Binance. Exiting.")
                        raise RuntimeError("Binance IP ban (HTTP 418)")
                    resp.raise_for_status()
                    return await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                backoff = 2 ** attempt
                logger.warning("HTTP error on attempt %d: %s – retry in %ds", attempt + 1, exc, backoff)
                await asyncio.sleep(backoff)

        raise RuntimeError(f"Failed to fetch klines for {symbol} after 5 attempts")


# ─────────────────────────────────────────────
# REST Producer
# ─────────────────────────────────────────────
def _delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed | topic=%s error=%s", msg.topic(), err)
    else:
        logger.debug("Delivered | topic=%s partition=%d offset=%d", msg.topic(), msg.partition(), msg.offset())


class BinanceRESTProducer:
    """
    Fetches historical OHLCV data for multiple symbols using Binance REST API
    and produces messages to Kafka.  Respects rate limits via configurable delays.
    """

    def __init__(
        self,
        symbols:    List[str],
        interval:   str,
        start_date: datetime,
        end_date:   datetime,
    ):
        self.symbols    = [s.upper() for s in symbols]
        self.interval   = interval
        self.start_ms   = int(start_date.timestamp() * 1000)
        self.end_ms     = int(end_date.timestamp() * 1000)
        self.interval_ms= INTERVAL_MS.get(interval, 60_000)
        self.producer   = Producer(PRODUCER_CONFIG)
        self._total_produced = 0
        self._invalid_count  = 0   # rows rejected by parse/validation
        self._dropped_count  = 0   # rows that could not be queued in Kafka

        invalid = [s for s in self.symbols if s not in SUPPORTED_SYMBOLS]
        if invalid:
            raise ValueError(f"Unsupported symbols: {invalid}. Supported: {SUPPORTED_SYMBOLS}")

    async def _fetch_and_produce_symbol(
        self,
        client: BinanceRESTClient,
        symbol: str,
    ):
        """Paginate through time range and produce all klines for one symbol."""
        logger.info("Fetching %s | interval=%s | %s → %s", symbol, self.interval,
                    datetime.fromtimestamp(self.start_ms / 1000, tz=timezone.utc).date(),
                    datetime.fromtimestamp(self.end_ms / 1000, tz=timezone.utc).date())

        cursor_ms = self.start_ms
        partition = get_partition_for_symbol(symbol)
        symbol_count = 0

        while cursor_ms < self.end_ms:
            batch_end_ms = min(cursor_ms + self.interval_ms * REST_MAX_LIMIT, self.end_ms)
            rows = await client.get_klines(symbol, self.interval, cursor_ms, batch_end_ms)  #Gọi API Binance

            if not rows:
                logger.info("No more data for %s at cursor %d", symbol, cursor_ms)
                break

            now_ms = int(time.time() * 1000)
            for row in rows:
                parsed = parse_kline_row(row, symbol, self.interval, now_ms=now_ms)
                if parsed is None:
                    self._invalid_count += 1
                    continue
                value_bytes = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
                key_bytes   = symbol.encode("utf-8")

                # partition=partition is intentionally NOT passed (unchanged behaviour).
                if produce_with_retry(self.producer, TOPICS["raw_ohlcv"],
                                      value_bytes, key_bytes, _delivery_report):
                    symbol_count += 1
                    self._total_produced += 1
                else:
                    self._dropped_count += 1

            # Poll to trigger callbacks
            self.producer.poll(0)

            # Advance cursor past the last row's open_time. If that row is
            # malformed, jump past this batch window so paging cannot stall.
            try:
                cursor_ms = int(rows[-1][0]) + self.interval_ms
            except (IndexError, TypeError, ValueError):
                logger.warning("Malformed last row for %s; skipping to end of batch window.", symbol)
                cursor_ms = batch_end_ms + 1

            # Respect REST rate limit
            await asyncio.sleep(REST_REQUEST_DELAY_MS / 1000)

        logger.info("Finished %s: produced %d records.", symbol, symbol_count)

    async def run(self):
        """Fetch all symbols concurrently (but with individual rate-limit pacing)."""
        #Lấy tất cả các mã (symbols) cùng một lúc nhưng vẫn điều tiết tốc độ cho từng yêu cầu để không vi phạm giới hạn của hệ thống
        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(connector=connector) as session:
            client = BinanceRESTClient(session)
            tasks  = [self._fetch_and_produce_symbol(client, s) for s in self.symbols]
            # Run concurrently per symbol, sequential per symbol internally.
            # return_exceptions=True: one failing symbol no longer cancels the others.
            results = await asyncio.gather(*tasks, return_exceptions=True)

        logger.info("Flushing Kafka buffer...")
        self.producer.flush(timeout=60)
        logger.info(
            "REST Producer done. Total produced: %d | invalid rows skipped: %d | dropped: %d",
            self._total_produced, self._invalid_count, self._dropped_count,
        )

        failed = {s: r for s, r in zip(self.symbols, results) if isinstance(r, BaseException)}
        for symbol, exc in failed.items():
            logger.error("Backfill failed for %s: %s", symbol, exc)
        if failed:
            # Still signal failure to callers (e.g. the scheduler must not
            # write its success marker), after the other symbols completed.
            raise RuntimeError(f"REST backfill failed for symbols: {sorted(failed)}")


# ─────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance REST API → Kafka Historical Producer")
    parser.add_argument(
        "--symbols", nargs="+", default=["BTCUSDT"],
        help="List of trading pair symbols",
    )
    parser.add_argument(
        "--interval", default="1m",
        choices=list(INTERVAL_MS.keys()),
        help="Kline interval (default: 1m)",
    )
    parser.add_argument(
        "--start-date", type=str, required=True,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date", type=str,
        default=datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD), default: today",
    )
    return parser.parse_args()


async def _main():  #Khai báo hàm bất đồng bộ
    args = parse_args()
    #parse_args() = lấy input từ command line (terminal)

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_date   = datetime.strptime(args.end_date,   "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)

    logger.info(
        "Starting REST Producer | symbols=%s interval=%s start=%s end=%s",
        args.symbols, args.interval, start_date.date(), end_date.date(),
    )

    producer = BinanceRESTProducer(
        symbols=args.symbols,
        interval=args.interval,
        start_date=start_date,
        end_date=end_date,
    )
    await producer.run()    #Kích hoạt và duy trì kết nối của Producer tới Broker (Kafka Server).


if __name__ == "__main__":
    asyncio.run(_main())
