"""
tests/test_ingestion.py
Unit tests for Binance producers and Kafka config.
"""

import json
import sys
import os
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.kafka_config import (
    get_partition_for_symbol,
    SUPPORTED_SYMBOLS,
    TOPICS,
)
from ingestion.binance_ws_producer import (
    parse_kline_event,
    parse_trade_event,
    parse_book_ticker_event,
    BinanceWebSocketProducer,
)
from ingestion.binance_rest_producer import parse_kline_row


# ─────────────────────────────────────────────
# Kafka Config Tests
# ─────────────────────────────────────────────

class TestKafkaConfig:
    def test_partition_for_known_symbol(self):
        for symbol in SUPPORTED_SYMBOLS:
            partition = get_partition_for_symbol(symbol)
            assert isinstance(partition, int)
            assert 0 <= partition < len(SUPPORTED_SYMBOLS)

    def test_partition_for_unknown_symbol_is_int(self):
        partition = get_partition_for_symbol("DOGEUSDT")
        assert isinstance(partition, int)

    def test_all_required_topics_exist(self):
        required = {"raw_ticks", "raw_ohlcv", "raw_orderbook", "processed_signals", "alert_events"}
        assert required.issubset(set(TOPICS.keys()))

    def test_partition_deterministic(self):
        """Same symbol always maps to same partition."""
        assert get_partition_for_symbol("BTCUSDT") == get_partition_for_symbol("BTCUSDT")


# ─────────────────────────────────────────────
# WebSocket Parser Tests
# ─────────────────────────────────────────────

SAMPLE_KLINE_EVENT = {
    "e": "kline",
    "E": 1700000000000,
    "k": {
        "t": 1700000000000,
        "T": 1700000059999,
        "s": "BTCUSDT",
        "i": "1m",
        "o": "37000.00",
        "h": "37100.00",
        "l": "36950.00",
        "c": "37050.00",
        "v": "10.5",
        "q": "388525.50",
        "n": 250,
        "V": "5.2",
        "Q": "192580.40",
        "x": True,
    }
}

SAMPLE_TRADE_EVENT = {
    "e": "trade",
    "E": 1700000000000,
    "s": "BTCUSDT",
    "t": 999888777,
    "p": "37050.00",
    "q": "0.001",
    "b": 111,
    "a": 222,
    "T": 1700000000000,
    "m": False,
}

SAMPLE_BOOK_TICKER = {
    "u": 12345678,
    "s": "BTCUSDT",
    "b": "37049.00",
    "B": "1.500",
    "a": "37050.00",
    "A": "2.000",
}


class TestKlineParser:
    def test_parse_valid_kline(self):
        result = parse_kline_event(SAMPLE_KLINE_EVENT)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["close"] == 37050.00
        assert result["volume"] == 10.5
        assert result["source"] == "websocket"
        assert result["is_closed"] is True
        assert "partition_date" in result

    def test_parse_kline_missing_key(self):
        bad = {"e": "kline", "E": 123}  # Missing "k"
        result = parse_kline_event(bad)
        assert result is None

    def test_parse_kline_invalid_price(self):
        bad = {**SAMPLE_KLINE_EVENT}
        bad["k"] = {**SAMPLE_KLINE_EVENT["k"], "c": "not_a_number"}
        result = parse_kline_event(bad)
        assert result is None


class TestTradeParser:
    def test_parse_valid_trade(self):
        result = parse_trade_event(SAMPLE_TRADE_EVENT)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["price"] == 37050.00
        assert result["is_buyer_maker"] is False
        assert result["source"] == "websocket"

    def test_parse_trade_missing_field(self):
        bad = {"e": "trade"}
        result = parse_trade_event(bad)
        assert result is None


class TestBookTickerParser:
    def test_parse_valid_book_ticker(self):
        result = parse_book_ticker_event(SAMPLE_BOOK_TICKER)
        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["best_bid"] == 37049.00
        assert result["best_ask"] == 37050.00

    def test_spread_is_positive(self):
        result = parse_book_ticker_event(SAMPLE_BOOK_TICKER)
        assert result["best_ask"] > result["best_bid"]


# ─────────────────────────────────────────────
# REST Producer Parser Tests
# ─────────────────────────────────────────────

SAMPLE_REST_ROW = [
    1700000000000,  # open_time
    "37000.00",     # open
    "37100.00",     # high
    "36950.00",     # low
    "37050.00",     # close
    "10.50",        # volume
    1700000059999,  # close_time
    "388525.50",    # quote_volume
    250,            # trade_count
    "5.20",         # taker_buy_base_vol
    "192580.40",    # taker_buy_quote_vol
    "0",            # ignore
]


class TestRESTParser:
    def test_parse_valid_row(self):
        result = parse_kline_row(SAMPLE_REST_ROW, "BTCUSDT", "1m")
        assert result["symbol"] == "BTCUSDT"
        assert result["close"] == 37050.00
        assert result["volume"] == 10.50
        assert result["trade_count"] == 250
        assert result["source"] == "rest"
        assert result["is_closed"] is True
        assert result["interval"] == "1m"

    def test_parse_row_partition_date_format(self):
        result = parse_kline_row(SAMPLE_REST_ROW, "BTCUSDT", "1m")
        assert len(result["partition_date"]) == 10   # YYYY-MM-DD


# ─────────────────────────────────────────────
# Producer Validation Tests
# ─────────────────────────────────────────────

class TestProducerValidation:
    def test_invalid_symbol_raises(self):
        with pytest.raises(ValueError, match="Unsupported symbols"):
            BinanceWebSocketProducer(symbols=["INVALID999"])

    def test_valid_symbols_accepted(self):
        # Should not raise
        producer = BinanceWebSocketProducer.__new__(BinanceWebSocketProducer)
        producer.symbols = ["BTCUSDT", "ETHUSDT"]
        producer._stop = False
        producer._msg_count = 0
        assert "BTCUSDT" in producer.symbols

    def test_stream_url_contains_all_symbols(self):
        # Build URL manually using the method logic
        symbols = ["BTCUSDT", "ETHUSDT"]
        streams = []
        for s in symbols:
            sl = s.lower()
            streams.extend([f"{sl}@kline_1m", f"{sl}@trade", f"{sl}@bookTicker"])
        url = "wss://stream.binance.com:9443/ws/" + "/".join(streams)
        assert "btcusdt@kline_1m" in url
        assert "ethusdt@trade" in url


# ─────────────────────────────────────────────
# Input validation (Nhiệm vụ 4/5 – data quality)
# ─────────────────────────────────────────────

import asyncio

import websockets

from ingestion.kafka_utils import produce_with_retry
from ingestion.kafka_config import WS_RECONNECT_INITIAL_DELAY


def _kline_with(**overrides):
    bad = {**SAMPLE_KLINE_EVENT}
    bad["k"] = {**SAMPLE_KLINE_EVENT["k"], **overrides}
    return bad


class TestKlineValidation:
    @pytest.mark.parametrize("overrides", [
        {"c": "-1"},                       # negative price
        {"o": "0"},                        # zero price
        {"v": "nan"},                      # NaN volume
        {"q": "inf"},                      # infinite quote volume
        {"v": "-0.5"},                     # negative volume
        {"h": "36900.00"},                 # high < low
        {"c": "38000.00"},                 # close above high
        {"t": 0},                          # invalid open_time
    ])
    def test_invalid_values_are_rejected(self, overrides):
        assert parse_kline_event(_kline_with(**overrides)) is None

    def test_invalid_event_time_is_rejected(self):
        assert parse_kline_event({**SAMPLE_KLINE_EVENT, "E": None}) is None

    def test_unclosed_candle_is_kept(self):
        result = parse_kline_event(_kline_with(x=False))
        assert result is not None and result["is_closed"] is False


class TestTradeValidation:
    def test_event_time_falls_back_to_trade_time(self):
        raw = {k: v for k, v in SAMPLE_TRADE_EVENT.items() if k != "E"}
        result = parse_trade_event(raw)
        assert result is not None
        assert result["event_time"] == result["trade_time"]

    def test_trade_time_falls_back_to_event_time(self):
        raw = {k: v for k, v in SAMPLE_TRADE_EVENT.items() if k != "T"}
        result = parse_trade_event(raw)
        assert result is not None
        assert result["trade_time"] == result["event_time"]

    def test_no_timestamp_is_rejected(self):
        raw = {k: v for k, v in SAMPLE_TRADE_EVENT.items() if k not in ("E", "T")}
        assert parse_trade_event(raw) is None

    @pytest.mark.parametrize("field,value", [("p", "0"), ("p", "-5"), ("p", "nan"), ("q", "0"), ("q", "abc")])
    def test_invalid_price_or_quantity_is_rejected(self, field, value):
        assert parse_trade_event({**SAMPLE_TRADE_EVENT, field: value}) is None


class TestBookTickerValidation:
    def test_crossed_book_is_rejected(self):
        assert parse_book_ticker_event({**SAMPLE_BOOK_TICKER, "b": "37060.00"}) is None

    def test_negative_quantity_is_rejected(self):
        assert parse_book_ticker_event({**SAMPLE_BOOK_TICKER, "B": "-1"}) is None

    def test_equal_bid_ask_is_kept(self):
        assert parse_book_ticker_event({**SAMPLE_BOOK_TICKER, "b": "37050.00"}) is not None


class TestRESTParserValidation:
    def test_in_progress_candle_is_not_closed(self):
        now_ms = SAMPLE_REST_ROW[6] - 1_000      # 1s before the candle closes
        result = parse_kline_row(SAMPLE_REST_ROW, "BTCUSDT", "1m", now_ms=now_ms)
        assert result["is_closed"] is False

    def test_finished_candle_is_closed(self):
        now_ms = SAMPLE_REST_ROW[6] + 1
        result = parse_kline_row(SAMPLE_REST_ROW, "BTCUSDT", "1m", now_ms=now_ms)
        assert result["is_closed"] is True

    def test_malformed_row_returns_none(self):
        assert parse_kline_row(SAMPLE_REST_ROW[:5], "BTCUSDT", "1m") is None

    def test_invalid_price_returns_none(self):
        bad = list(SAMPLE_REST_ROW)
        bad[4] = "-1"
        assert parse_kline_row(bad, "BTCUSDT", "1m") is None


# ─────────────────────────────────────────────
# Dispatcher / Kafka producer robustness
# ─────────────────────────────────────────────

def _bare_ws_producer():
    """Producer instance without connecting to Kafka."""
    producer = BinanceWebSocketProducer.__new__(BinanceWebSocketProducer)
    producer.symbols = ["BTCUSDT"]
    producer.interval = "1m"
    producer.producer = MagicMock()
    producer._stop = False
    producer._msg_count = 0
    producer._invalid_count = 0
    producer._dropped_count = 0
    producer._error_count = 0
    producer._connected = False
    return producer


class TestDispatcher:
    def test_non_dict_payload_is_skipped(self):
        p = _bare_ws_producer()
        p._dispatch(["not", "a", "dict"])
        p.producer.produce.assert_not_called()
        assert p._error_count == 1

    def test_book_ticker_routes_to_orderbook_topic(self):
        p = _bare_ws_producer()
        p._dispatch(SAMPLE_BOOK_TICKER)
        assert p.producer.produce.call_args.kwargs["topic"] == TOPICS["raw_orderbook"]

    def test_other_event_with_bid_ask_is_not_routed_to_orderbook(self):
        p = _bare_ws_producer()
        p._dispatch({"e": "depthUpdate", "s": "BTCUSDT", "b": [], "a": [], "u": 1})
        p.producer.produce.assert_not_called()

    def test_topics_and_keys_unchanged(self):
        p = _bare_ws_producer()
        p._dispatch(SAMPLE_KLINE_EVENT)
        p._dispatch(SAMPLE_TRADE_EVENT)
        calls = p.producer.produce.call_args_list
        assert [c.kwargs["topic"] for c in calls] == [TOPICS["raw_ohlcv"], TOPICS["raw_ticks"]]
        assert all(c.kwargs["key"] == b"BTCUSDT" for c in calls)
        assert all("partition" not in c.kwargs for c in calls)

    def test_invalid_message_is_counted_not_sent(self):
        p = _bare_ws_producer()
        p._dispatch({**SAMPLE_TRADE_EVENT, "p": "-1"})
        p.producer.produce.assert_not_called()
        assert p._invalid_count == 1


class TestProduceWithRetry:
    def test_retries_once_after_buffer_error(self):
        producer = MagicMock()
        producer.produce.side_effect = [BufferError("full"), None]
        assert produce_with_retry(producer, "raw-ohlcv", b"{}", b"BTCUSDT") is True
        assert producer.produce.call_count == 2
        producer.flush.assert_called_once()

    def test_returns_false_when_retry_fails(self):
        producer = MagicMock()
        producer.produce.side_effect = BufferError("full")
        assert produce_with_retry(producer, "raw-ohlcv", b"{}", b"BTCUSDT") is False


class TestReconnectBackoff:
    def _run(self, connects: bool, sessions: int = 4):
        p = _bare_ws_producer()
        delays = []

        async def fake_listen(url):
            if connects:
                p._connected = True
            if len(delays) + 1 >= sessions:
                p._stop = True
            raise websockets.ConnectionClosedError(None, None)

        async def fake_sleep(seconds):
            delays.append(seconds)

        p._listen = fake_listen
        with patch("ingestion.binance_ws_producer.asyncio.sleep", fake_sleep):
            asyncio.run(p._run_with_backoff())
        return delays

    def test_backoff_resets_after_successful_session(self):
        assert set(self._run(connects=True)) == {WS_RECONNECT_INITIAL_DELAY}

    def test_backoff_grows_while_connection_keeps_failing(self):
        delays = self._run(connects=False)
        assert delays == sorted(delays) and delays[-1] > delays[0]
