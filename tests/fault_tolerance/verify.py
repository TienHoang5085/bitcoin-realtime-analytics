"""Read-only reconciliation of the fault-tolerance run.

Recomputes Silver, Gold and their indicators from Bronze with the job's own
functions and compares them with what the pipeline stored in Delta and MongoDB.
Prints RESULT lines; any mismatch, missing or duplicate count above 0 is a failure.
"""
import os

from pymongo import MongoClient
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from spark.jobs import unified_streaming as job
from spark.udfs.indicator_udfs import udf_classify_candle
from spark.utils.spark_session import delta_path

spark = SparkSession.builder.appName("ft-verify").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
assert "/ft/" in delta_path("gold", "ohlcv"), "not an ft path"
read = lambda layer: spark.read.format("delta").load(delta_path(layer, "ohlcv")).coalesce(4)  # noqa: E731
b, s, g = read("bronze").cache(), read("silver").cache(), read("gold").cache()
KEY, GKEY = job.CANDLE_KEY, job.GOLD_KEY


def differs(a, t):
    return ~((a.isNull() & t.isNull()) | (a.isNotNull() & t.isNotNull() & (F.abs(a - t) <= F.abs(t) * 1e-6 + 1e-6)))


print("RESULT bronze rows", b.count(), "| null lineage", b.filter("_kafka_offset is null").count(),
      "| duplicated (topic, partition, offset)",
      b.filter("_kafka_offset is not null")
      .groupBy("_kafka_topic", "_kafka_partition", "_kafka_offset").count().filter("count > 1").count())
print("RESULT bronze ft-test messages", b.filter("_kafka_topic = 'ft-test'").count())

closed = job._latest_closed_candles(b).cache()
print("RESULT silver rows", s.count(), "distinct", s.select(KEY).distinct().count(), "expected", closed.count(),
      "missing", closed.join(s, KEY, "left_anti").count(), "unexpected", s.join(closed, KEY, "left_anti").count())
truth_s = job.transform_silver(closed)
j = s.alias("s").join(truth_s.alias("t"), KEY).cache()
for c in ["close", "volume", "ma7", "ma25", "ma99", "rsi_14", "macd", "macd_signal", "atr_14", "volume_ratio"]:
    print(f"RESULT silver mismatch {c}:", j.filter(differs(F.col(f"s.{c}"), F.col(f"t.{c}"))).count(), flush=True)

frames = []
for tf, secs in job.RESAMPLE_INTERVALS.items():
    frames.append(
        closed.groupBy("symbol", F.window("open_time", job._seconds_to_duration(secs)).alias("w"))
        .agg(F.min_by("open", "open_time").alias("open"), F.max("high").alias("high"),
             F.min("low").alias("low"), F.max_by("close", "open_time").alias("close"),
             F.sum("volume").alias("volume"), F.sum("quote_volume").alias("quote_volume"),
             F.sum("trade_count").alias("trade_count"))
        .select("symbol", F.lit(tf).alias("timeframe"), F.col("w.start").alias("window_start"),
                "open", "high", "low", "close", "volume", "quote_volume", "trade_count"))
truth = frames[0]
for f in frames[1:]:
    truth = truth.unionByName(f)
win = Window.partitionBy("symbol", "timeframe").orderBy("window_start")
truth = job._add_price_indicators(truth, win).withColumn(
    "candle_pattern", udf_classify_candle("open", "high", "low", "close"))
tp = (F.col("high") + F.col("low") + F.col("close")) / 3
truth = (truth
         .withColumn("vwap", F.sum(tp * F.col("volume")).over(win.rowsBetween(-20, 0))
                     / F.sum("volume").over(win.rowsBetween(-20, 0)))
         .withColumn("volume_ratio", F.col("volume") / F.avg("volume").over(win.rowsBetween(-19, 0)))
         .cache())
print("RESULT gold rows", g.count(), "distinct", g.select(GKEY).distinct().count(), "expected", truth.count(),
      "missing", truth.join(g, GKEY, "left_anti").count(), "unexpected", g.join(truth, GKEY, "left_anti").count())
print("RESULT gold invalid candles", g.filter("open > high or open < low or close > high or close < low").count())
jg = g.alias("g").join(truth.alias("t"), GKEY).cache()
for c in ["open", "high", "low", "close", "volume", "quote_volume", "trade_count", "vwap", "volume_ratio",
          "ma7", "ma25", "rsi_14", "macd", "macd_signal", "bb_upper", "bb_lower", "atr_14"]:
    print(f"RESULT gold mismatch {c}:", jg.filter(differs(F.col(f"g.{c}"), F.col(f"t.{c}"))).count(), flush=True)
print("RESULT gold mismatch candle_pattern:",
      jg.filter(~F.col("g.candle_pattern").eqNullSafe(F.col("t.candle_pattern"))).count())

col = MongoClient(job.MONGO_URI)[os.environ["MONGO_DB"]]["gold_ohlcv"]
docs = list(col.find({}, {"_id": 0}))
delta = {(r.symbol, r.timeframe, r.window_start): (r.close, r.volume, r.ma7, r.rsi_14) for r in g.collect()}


def same(x, y):
    return (x is None and y is None) or (x is not None and y is not None and abs(x - y) < 1e-6)


bad = sum(1 for d in docs
          if (d["symbol"], d["timeframe"], d["window_start"]) not in delta
          or not all(same(x, d.get(f)) for x, f in zip(
              delta[(d["symbol"], d["timeframe"], d["window_start"])], ("close", "volume", "ma7", "rsi_14"))))
print("RESULT mongo docs", len(docs), "keys", len({(d["symbol"], d["timeframe"], d["window_start"]) for d in docs}),
      "delta rows", len(delta), "docs differing", bad)
spark.stop()
