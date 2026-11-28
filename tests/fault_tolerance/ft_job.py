"""Run the real streaming job on the isolated fault-tolerance copy.

Same code path as production (unified_streaming.main); only the topic, the
alert topic and the trigger are changed so the test never touches real data.
"""
import logging
import os

from spark.jobs import unified_streaming as job
from spark.utils.spark_session import checkpoint_path, delta_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
logging.getLogger("py4j").setLevel(logging.ERROR)

assert "/ft/" in delta_path("bronze", "ohlcv") and "/ft/" in checkpoint_path("x"), "not an ft path"
assert os.environ["MONGO_DB"] == "crypto_analytics_ft", "not the ft database"
job.KAFKA_TOPICS = "ft-test"
job.ALERT_TOPIC = "ft-alerts"
job.TRIGGER_INTERVAL = os.environ.get("FT_TRIGGER", "1 minute")
job.main()
