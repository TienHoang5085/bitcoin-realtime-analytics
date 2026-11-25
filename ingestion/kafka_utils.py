"""
ingestion/kafka_utils.py
Helper gửi bản tin vào Kafka có xử lý BufferError (hàng đợi local của
librdkafka bị đầy) để không làm mất bản tin một cách âm thầm.
"""

import logging
from typing import Callable, Optional

from confluent_kafka import KafkaException, Producer

logger = logging.getLogger("KafkaUtils")

# Thời gian tối đa (giây) chờ giải phóng hàng đợi trước khi gửi lại
BUFFER_FULL_FLUSH_TIMEOUT: float = 10.0


def produce_with_retry(
    producer: Producer,
    topic: str,
    value: bytes,
    key: bytes,
    callback: Optional[Callable] = None,
) -> bool:
    """
    Gọi `producer.produce()`; nếu hàng đợi local đầy (BufferError) thì flush
    rồi gửi lại đúng 1 lần. Trả về True nếu bản tin đã vào hàng đợi.

    Logic gửi (topic, key, value, partition do Kafka tự chọn theo key)
    giữ nguyên như trước, chỉ bổ sung bước gửi lại.
    """
    try:
        producer.produce(topic=topic, value=value, key=key, callback=callback)
        return True
    except BufferError:
        logger.warning("Kafka local queue full (topic=%s) – flushing then retrying once.", topic)
        producer.flush(timeout=BUFFER_FULL_FLUSH_TIMEOUT)
    except KafkaException as exc:
        logger.error("Kafka produce error (topic=%s): %s", topic, exc)
        return False

    try:
        producer.produce(topic=topic, value=value, key=key, callback=callback)
        return True
    except (BufferError, KafkaException) as exc:
        logger.error("Message dropped after retry (topic=%s key=%s): %s",
                     topic, key.decode("utf-8", "replace"), exc)
        return False
