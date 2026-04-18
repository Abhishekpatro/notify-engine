from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS_RAW

logger = logging.getLogger(__name__)

# Module-level singleton. One producer instance is reused across all requests.
# KafkaProducer is thread-safe and expensive to initialize — never create
# per-request instances.
_producer = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

            # Serialize event dicts to UTF-8 JSON bytes before sending.
            # default=str handles non-serializable types like datetime.
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),

            # Partition key — events for the same user always go to the
            # same partition, preserving per-user ordering guarantees.
            key_serializer=lambda k: k.encode("utf-8") if k else None,

            # acks="all" — leader AND all in-sync replicas must acknowledge
            # before the send is considered successful. Strongest durability guarantee.
            acks="all",

            retries=3,

            # Max time to block on send() if the buffer is full.
            max_block_ms=5000,
        )
        logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
    return _producer


def produce_event(event: dict) -> bool:
    try:
        producer = get_producer()

        future = producer.send(
            KAFKA_TOPIC_EVENTS_RAW,
            key=event.get("user_id"),   # routes to consistent partition per user
            value=event,
        )

        # flush() ensures the message is actually sent before we return.
        # Without this, messages sit in an internal buffer and may not
        # be sent before the function returns.
        producer.flush()

        record_metadata = future.get(timeout=5)
        logger.info(
            f"Event {event.get('event_id')} produced → "
            f"topic={record_metadata.topic} "
            f"partition={record_metadata.partition} "
            f"offset={record_metadata.offset}"
        )
        return True

    except KafkaError as e:
        logger.error(f"Kafka error producing event {event.get('event_id')}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error producing event {event.get('event_id')}: {e}")
        return False


def close_producer():
    # Called on service shutdown to flush remaining messages
    # and release the connection cleanly.
    global _producer
    if _producer:
        _producer.close()
        _producer = None
        logger.info("Kafka producer closed")