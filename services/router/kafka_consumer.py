from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS_RAW,
    KAFKA_CONSUMER_GROUP
)

logger = logging.getLogger(__name__)

# Module-level singleton consumer
_consumer = None


def get_consumer() -> KafkaConsumer:
    # Returns the singleton Kafka consumer. Created once, reused forever.
    global _consumer
    
    if _consumer is None:
        _consumer = KafkaConsumer(
            KAFKA_TOPIC_EVENTS_RAW,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            
            # Consumer group — multiple routers can join the same group,
            # and Kafka automatically partitions the work between them.
            # If you have 3 routers and 3 partitions, each router reads 1 partition.
            group_id=KAFKA_CONSUMER_GROUP,
            
            # Value deserializer — Kafka gives us JSON bytes, we parse to dict
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
            
            # auto_offset_reset — if this consumer group has never read before,
            # start from the earliest message in the topic (not from the end).
            # "latest" would skip everything and only read new messages going forward.
            auto_offset_reset="earliest",
            
            # enable_auto_commit — automatically commit offsets after processing.
            # If False, you manually commit (more control, more complexity).
            enable_auto_commit=True,
            
            # auto_commit_interval_ms — how often to commit offsets (every 5 seconds)
            auto_commit_interval_ms=5000,
            
            # max_poll_records — how many messages to fetch at once (1 = process one at a time)
            max_poll_records=1,
            
            # session_timeout_ms — if Kafka doesn't hear from this consumer for 30 seconds,
            # consider it dead and rebalance partitions to other consumers
            session_timeout_ms=30000,
        )
        logger.info(f"Kafka consumer created for group '{KAFKA_CONSUMER_GROUP}'")
    
    return _consumer


def read_messages():
    # Generator that yields messages one at a time from Kafka.
    # This is the main event loop for the router.
    try:
        consumer = get_consumer()
        
        for message in consumer:
            # message.value is the deserialized JSON (already a dict)
            # message.offset tells us which offset in the partition this is
            # message.partition tells us which partition it came from
            
            if message.value is None:
                logger.warning(f"Received null message at offset {message.offset}")
                continue
            
            yield {
                "data": message.value,
                "offset": message.offset,
                "partition": message.partition,
                "timestamp": message.timestamp,
            }
    
    except KafkaError as e:
        logger.error(f"Kafka consumer error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error reading messages: {e}")
        raise


def close_consumer():
    # Called on shutdown — close the consumer connection cleanly
    global _consumer
    if _consumer:
        _consumer.close()
        _consumer = None
        logger.info("Kafka consumer closed")