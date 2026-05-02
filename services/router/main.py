import logging
import json
import uuid
from datetime import datetime, timedelta
import redis
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

from config import (
    APP_ENV,
    LOG_LEVEL,
    KAFKA_BOOTSTRAP_SERVERS,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
)
from models import RoutingRule, Event, Notification
from database import get_routing_rules, log_routing_decision, init_db
from kafka_consumer import read_messages, close_consumer

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Prometheus metrics
EVENTS_ROUTED = Counter(
    "events_routed_total",
    "Total events routed by the router",
    ["event_type", "channel", "status"]
)

ROUTING_LATENCY = Histogram(
    "routing_latency_seconds",
    "Time to route one event (load rules, dedup, rate limit, produce)",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

DEDUP_HITS = Counter(
    "dedup_hits_total",
    "Number of duplicate events caught by Redis dedup",
    ["event_type"]
)

RATE_LIMIT_DROPS = Counter(
    "rate_limit_drops_total",
    "Number of notifications dropped due to rate limiting",
    ["user_id"]
)

# Redis for dedup and rate limiting
redis_client = None


def init_redis():
    global redis_client
    try:
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_connect_timeout=5
        )
        redis_client.ping()
        logger.info("Redis connected successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise


def is_duplicate(event_id: str) -> bool:
    # Check if we've already processed this event_id.
    # Redis SET NX returns True if the key didn't exist (new event).
    # Returns False if the key existed (duplicate).
    try:
        result = redis_client.set(
            f"dedup:{event_id}",
            "1",
            nx=True,  # Only set if key doesn't exist
            ex=86400  # Expire after 24 hours
        )
        return not result  # True if already existed (duplicate)
    except Exception as e:
        logger.error(f"Redis dedup check failed for {event_id}: {e}")
        # On Redis error, assume not duplicate (fail open, not closed)
        return False


def check_rate_limit(user_id: str, max_per_minute: int = 10) -> bool:
    # Check if user has already received max_per_minute notifications in the last 60 seconds.
    # Uses Redis sorted sets with timestamps.
    # Returns True if user is over limit, False if under.
    try:
        now = datetime.utcnow().timestamp()
        window_start = now - 60  # Last 60 seconds
        
        # Count notifications in the last 60 seconds
        key = f"rate_limit:{user_id}"
        count = redis_client.zcount(key, window_start, now)
        
        if count >= max_per_minute:
            return True  # Over limit
        
        # Add this notification to the sorted set
        redis_client.zadd(key, {str(now): now})
        # Set expiry on the key
        redis_client.expire(key, 60)
        
        return False  # Under limit
    
    except Exception as e:
        logger.error(f"Rate limit check failed for {user_id}: {e}")
        return False


def route_event(event_data: dict) -> bool:
    # Main routing logic. Returns True if successfully routed, False otherwise.
    with ROUTING_LATENCY.time():
        try:
            # Parse the event from Kafka
            event = Event(**event_data)
            
            # Step 1: Check deduplication
            if is_duplicate(event.event_id):
                logger.info(f"Duplicate event detected: {event.event_id}")
                DEDUP_HITS.labels(event_type=event.event_type).inc()
                log_routing_decision({
                    "event_id": event.event_id,
                    "user_id": event.user_id,
                    "event_type": event.event_type,
                    "channel": "N/A",
                    "rule_id": None,
                    "status": "dropped_duplicate",
                    "error_message": None
                })
                return True  # Consider this successful (duplicate handled)
            
            # Step 2: Load routing rules for this event type
            rules = get_routing_rules(event.event_type)
            if not rules:
                logger.warning(f"No routing rules found for event_type={event.event_type}")
                log_routing_decision({
                    "event_id": event.event_id,
                    "user_id": event.user_id,
                    "event_type": event.event_type,
                    "channel": "N/A",
                    "rule_id": None,
                    "status": "no_matching_rule",
                    "error_message": "No rules found"
                })
                return True  # No rule = no action needed (not an error)
            
            # Step 3: Apply the first matching rule (highest priority)
            rule = rules[0]  # rules are sorted by priority DESC
            channel = rule["channel"]
            
            # Step 4: Check rate limit
            if check_rate_limit(event.user_id):
                logger.warning(f"Rate limit exceeded for user {event.user_id}")
                RATE_LIMIT_DROPS.labels(user_id=event.user_id).inc()
                log_routing_decision({
                    "event_id": event.event_id,
                    "user_id": event.user_id,
                    "event_type": event.event_type,
                    "channel": channel,
                    "rule_id": rule["rule_id"],
                    "status": "dropped_rate_limit",
                    "error_message": None
                })
                return True  # Rate limited (not an error, expected behavior)
            
            # Step 5: Create notification object
            notification = Notification(
                notification_id=str(uuid.uuid4()),
                event_id=event.event_id,
                user_id=event.user_id,
                channel=channel,
                template_id=rule["template_id"],
                payload=event.payload,
                created_at=datetime.utcnow().isoformat()
            )
            
            # Step 6: Produce to notifications.outbound
            # (In Week 5 we'll add the actual Kafka producer here)
            # For now, just log it
            logger.info(
                f"Routed event_id={event.event_id} "
                f"user={event.user_id} "
                f"channel={channel} "
                f"notification_id={notification.notification_id}"
            )
            
            # Step 7: Log the routing decision
            log_routing_decision({
                "event_id": event.event_id,
                "user_id": event.user_id,
                "event_type": event.event_type,
                "channel": channel,
                "rule_id": rule["rule_id"],
                "status": "routed",
                "error_message": None
            })
            
            # Record metric
            EVENTS_ROUTED.labels(
                event_type=event.event_type,
                channel=channel,
                status="success"
            ).inc()
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to route event: {e}")
            EVENTS_ROUTED.labels(
                event_type="unknown",
                channel="unknown",
                status="error"
            ).inc()
            return False


def main():
    # Main router loop — runs forever, consuming from Kafka
    logger.info(f"Starting router in {APP_ENV} mode")
    logger.info(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    
    init_db()
    init_redis()
    
    try:
        logger.info("Router started — listening for events from Kafka")
        
        for message in read_messages():
            event_data = message["data"]
            
            logger.debug(
                f"Received event: {event_data.get('event_id')} "
                f"from partition {message['partition']} "
                f"offset {message['offset']}"
            )
            
            # Route the event
            success = route_event(event_data)
            
            if not success:
                logger.error(f"Failed to route event {event_data.get('event_id')}")
            
            # Kafka consumer auto-commits offset after each message
            # (because enable_auto_commit=True in kafka_consumer.py)
    
    except KeyboardInterrupt:
        logger.info("Router interrupted by user")
    except Exception as e:
        logger.error(f"Router crashed: {e}")
        raise
    finally:
        close_consumer()
        if redis_client:
            redis_client.close()
        logger.info("Router shutdown complete")


if __name__ == "__main__":
    main()