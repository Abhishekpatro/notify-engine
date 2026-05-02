from dotenv import load_dotenv
import os

load_dotenv()

# PostgreSQL
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "notify_db")
DB_USER = os.getenv("DB_USER", "notify_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "notify_pass")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_EVENTS_RAW = os.getenv("KAFKA_TOPIC_EVENTS_RAW", "events.raw")
KAFKA_TOPIC_NOTIFICATIONS_OUT = os.getenv("KAFKA_TOPIC_NOTIFICATIONS_OUT", "notifications.outbound")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "router-group")

# Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# App
APP_ENV = os.getenv("APP_ENV", "development")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")