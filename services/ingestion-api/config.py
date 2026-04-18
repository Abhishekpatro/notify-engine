from dotenv import load_dotenv
import os

# Load from .env in development. In production (ECS/K8s),
# these are injected as container environment variables.
load_dotenv()

# PostgreSQL — matches docker-compose.yml service definition
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "notify_db")
DB_USER = os.getenv("DB_USER", "notify_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "notify_pass")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Kafka — bootstrap_servers is the initial contact point for
# cluster discovery. Single broker in dev, multi-broker in prod.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_EVENTS_RAW = os.getenv("KAFKA_TOPIC_EVENTS_RAW", "events.raw")

# App
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "8000"))
APP_ENV = os.getenv("APP_ENV", "development")