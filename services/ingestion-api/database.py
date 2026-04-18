import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from config import DATABASE_URL

logger = logging.getLogger(__name__)


def get_connection():
    # RealDictCursor returns rows as dicts instead of tuples.
    # Allows row["event_id"] access pattern throughout the codebase.
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    # Runs at service startup. Idempotent — safe to call on every restart.
    # In a larger system this would be replaced by Alembic migrations.
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id          SERIAL PRIMARY KEY,
                event_id    VARCHAR(255) UNIQUE NOT NULL,
                event_type  VARCHAR(100) NOT NULL,
                user_id     VARCHAR(100) NOT NULL,
                payload     JSONB DEFAULT '{}',
                timestamp   TIMESTAMP NOT NULL,
                source      VARCHAR(100) NOT NULL,
                received_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # Covering indexes for the three most common query patterns.
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_event_id
            ON events(event_id);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_user_id
            ON events(user_id);
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_event_type
            ON events(event_type);
        """)

        conn.commit()
        logger.info("Database initialized successfully")

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def insert_event(event: dict) -> bool:
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # ON CONFLICT DO NOTHING provides DB-level idempotency as a
        # safety net. Primary deduplication happens in the router via Redis.
        cursor.execute("""
            INSERT INTO events
                (event_id, event_type, user_id, payload, timestamp, source)
            VALUES
                (%(event_id)s, %(event_type)s, %(user_id)s,
                 %(payload)s, %(timestamp)s, %(source)s)
            ON CONFLICT (event_id) DO NOTHING;
        """, event)

        conn.commit()
        return True

    except Exception as e:
        logger.error(f"Failed to insert event {event.get('event_id')}: {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_event_by_id(event_id: str):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM events WHERE event_id = %s", (event_id,)
        )
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Failed to fetch event {event_id}: {e}")
        return None
    finally:
        if conn:
            conn.close()