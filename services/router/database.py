import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from config import DATABASE_URL

logger = logging.getLogger(__name__)


def get_connection():
    # Open a PostgreSQL connection. RealDictCursor returns rows as dicts.
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def get_routing_rules(event_type: str) -> list:
    # Load routing rules from PostgreSQL for a specific event_type.
    # Returns all matching rules sorted by priority (highest first).
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT rule_id, event_type, channel, template_id, priority, enabled
            FROM routing_rules
            WHERE event_type = %s AND enabled = TRUE
            ORDER BY priority DESC
        """, (event_type,))
        
        rules = cursor.fetchall()
        return [dict(row) for row in rules] if rules else []
    
    except Exception as e:
        logger.error(f"Failed to fetch routing rules for {event_type}: {e}")
        return []
    finally:
        if conn:
            conn.close()


def log_routing_decision(decision: dict) -> bool:
    # Log the routing decision to PostgreSQL for audit trail.
    # Tracks: which event was routed, which channel, when, any errors.
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO routing_decisions
                (event_id, user_id, event_type, channel, rule_id, routed_at, status, error_message)
            VALUES
                (%(event_id)s, %(user_id)s, %(event_type)s, %(channel)s,
                 %(rule_id)s, NOW(), %(status)s, %(error_message)s)
        """, decision)
        
        conn.commit()
        return True
    
    except Exception as e:
        logger.error(f"Failed to log routing decision: {e}")
        return False
    finally:
        if conn:
            conn.close()


def init_db():
    # Called at startup — create tables if they don't exist.
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Routing rules table — stores the rules that decide how to route each event type
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS routing_rules (
                rule_id     VARCHAR(255) PRIMARY KEY,
                event_type  VARCHAR(100) NOT NULL,
                channel     VARCHAR(50) NOT NULL,
                template_id VARCHAR(255) NOT NULL,
                priority    INTEGER DEFAULT 0,
                enabled     BOOLEAN DEFAULT TRUE,
                created_at  TIMESTAMP DEFAULT NOW()
            );
        """)
        
        # Routing decisions table — audit log of what the router did
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS routing_decisions (
                id             SERIAL PRIMARY KEY,
                event_id       VARCHAR(255) NOT NULL,
                user_id        VARCHAR(100) NOT NULL,
                event_type     VARCHAR(100) NOT NULL,
                channel        VARCHAR(50) NOT NULL,
                rule_id        VARCHAR(255),
                routed_at      TIMESTAMP NOT NULL,
                status         VARCHAR(50),
                error_message  TEXT,
                created_at     TIMESTAMP DEFAULT NOW()
            );
        """)
        
        # Create indexes for fast lookups
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_routing_rules_event_type
            ON routing_rules(event_type);
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_routing_decisions_event_id
            ON routing_decisions(event_id);
        """)
        
        conn.commit()
        logger.info("Database initialized successfully")
    
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    finally:
        if conn:
            conn.close()