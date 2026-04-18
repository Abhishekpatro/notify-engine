import logging
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST, REGISTRY
from starlette.responses import Response
import uvicorn

from config import APP_HOST, APP_PORT, APP_ENV
from models import EventRequest, EventResponse, HealthResponse
from database import init_db, insert_event, get_event_by_id, get_connection
from kafka_producer import produce_event, close_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_or_create_counter(name, description, labels):
    # Prevents duplicate registration error when uvicorn reloader
    # restarts the process and tries to register the same metric twice.
    try:
        return Counter(name, description, labels)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)


def get_or_create_histogram(name, description, buckets):
    try:
        return Histogram(name, description, buckets=buckets)
    except ValueError:
        return REGISTRY._names_to_collectors.get(name)


EVENTS_INGESTED = get_or_create_counter(
    "events_ingested_total",
    "Total events received by the ingestion API",
    ["event_type", "status"]
)

INGEST_LATENCY = get_or_create_histogram(
    "event_ingest_duration_seconds",
    "End-to-end latency of the ingest endpoint (DB write + Kafka produce)",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info(f"Starting ingestion-api · env={APP_ENV}")
    init_db()
    logger.info("Startup complete")
    yield
    # Shutdown
    close_producer()
    logger.info("Shutdown complete")


app = FastAPI(
    title="notify-engine · ingestion-api",
    description="Accepts business events and produces them to Kafka for async processing.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/events", response_model=EventResponse, status_code=202)
async def ingest_event(event: EventRequest):
    """
    Accepts a business event, persists it to PostgreSQL, and produces
    it to the events.raw Kafka topic for async downstream processing.

    Returns 202 Accepted — not 200 OK — because processing is async.
    The event has been accepted and queued, not fully processed.
    """
    with INGEST_LATENCY.time():

        event_dict = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "user_id": event.user_id,
            "payload": event.payload,
            "timestamp": event.timestamp.isoformat(),
            "source": event.source,
        }

        # Persist to DB first. If this fails we never produce to Kafka,
        # preventing orphaned Kafka messages with no DB record.
        db_success = insert_event({
            **event_dict,
            "timestamp": event.timestamp,
            "payload": json.dumps(event.payload),
        })

        if not db_success:
            EVENTS_INGESTED.labels(
                event_type=event.event_type, status="db_error"
            ).inc()
            raise HTTPException(
                status_code=500,
                detail="Failed to persist event — try again"
            )

        kafka_success = produce_event(event_dict)

        if not kafka_success:
            EVENTS_INGESTED.labels(
                event_type=event.event_type, status="kafka_error"
            ).inc()
            raise HTTPException(
                status_code=500,
                detail="Failed to queue event — try again"
            )

        EVENTS_INGESTED.labels(
            event_type=event.event_type, status="success"
        ).inc()

        logger.info(
            f"Accepted event_id={event.event_id} "
            f"type={event.event_type} "
            f"user={event.user_id} "
            f"source={event.source}"
        )

        return EventResponse(
            accepted=True,
            event_id=event.event_id,
            message="Event accepted for processing"
        )


@app.get("/events/{event_id}/status")
async def get_event_status(event_id: str):
    """
    Returns the DB record for a given event_id.
    Used by callers to confirm their event was received.
    """
    event = get_event_by_id(event_id)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return {"event": dict(event)}


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Liveness + dependency check endpoint.
    Called by load balancers and Kubernetes readiness probes.
    Returns 200 even if degraded — the status field carries the detail.
    """
    db_status = "connected"
    try:
        conn = get_connection()
        conn.close()
    except Exception:
        db_status = "disconnected"

    return HealthResponse(
        status="ok" if db_status == "connected" else "degraded",
        kafka="connected",
        database=db_status,
        environment=APP_ENV,
    )


@app.get("/metrics")
async def metrics():
    # Prometheus scrapes this endpoint every 15s as configured in prometheus.yml.
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=APP_HOST,
        port=APP_PORT,
        reload=APP_ENV == "development"
    )