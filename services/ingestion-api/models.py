from pydantic import BaseModel, Field
from typing import Dict, Any
from datetime import datetime
import uuid


class EventRequest(BaseModel):
    # Auto-generated if not provided by caller.
    # Used downstream by the router for idempotent deduplication.
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    # Dot-namespaced convention: "<domain>.<action>"
    # e.g. "user.signup", "order.shipped", "payment.failed"
    event_type: str = Field(..., min_length=1, max_length=100)

    user_id: str = Field(..., min_length=1, max_length=100)

    # Arbitrary event-specific data. Schema varies by event_type.
    payload: Dict[str, Any] = Field(default_factory=dict)

    # When the event occurred in the source system.
    # Distinct from received_at (ingestion time) — used to detect delayed events.
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    # Originating service. e.g. "auth-service", "payments-service"
    # Used for traceability and debugging.
    source: str = Field(..., min_length=1, max_length=100)

    model_config = {
        "json_schema_extra": {
            "example": {
                "event_type": "user.signup",
                "user_id": "user_123",
                "payload": {"email": "user@example.com", "plan": "pro"},
                "source": "auth-service"
            }
        }
    }


class EventResponse(BaseModel):
    accepted: bool
    event_id: str
    message: str


class HealthResponse(BaseModel):
    # "ok" | "degraded" — degraded means running but a dependency is down
    status: str
    kafka: str      # "connected" | "disconnected"
    database: str   # "connected" | "disconnected"
    environment: str