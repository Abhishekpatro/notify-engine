from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime


class RoutingRule(BaseModel):
    # Loaded from PostgreSQL — tells us how to route each event type
    rule_id: str
    event_type: str
    channel: str  # "email" | "webhook" | "slack"
    template_id: str
    priority: int  # Higher = evaluated first
    enabled: bool

    class Config:
        schema_extra = {
            "example": {
                "rule_id": "rule_001",
                "event_type": "user.signup",
                "channel": "email",
                "template_id": "welcome_email",
                "priority": 1,
                "enabled": True
            }
        }


class Event(BaseModel):
    # The event that came from Kafka (from ingestion-api)
    event_id: str
    event_type: str
    user_id: str
    payload: Dict[str, Any]
    timestamp: str  # ISO format from Kafka
    source: str


class Notification(BaseModel):
    # What the router produces to notifications.outbound
    notification_id: str  # UUID, unique per notification
    event_id: str  # Foreign key back to original event
    user_id: str
    channel: str  # Where to deliver
    template_id: str  # Which message template
    payload: Dict[str, Any]  # Event data + metadata
    created_at: str  # ISO timestamp