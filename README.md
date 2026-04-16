# notify-engine

A production-grade, event-driven notification platform built with a microservices architecture.
Simulates how companies like GitHub, Uber, and Stripe route and deliver notifications
(email, webhook, SMS) at scale.

## Architecture

Client → FastAPI Ingestion API → Kafka (events.raw) → Router / Rules Engine
→ Kafka (notifications.outbound) → Delivery Workers → Webhook / Email / Slack

Redis handles deduplication and per-user rate limiting.  
PostgreSQL stores the event log and delivery history.  
All services are independently deployable and communicate only through Kafka.

## Services

| Service           | Responsibility                                                         |
| ----------------- | ---------------------------------------------------------------------- |
| `ingestion-api`   | Accepts events via REST, validates, produces to Kafka                  |
| `router`          | Consumes events, applies routing rules, deduplicates, rate limits      |
| `delivery-worker` | Consumes notifications, delivers via webhook/email, retries on failure |

## Tech Stack

| Layer         | Technology                  | Why                                                         |
| ------------- | --------------------------- | ----------------------------------------------------------- |
| API           | FastAPI (Python 3.12)       | Async, Pydantic validation, auto-docs                       |
| Message bus   | Apache Kafka                | Durable, replayable, fault-tolerant event streaming         |
| Cache         | Redis 7                     | Sub-millisecond dedup and sliding-window rate limiting      |
| Database      | PostgreSQL 15               | ACID compliance, strong consistency                         |
| Containers    | Docker + Kubernetes         | Compose for local dev, K8s for prod-style deploy            |
| Cloud         | AWS (ECS, RDS, ElastiCache) | Managed services provisioned via Terraform                  |
| CI/CD         | GitHub Actions              | Automated test → lint → build → deploy on every merge       |
| Observability | Prometheus + Grafana        | Per-service metrics, latency histograms, Kafka consumer lag |
| Load testing  | Locust                      | Python-native, scriptable throughput benchmarking           |

## Key Design Decisions

**Why Kafka instead of direct HTTP between services?**  
Kafka is a durable log. If the router crashes mid-processing, events are not lost —
the consumer resumes from its last committed offset on restart.
Direct HTTP calls have no such guarantee.

**Why Redis for deduplication and not PostgreSQL?**  
A Redis `SET NX EX` command completes in under 1ms.
Querying PostgreSQL for every event at high throughput adds unacceptable latency.
Redis is the speed layer, PostgreSQL is the source of truth.

**Why separate router and delivery-worker?**  
They have different scaling profiles. The router is CPU-bound (rule matching, Redis lookups).
The delivery worker is I/O-bound (waiting on external HTTP endpoints).
Separating them means a slow webhook never backs up the routing pipeline.

## API

POST /events — ingest a new event (returns 202 Accepted)
GET /events/{event_id}/status — get delivery status for an event
GET /notifications?user_id=X — list notifications for a user
GET /health — service health check

## Running locally

```bash
# Coming Day 2 — full docker-compose setup
docker compose up
```

## Load test results

Coming Week 4 — Locust benchmark results
Peak throughput: TBD events/sec
p99 latency: TBD ms
Concurrent users: TBD

## Project status

| Week   | Focus                                                        | Status         |
| ------ | ------------------------------------------------------------ | -------------- |
| Week 1 | Scaffold + docker-compose + ingestion API + Kafka            | 🔨 In progress |
| Week 2 | Rules engine + Redis dedup + rate limiting + delivery worker | ⏳ Pending     |
| Week 3 | Observability + GitHub Actions CI/CD + Terraform             | ⏳ Pending     |
| Week 4 | Kubernetes + load testing + polish                           | ⏳ Pending     |

---
