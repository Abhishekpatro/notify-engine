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

**Current Phase:** Week 1 Complete ✅ | Week 2 In Progress 🚧

### Completed

- ✅ **Infrastructure** — Kafka, PostgreSQL, Redis, Prometheus, Grafana running in Docker
- ✅ **Ingestion API** — FastAPI service with Pydantic validation, PostgreSQL persistence, Kafka producer
- ✅ **Router Service** — Kafka consumer with Redis deduplication, rate limiting, and PostgreSQL routing rules
- ✅ **End-to-End Flow** — Events flow from ingestion → Kafka → router → routing decisions logged to DB

### In Progress

- 🚧 **Docker Networking Fix** — Resolving ingestion-api containerization issue (router works, ingestion-api tested locally)
- 🚧 **Delivery Worker** — Next up: consume from notifications.outbound, deliver via webhook/email

### Upcoming

- ⏳ **Monitoring Dashboards** — Grafana panels for throughput, latency, consumer lag
- ⏳ **CI/CD Pipeline** — GitHub Actions: test → lint → build → deploy
- ⏳ **Kubernetes Deployment** — Helm charts, HPA autoscaling, pod resource limits
- ⏳ **Terraform IaC** — Provision AWS ECS, RDS, ElastiCache, VPC

| Week   | Milestone                                       | Status         |
| ------ | ----------------------------------------------- | -------------- |
| Week 1 | Scaffold, Infrastructure, Ingestion API, Router | ✅ Complete    |
| Week 2 | Delivery Worker, Redis Dedup, Full Event Flow   | 🚧 In Progress |
| Week 3 | Observability, CI/CD, Terraform                 | ⏳ Planned     |
| Week 4 | Kubernetes, Load Testing, Production Polish     | ⏳ Planned     |

**Last Updated:** May 2, 2026 — Day 5

---

## Recent Commits

- `feat: add router service - Kafka consumer with Redis dedup, rate limiting, and routing rules`
- `feat: dockerize ingestion-api and add to docker-compose with healthcheck`
- `feat: add ingestion-api service with FastAPI, Kafka producer, PostgreSQL, and Prometheus metrics`
- `feat: add docker-compose with Kafka, PostgreSQL, Redis, Prometheus, Grafana`
- `feat: initial project scaffold, folder structure, and README`

---
