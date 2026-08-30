# Real-Time Customer Heart Beat Monitoring System

Status: **Phase 1 — Project Foundation** complete.

A synthetic heart-rate monitoring pipeline: Generator → Kafka Producer → Kafka → Kafka Consumer → Validation/Anomaly Detection → PostgreSQL.

This README will be filled out in full during the documentation phase (architecture diagrams, setup guide, SQL queries, troubleshooting, etc). For now:

## Quickstart (Phase 1 only — infra + config, no pipeline yet)

```bash
cp .env.example .env
make install
make up          # starts Kafka + PostgreSQL via Docker Compose
make test-unit   # confirms configuration loads correctly
```
