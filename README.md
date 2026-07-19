# Reconciliation + Data Quality Monitoring Pipeline

Production-grade reactive Java service that detects missing Kafka source records, duplicate event IDs, missing PostgreSQL projections, aggregate count drift, and aggregate amount drift. It stores immutable evidence, delivers alerts through a transactional alert outbox, and supports resumable idempotent replay/backfill.

## Stack (July 2026)

Java 25 LTS, Spring Boot 4.1.0, WebFlux, R2DBC, PostgreSQL 18.4, Flyway, Apache Kafka 4.3.1 in KRaft mode, Redis 8.8, Gradle 9.6.1, Lombok 1.18.46, Micrometer/Prometheus, JUnit 5, Mockito, Reactor Test, Testcontainers 2.0.5, JaCoCo 0.8.14, and Checkstyle 13.7.0.

Flyway intentionally uses JDBC only for deterministic startup migrations. Runtime database access is reactive.

## What is reconciled

The service compares five independently persisted checkpoints for each UTC Kafka-ingestion day:

1. **Kafka source records** — calculated from broker timestamp-to-offset boundaries for each partition.
2. **Consumed source observations** — one immutable row per Kafka topic/partition/offset.
3. **Unique business events** — one immutable ledger row per event ID.
4. **Projected database records** — one materialized business row per event ID, including an independently maintained total amount.
5. **Daily aggregate** — count and amount maintained by the projection.

This produces actionable mismatch types:

- `KAFKA_VS_SOURCE_OBSERVATIONS`
- `SOURCE_OBSERVATIONS_VS_UNIQUE_EVENTS`
- `UNIQUE_EVENTS_VS_DATABASE`
- `DATABASE_VS_AGGREGATE_COUNT`
- `DATABASE_VS_AGGREGATE_AMOUNT`

Every report includes the exact half-open Kafka offset range `[startOffset, endOffset)` for every partition, expected/actual values, signed delta, operator action, trigger type, correlation ID, and creation time.

## Low-load design

The source topic uses broker `LogAppendTime`; this broker-assigned timestamp is the authoritative reconciliation day. Kafka source counts require only timestamp-to-offset lookups per partition. PostgreSQL triggers maintain compact exact counters from real row mutations, so normal daily reconciliation reads one `daily_metrics` row instead of scanning event and business tables.

The original `BusinessEvent.eventTime` remains immutable domain data and may be delayed or out of order without changing the reconciliation day.

## Durable alerts

A mismatch report and one `PENDING` outbox row per configured channel are inserted in the same reactive PostgreSQL transaction. Immediate Kafka/webhook delivery updates the durable attempt count and status. A bounded recovery scheduler retries stale `PENDING` or `FAILED` rows after crashes or transient outages. Kafka uses the alert ID as its message key, and webhooks receive an `Idempotency-Key` header based on the report ID so receivers can deduplicate at-least-once delivery.

## Replay/backfill

Replay requests are durable and bound to `Idempotency-Key`. A Kafka command wakes a worker, while a recovery scheduler republishes undispatched commands and recovers stale workers.

Each job stores an immutable per-partition source snapshot containing start/end offsets and mutable progress containing next offset, discovered records, repaired records, status, and heartbeat. A crashed worker resumes from its last durable checkpoint without widening the source range. Every progress write carries the current execution-attempt fence, so a recovered stale worker cannot advance a newer attempt.

Real replay synchronously applies the same validated idempotent projector using the original Kafka coordinates and timestamp. It then rebuilds exact aggregates only for requested indexed dates and creates `REPLAY_VALIDATION` reports. Dry-run replay only counts candidates.

## Start locally

Prerequisites: Java 25, Docker Compose, and outbound access for the first Gradle dependency download.

Start infrastructure:

```bash
cp .env.example .env
docker compose up -d postgres redis kafka
```

Run the application with the explicit local profile:

```bash
SPRING_PROFILES_ACTIVE=local ./gradlew clean bootRun
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
docker compose up -d postgres redis kafka
$env:SPRING_PROFILES_ACTIVE = "local"
.\gradlew.bat clean bootRun
```

Run the entire stack in containers after setting a non-default API key in `.env`:

```bash
docker compose --profile app up --build
```

The local profile uses `local-development-key-change-me`. Never enable it in a deployed environment. The base configuration requires `APP_SECURITY_API_KEY` and has no fallback secret.

## API

All `/api/**` requests require `X-API-Key`.

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/v1/events` | Publish a validated source event |
| GET | `/api/v1/daily-metrics/{date}` | Read compact independent counters and amounts |
| POST | `/api/v1/reconciliations/{date}/runs` | Run reconciliation |
| GET | `/api/v1/reconciliations/{reportId}` | Read one immutable report |
| GET | `/api/v1/reconciliations` | Filter and page reports |
| POST | `/api/v1/replays` | Request dry-run or real replay |
| GET | `/api/v1/replays/{jobId}` | Read replay lifecycle and totals |
| GET | `/api/v1/replays/{jobId}/checkpoints` | Read per-partition source bounds and progress |
| POST | `/api/v1/replays/{jobId}/retry` | Retry a failed replay within the attempt limit |
| GET | `/actuator/health` | Liveness/readiness probes |
| GET | `/actuator/prometheus` | Prometheus metrics |

## Postman

Import:

- `postman/Reconciliation-Pipeline.postman_collection.json`
- `postman/Local.postman_environment.json`

The collection covers ingestion, metrics, reconciliation evidence, report search, replay request/status/checkpoints, and the explicit failed-job retry endpoint.

## Verification

```bash
./gradlew --no-daemon clean check integrationTest bootJar javadoc
./scripts/verify-project.sh
```

The build enforces Checkstyle, Javadoc generation, service-layer JaCoCo coverage, unit tests, Docker-backed PostgreSQL migration/projection tests, executable JAR creation, and source/Javadoc JAR creation. GitHub Actions also validates Compose, submits the dependency graph, and uploads verification reports.

See `VERIFICATION.md` for exactly what was executable in the artifact environment and what is delegated to the Java 25/Docker CI runner.

## Repository

**Name:** `reconciliation-data-quality-pipeline`

**Description:** Reactive reconciliation and data-quality monitoring with independent Kafka/PostgreSQL evidence, actionable immutable reports, audited alerts, and resumable idempotent replay.

See `REPOSITORY.md` for publishing commands and suggested topics.

## License

MIT.
