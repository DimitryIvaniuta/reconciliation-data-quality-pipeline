# Environment and approach research

Research date: 18 July 2026.

## Selected versions

- Java 25 LTS
- Spring Boot 4.1.0 / Spring Framework 7
- Gradle 9.6.1
- Apache Kafka 4.3.1 in KRaft mode
- PostgreSQL 18.4
- Redis 8.8
- Lombok 1.18.46
- Testcontainers 2.0.5
- JaCoCo 0.8.14
- Checkstyle 13.7.0

## Primary references

- Spring Boot releases and system requirements: https://spring.io/projects/spring-boot and https://docs.spring.io/spring-boot/system-requirements.html
- Java 25 release: https://openjdk.org/projects/jdk/25/
- Gradle releases, wrapper, and dependency submission: https://gradle.org/releases/, https://docs.gradle.org/current/userguide/gradle_wrapper.html, and https://github.com/gradle/actions
- Kafka downloads, KRaft, and timestamp offset lookup: https://kafka.apache.org/downloads, https://kafka.apache.org/documentation/#kraft, and Kafka Admin `OffsetSpec.forTimestamp`
- PostgreSQL releases: https://www.postgresql.org/support/versioning/
- Redis releases: https://github.com/redis/redis/releases
- Spring Data R2DBC reference: https://docs.spring.io/spring-data/relational/reference/r2dbc.html

## Findings implemented in version 2.0

### Separate source occurrence from event identity

A Kafka position and a business event ID answer different questions. The original schema made the unique event ledger represent both, which silently collapsed duplicate event IDs. The upgraded schema stores one immutable source observation per topic/partition/offset and one immutable event per event ID. This supports independent duplicate detection and retains original source lineage.

### Reconcile both count and value

Equal row counts do not prove equal business results. The upgraded compact metrics independently maintain the total amount from business rows and the aggregate amount. Reconciliation compares them using a configurable decimal tolerance.

### Persist source evidence

The daily source count is now returned as a partition snapshot rather than only a scalar. Reports store each half-open offset range, making an incident reproducible while Kafka retention still covers the date.

### Resume replay from durable checkpoints

A single date-range status is insufficient for large backfills. Each replay now stores immutable partition bounds and mutable next-offset/counter/heartbeat state. A stale worker can resume without rescanning completed partitions or changing the source snapshot. The execution attempt acts as a database fencing token, so an old worker cannot update progress after recovery starts a new attempt.

### Make recovery bounded and auditable

Replay attempts are limited, stale-running lookup is indexed, progress invariants are enforced by database checks, and command publication remains recoverable. Sensitive business-row and aggregate mutations are recorded in an append-only audit table.

### Use a transactional alert outbox

A persisted mismatch without a durable alert intent can be lost when the process crashes between the database commit and transport send. Version 2 inserts report evidence and per-channel pending alert rows in one reactive transaction. Delivery attempts are timed, audited, retried in bounded batches, and protected by per-alert distributed locks. Kafka keys and webhook idempotency headers let downstream consumers deduplicate at-least-once delivery.

### Preserve low database load

Kafka timestamp-to-offset lookup remains proportional to partition count. PostgreSQL triggers maintain exact compact state from real inserts/deletes/aggregate changes. Full indexed scans occur only during explicit repair/backfill.

### Harden build and deployment

The project requires a runtime API key rather than shipping a base-profile default, uses a checksum-pinned Gradle distribution and wrapper bootstrap, validates Compose in CI, submits the Gradle dependency graph, uses a read-only application container filesystem, and exposes a readiness health check.
