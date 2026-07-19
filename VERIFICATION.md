# Verification record

Verification date: 18 July 2026.

## Performed in the artifact environment

- Parsed every checked-in JSON, YAML, and XML asset.
- Ran `scripts/static-verify.py` successfully across 75 production Java sources and 15 test classes, covering package-to-path consistency, public declaration Javadocs, repository hygiene, required production assets, migration markers, security/build markers, and test inventory.
- Ran shell syntax validation for project scripts and `git diff --check`.
- Inspected the Flyway migrations, independent evidence model, Kafka timestamp/offset calculations, reactive transaction boundaries, replay idempotency, checkpoint resume logic, stale-worker recovery, Redis lock ownership, API filters, CI workflows, Docker topology, and Postman collection.
- Performed a Java compiler parser pass with the available OpenJDK 21 compiler. Dependency/type-resolution errors are expected without the Java 25 dependency graph; no Java syntax diagnostics were accepted as passing until reviewed.
- Verified the Gradle 9.6.1 distribution and wrapper SHA-256 pins against the published release values.
- Verified archive integrity after packaging and generated a SHA-256 checksum for the final ZIP.

## Runtime verification limitation

This artifact environment provides OpenJDK 21 only. It does not provide Java 25, Gradle, Docker/Podman, a populated dependency cache, or outbound DNS from shell processes. Therefore it cannot truthfully execute the full Java 25 dependency compilation, unit suite, Testcontainers PostgreSQL test, or Compose runtime here.

The repository performs these gates on a normal Java 25/Docker runner:

```bash
./gradlew --no-daemon clean check integrationTest bootJar javadoc
```

GitHub Actions validates Compose, bootstraps the checksum-verified Gradle wrapper, runs unit and Docker-backed integration tests, enforces Checkstyle and JaCoCo, builds the executable/source/Javadoc JARs, submits the dependency graph, and uploads reports.

## High-value integration scenarios included

- Flyway applies both schema versions to PostgreSQL 18.4.
- Projection of the same Kafka position is idempotent.
- A distinct Kafka position with the same event ID increments source observations but not unique events/business rows.
- Compact counters and database amount follow real persisted rows.
- Deliberate aggregate count/amount corruption is detected and exact repair restores it.
- Replay creates bounded per-partition checkpoints, processes the snapshot, fences progress by execution attempt, updates progress/heartbeat, requires explicit retry after failure, and completes at the persisted end offset.
- Reconciliation distinguishes source loss, duplicate IDs, projection loss, aggregate count drift, and aggregate amount drift.
- Durable alert recovery retries later entries even when one channel attempt fails, and absorbs top-level recovery scan outages without dropped reactive errors.
