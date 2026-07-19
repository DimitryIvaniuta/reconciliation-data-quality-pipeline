# Changelog

## 2.0.0 — 18 July 2026

### Added

- Independent immutable Kafka source observations keyed by topic/partition/offset.
- Unique-event, database-row, aggregate-count, database-amount, and aggregate-amount reconciliation.
- Exact per-partition Kafka offset evidence in every report.
- Durable replay partition checkpoints with immutable bounds, next offset, counters, status, and heartbeat.
- Manual failed-job retry, stale-worker recovery, maximum attempts, and indexed recovery lookup.
- Append-only sensitive data mutation audit.
- Dependency submission workflow, stronger Compose validation, read-only application container, and explicit readiness health check.
- Local-only Spring profile for the development API key.
- Transactional per-channel alert outbox with durable attempts, bounded stale-delivery recovery, and downstream idempotency identifiers.
- Expanded tests, Postman requests, architecture documentation, and operator runbook.

### Fixed

- Duplicate event IDs are no longer silently collapsed into the consumed count.
- Idempotent source-position redelivery no longer binds a nonexistent SQL parameter.
- Aggregate monetary drift is detectable even when row counts match.
- Replay can resume rather than restarting all completed partition work after a stale worker/crash.
- Base configuration no longer contains a default API secret.
- Failed replay jobs cannot be restarted accidentally by stale Kafka commands; an explicit retry is required.
- Replay workers refresh their heartbeat during empty bounded polls.
- Replay progress writes are fenced by execution attempt to reject recovered stale workers.
- The Gradle launcher now checksum-verifies its wrapper bootstrap and distribution.
