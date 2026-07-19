# Operations runbook

## Mismatch triage

Open the newest report for the UTC ingestion date and inspect its `sourceOffsets`, `issues`, correlation ID, and alert audit.

| Mismatch | Interpretation | Primary action |
|---|---|---|
| `KAFKA_VS_SOURCE_OBSERVATIONS` | Kafka contains a different number of source positions than the projector persisted | Check consumer lag, listener errors, and DLT; run a narrow dry-run and then real replay |
| `SOURCE_OBSERVATIONS_VS_UNIQUE_EVENTS` | Multiple Kafka positions contain the same event ID | Inspect producer retry/idempotency behavior using partition offsets and event IDs |
| `UNIQUE_EVENTS_VS_DATABASE` | Unique ledger events and materialized rows differ | Inspect `data_mutation_audit`; replay the date to rebuild missing projections |
| `DATABASE_VS_AGGREGATE_COUNT` | Business-row and aggregate counts differ | Run real replay/repair and inspect aggregate repair/delete audit |
| `DATABASE_VS_AGGREGATE_AMOUNT` | Count may match, but independently maintained amounts differ above tolerance | Run aggregate repair; inspect unauthorized/manual aggregate changes |

After a real replay, review the automatically created `REPLAY_VALIDATION` report. Do not delete earlier mismatch reports; they are operational evidence.

## Replay safety

1. Start with one date and `dryRun=true`.
2. Supply a stable `Idempotency-Key`. Repeating the identical request returns the same job; using the key for different parameters is rejected.
3. Confirm the discovered record count and inspect `/api/v1/replays/{jobId}/checkpoints`.
4. Submit a real replay only after the dry-run range is correct.
5. A request is limited to the configured retention-safe window (31 days by default).
6. The worker reads immutable offset ranges for `[fromDate, toDate + 1 day)` and checkpoints every configured number of records.
7. A stale worker is reset to `REQUESTED` and resumes from stored `nextOffset`; it does not create a new range.
8. A failed job can be retried explicitly until `maximumReplayAttempts` is reached.
9. Exact repair scans only requested indexed dates and is not part of normal daily monitoring.

## Stale worker handling

A job is stale when `heartbeatAt` (or `startedAt` when no heartbeat exists) is older than `replayStaleTimeout`. The recovery scheduler:

- finds stale `RUNNING` jobs in a bounded batch using the partial expression index;
- acquires the per-job Redis lock;
- rechecks the persisted state and heartbeat;
- returns the job to `REQUESTED` without deleting checkpoints;
- republishes the wake-up command;
- increments the recovery metric.

If Redis is unavailable, no second worker is started. The next scheduler cycle retries recovery.

## Suggested SLOs and alerts

- Scheduled report available within five minutes of the configured settlement run.
- Alert delivery attempt within 60 seconds of mismatch report persistence.
- Zero unexplained mismatches older than one ingestion day.
- No `RUNNING` replay heartbeat older than `replayStaleTimeout`.
- Replay failure and recovery counters alert on any sustained increase.
- Source-topic retention always exceeds `maximumReplayDays` plus operational investigation time.

## Production deployment checklist

- Replace API-key authentication with an identity-aware gateway or OAuth2 resource server.
- Store secrets in a managed secret store; never enable the `local` Spring profile.
- Enable TLS/SASL and ACLs for Kafka, TLS/ACLs for Redis, and TLS/least-privilege roles for PostgreSQL.
- Use Kafka replication factor at least three and production-appropriate KRaft controller/broker topology.
- Restrict Actuator endpoints and scrape Prometheus through a private network.
- Set database `application_name` so mutation-audit entries identify the client.
- Back up reports, alerts, replay jobs/checkpoints, mutation audit, and source Kafka retention according to the evidence policy.


## Alert delivery recovery

1. Query `reconciliation_alerts` by `report_id` and inspect `status`, `attempt_count`, `error_message`, and `updated_at`.
2. Confirm the Kafka alert topic and optional webhook are reachable.
3. Do not manually insert duplicate channel rows; `(report_id, channel)` is unique.
4. Leave stale `PENDING`/`FAILED` rows for the bounded recovery scheduler after correcting the transport issue.
5. Alert consumers must deduplicate by Kafka alert ID or webhook `Idempotency-Key`, because delivery is intentionally at least once.
