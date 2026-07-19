package com.dxi.reconciliation.adapter.db;

import com.dxi.reconciliation.domain.ReplayJob;
import com.dxi.reconciliation.domain.ReplayStatus;
import com.dxi.reconciliation.port.ReplayJobStore;
import com.dxi.reconciliation.service.ConflictException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** PostgreSQL durable replay-job repository. */
public class R2dbcReplayJobStore implements ReplayJobStore {

    private static final String COLUMNS = """
            job_id, idempotency_key, from_date, to_date, dry_run, status,
            discovered_events, replayed_events, attempt_count, requested_by, correlation_id,
            error_message, requested_at, started_at, completed_at, command_published_at,
            heartbeat_at
            """;

    private final DatabaseClient client;

    /** Creates the replay job adapter. */
    public R2dbcReplayJobStore(DatabaseClient client) {
        this.client = client;
    }

    /** Creates a durable replay request. */
    @Override
    public Mono<ReplayJob> create(ReplayJob job) {
        DatabaseClient.GenericExecuteSpec spec = client.sql("""
                        INSERT INTO replay_jobs (
                            job_id, idempotency_key, from_date, to_date, dry_run, status,
                            discovered_events, replayed_events, attempt_count, requested_by,
                            correlation_id, error_message, requested_at, started_at, completed_at,
                            command_published_at, heartbeat_at)
                        VALUES (:jobId, :idempotencyKey, :fromDate, :toDate, :dryRun, :status,
                                :discoveredEvents, :replayedEvents, :attemptCount, :requestedBy,
                                :correlationId, :errorMessage, :requestedAt, :startedAt,
                                :completedAt, :commandPublishedAt, :heartbeatAt)
                        """)
                .bind("jobId", job.jobId())
                .bind("idempotencyKey", job.idempotencyKey())
                .bind("fromDate", job.fromDate())
                .bind("toDate", job.toDate())
                .bind("dryRun", job.dryRun())
                .bind("status", job.status().name())
                .bind("discoveredEvents", job.discoveredEvents())
                .bind("replayedEvents", job.replayedEvents())
                .bind("attemptCount", job.attemptCount())
                .bind("requestedBy", job.requestedBy())
                .bind("correlationId", job.correlationId())
                .bind("requestedAt", job.requestedAt());
        spec = bindNullable(spec, "errorMessage", job.errorMessage(), String.class);
        spec = bindNullable(spec, "startedAt", job.startedAt(), Instant.class);
        spec = bindNullable(spec, "completedAt", job.completedAt(), Instant.class);
        spec = bindNullable(spec, "commandPublishedAt", job.commandPublishedAt(), Instant.class);
        spec = bindNullable(spec, "heartbeatAt", job.heartbeatAt(), Instant.class);
        return spec.fetch().rowsUpdated().thenReturn(job);
    }

    /** Finds a job by identifier. */
    @Override
    public Mono<ReplayJob> findById(UUID jobId) {
        return client.sql("SELECT " + COLUMNS + " FROM replay_jobs WHERE job_id = :jobId")
                .bind("jobId", jobId)
                .map(this::mapJob)
                .one();
    }

    /** Finds a job by idempotency key. */
    @Override
    public Mono<ReplayJob> findByIdempotencyKey(String idempotencyKey) {
        return client.sql("SELECT " + COLUMNS
                        + " FROM replay_jobs WHERE idempotency_key = :idempotencyKey")
                .bind("idempotencyKey", idempotencyKey)
                .map(this::mapJob)
                .one();
    }

    /** Finds requested jobs needing command dispatch or recovery. */
    @Override
    public Flux<ReplayJob> findDispatchable(Instant staleBefore, int limit) {
        return client.sql("SELECT " + COLUMNS + " FROM replay_jobs "
                        + "WHERE status = 'REQUESTED' AND "
                        + "(command_published_at IS NULL OR command_published_at < :staleBefore) "
                        + "ORDER BY requested_at ASC LIMIT :limit")
                .bind("staleBefore", staleBefore)
                .bind("limit", limit)
                .map(this::mapJob)
                .all();
    }

    /** Finds stale running jobs that still have retry capacity. */
    @Override
    public Flux<ReplayJob> findStaleRunning(
            Instant staleBefore,
            int maximumAttempts,
            int limit) {
        return client.sql("SELECT " + COLUMNS + " FROM replay_jobs "
                        + "WHERE status = 'RUNNING' "
                        + "AND attempt_count < :maximumAttempts "
                        + "AND COALESCE(heartbeat_at, started_at, requested_at) < :staleBefore "
                        + "ORDER BY COALESCE(heartbeat_at, started_at, requested_at) ASC "
                        + "LIMIT :limit")
                .bind("maximumAttempts", maximumAttempts)
                .bind("staleBefore", staleBefore)
                .bind("limit", limit)
                .map(this::mapJob)
                .all();
    }

    /** Records successful command publication. */
    @Override
    public Mono<Void> markCommandPublished(UUID jobId, Instant publishedAt) {
        return client.sql("""
                        UPDATE replay_jobs
                        SET command_published_at = :publishedAt
                        WHERE job_id = :jobId AND status = 'REQUESTED'
                        """)
                .bind("publishedAt", publishedAt)
                .bind("jobId", jobId)
                .fetch()
                .rowsUpdated()
                .then();
    }

    /** Updates counters only when the execution-attempt fence still matches. */
    @Override
    public Mono<Void> updateProgress(
            UUID jobId,
            int executionAttempt,
            long discoveredEvents,
            long replayedEvents,
            Instant heartbeatAt) {
        return client.sql("""
                        UPDATE replay_jobs
                        SET discovered_events = :discoveredEvents,
                            replayed_events = :replayedEvents,
                            heartbeat_at = :heartbeatAt
                        WHERE job_id = :jobId
                          AND status = 'RUNNING'
                          AND attempt_count = :executionAttempt
                        """)
                .bind("discoveredEvents", discoveredEvents)
                .bind("replayedEvents", replayedEvents)
                .bind("heartbeatAt", heartbeatAt)
                .bind("jobId", jobId)
                .bind("executionAttempt", executionAttempt)
                .fetch()
                .rowsUpdated()
                .flatMap(updated -> updated == 1L
                        ? Mono.empty()
                        : Mono.error(new ConflictException(
                                "Replay execution fence rejected a stale worker: " + jobId)));
    }

    /** Updates mutable replay lifecycle fields. */
    @Override
    public Mono<ReplayJob> update(ReplayJob job) {
        DatabaseClient.GenericExecuteSpec spec = client.sql("""
                        UPDATE replay_jobs
                        SET status = :status,
                            discovered_events = :discoveredEvents,
                            replayed_events = :replayedEvents,
                            attempt_count = :attemptCount,
                            error_message = :errorMessage,
                            started_at = :startedAt,
                            completed_at = :completedAt,
                            command_published_at = :commandPublishedAt,
                            heartbeat_at = :heartbeatAt
                        WHERE job_id = :jobId
                        """)
                .bind("status", job.status().name())
                .bind("discoveredEvents", job.discoveredEvents())
                .bind("replayedEvents", job.replayedEvents())
                .bind("attemptCount", job.attemptCount())
                .bind("jobId", job.jobId());
        spec = bindNullable(spec, "errorMessage", job.errorMessage(), String.class);
        spec = bindNullable(spec, "startedAt", job.startedAt(), Instant.class);
        spec = bindNullable(spec, "completedAt", job.completedAt(), Instant.class);
        spec = bindNullable(spec, "commandPublishedAt", job.commandPublishedAt(), Instant.class);
        spec = bindNullable(spec, "heartbeatAt", job.heartbeatAt(), Instant.class);
        return spec.fetch().rowsUpdated().then(findById(job.jobId()));
    }

    private ReplayJob mapJob(Row row, RowMetadata metadata) {
        return new ReplayJob(
                row.get("job_id", UUID.class),
                row.get("idempotency_key", String.class),
                row.get("from_date", LocalDate.class),
                row.get("to_date", LocalDate.class),
                Boolean.TRUE.equals(row.get("dry_run", Boolean.class)),
                ReplayStatus.valueOf(row.get("status", String.class)),
                number(row.get("discovered_events")),
                number(row.get("replayed_events")),
                integer(row.get("attempt_count")),
                row.get("requested_by", String.class),
                row.get("correlation_id", String.class),
                row.get("error_message", String.class),
                row.get("requested_at", Instant.class),
                row.get("started_at", Instant.class),
                row.get("completed_at", Instant.class),
                row.get("command_published_at", Instant.class),
                row.get("heartbeat_at", Instant.class));
    }

    private long number(Object value) {
        return value == null ? 0 : ((Number) value).longValue();
    }

    private int integer(Object value) {
        return value == null ? 0 : ((Number) value).intValue();
    }

    private <T> DatabaseClient.GenericExecuteSpec bindNullable(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            T value,
            Class<T> type) {
        return value == null ? spec.bindNull(name, type) : spec.bind(name, value);
    }
}
