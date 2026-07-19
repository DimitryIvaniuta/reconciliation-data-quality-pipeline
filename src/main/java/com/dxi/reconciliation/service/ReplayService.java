package com.dxi.reconciliation.service;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.ReplayCommand;
import com.dxi.reconciliation.domain.ReplayExecutionResult;
import com.dxi.reconciliation.domain.ReplayJob;
import com.dxi.reconciliation.domain.ReplayPartitionCheckpoint;
import com.dxi.reconciliation.domain.ReplayStatus;
import com.dxi.reconciliation.domain.TriggerType;
import com.dxi.reconciliation.port.DailyAggregateRepairStore;
import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.port.ReplayCheckpointStore;
import com.dxi.reconciliation.port.ReplayCommandPublisher;
import com.dxi.reconciliation.port.ReplayExecutor;
import com.dxi.reconciliation.port.ReplayJobStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Coordinates durable idempotent replay requests and resumable validation. */
@Slf4j
public class ReplayService {

    private final ReplayJobStore jobStore;
    private final ReplayCheckpointStore checkpointStore;
    private final ReplayCommandPublisher commandPublisher;
    private final ReplayExecutor replayExecutor;
    private final DailyAggregateRepairStore repairStore;
    private final ReconciliationService reconciliationService;
    private final DistributedLockService lockService;
    private final AppProperties properties;
    private final Clock clock;
    private final Counter failureCounter;
    private final Counter recoveryCounter;

    /** Creates the replay coordinator. */
    public ReplayService(
            ReplayJobStore jobStore,
            ReplayCheckpointStore checkpointStore,
            ReplayCommandPublisher commandPublisher,
            ReplayExecutor replayExecutor,
            DailyAggregateRepairStore repairStore,
            ReconciliationService reconciliationService,
            DistributedLockService lockService,
            AppProperties properties,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.jobStore = jobStore;
        this.checkpointStore = checkpointStore;
        this.commandPublisher = commandPublisher;
        this.replayExecutor = replayExecutor;
        this.repairStore = repairStore;
        this.reconciliationService = reconciliationService;
        this.lockService = lockService;
        this.properties = properties;
        this.clock = clock;
        this.failureCounter = meterRegistry.counter("replay.failures");
        this.recoveryCounter = meterRegistry.counter("replay.recoveries");
    }

    /** Creates or returns an idempotent replay request and dispatches its command. */
    public Mono<ReplayJob> request(
            String idempotencyKey,
            LocalDate fromDate,
            LocalDate toDate,
            boolean dryRun,
            String requestedBy,
            String correlationId) {
        validateRequest(idempotencyKey, fromDate, toDate, requestedBy);
        ReplayJob job = new ReplayJob(
                UUID.randomUUID(), idempotencyKey, fromDate, toDate, dryRun,
                ReplayStatus.REQUESTED, 0, 0, 0, requestedBy, correlationId,
                null, clock.instant(), null, null, null, null);

        return jobStore.create(job)
                .map(created -> new RequestResolution(created, true))
                .onErrorResume(DuplicateKeyException.class,
                        exception -> jobStore.findByIdempotencyKey(idempotencyKey)
                                .switchIfEmpty(Mono.error(new ConflictException(
                                        "Replay idempotency conflict could not be resolved")))
                                .flatMap(existing -> validateIdempotentRetry(
                                        existing, fromDate, toDate, dryRun, requestedBy))
                                .map(existing -> new RequestResolution(existing, false)))
                .flatMap(resolution -> resolution.created()
                        ? dispatchBestEffort(resolution.job())
                        : Mono.just(resolution.job()));
    }

    /** Reads one replay job or emits a not-found error. */
    public Mono<ReplayJob> get(UUID jobId) {
        return jobStore.findById(jobId)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException(
                        "Replay job not found: " + jobId)));
    }

    /** Returns durable per-partition progress after validating that the job exists. */
    public Flux<ReplayPartitionCheckpoint> checkpoints(UUID jobId) {
        return get(jobId).thenMany(checkpointStore.findByJobId(jobId));
    }

    /** Executes one replay job under a per-job distributed lock. */
    public Mono<ReplayJob> execute(UUID jobId) {
        Mono<ReplayJob> action = get(jobId).flatMap(job -> {
            if (job.status() == ReplayStatus.COMPLETED
                    || job.status() == ReplayStatus.RUNNING) {
                return Mono.just(job);
            }
            if (job.status() == ReplayStatus.FAILED) {
                return Mono.error(new ConflictException(
                        "Failed replay jobs require an explicit retry request: " + jobId));
            }
            if (job.attemptCount() >= properties.reconciliation().maximumReplayAttempts()) {
                return Mono.error(new ConflictException(
                        "Replay job exhausted the configured attempt limit: " + jobId));
            }
            ReplayJob running = transitionRunning(job);
            return jobStore.update(running)
                    .flatMap(replayExecutor::execute)
                    .flatMap(result -> finish(running, result))
                    .onErrorResume(exception -> fail(running, exception));
        });
        return lockService.withLock(
                "replay:" + jobId,
                properties.reconciliation().lockTtl(),
                action);
    }

    /** Moves a failed replay back to requested state and republishes its command. */
    public Mono<ReplayJob> retry(UUID jobId) {
        return lockService.withLock(
                "replay:" + jobId,
                properties.reconciliation().lockTtl(),
                get(jobId).flatMap(job -> {
                    if (job.status() != ReplayStatus.FAILED) {
                        return Mono.error(new ConflictException(
                                "Only failed replay jobs can be retried"));
                    }
                    return resetForRecovery(job, "Manually retried")
                            .flatMap(this::dispatchBestEffort);
                }));
    }

    /** Recovers a stale running job while preserving its immutable checkpoint ranges. */
    public Mono<ReplayJob> recoverStale(ReplayJob job) {
        return lockService.withLock(
                "replay:" + job.jobId(),
                properties.reconciliation().lockTtl(),
                get(job.jobId()).flatMap(current -> {
                    Instant staleBefore = clock.instant().minus(
                            properties.reconciliation().replayStaleTimeout());
                    Instant lastHeartbeat = current.heartbeatAt() == null
                            ? current.startedAt()
                            : current.heartbeatAt();
                    if (current.status() != ReplayStatus.RUNNING
                            || lastHeartbeat == null
                            || !lastHeartbeat.isBefore(staleBefore)) {
                        return Mono.just(current);
                    }
                    recoveryCounter.increment();
                    return resetForRecovery(current, "Recovered after stale worker heartbeat")
                            .flatMap(this::dispatchBestEffort);
                }));
    }

    /** Republishes a command and records successful publication. */
    public Mono<Void> dispatch(ReplayJob job) {
        Instant publishedAt = clock.instant();
        return commandPublisher.publish(new ReplayCommand(job.jobId()))
                .then(jobStore.markCommandPublished(job.jobId(), publishedAt));
    }

    private Mono<ReplayJob> resetForRecovery(ReplayJob job, String reason) {
        if (job.attemptCount() >= properties.reconciliation().maximumReplayAttempts()) {
            return Mono.error(new ConflictException(
                    "Replay job exhausted the configured attempt limit: " + job.jobId()));
        }
        ReplayJob requested = new ReplayJob(
                job.jobId(), job.idempotencyKey(), job.fromDate(), job.toDate(), job.dryRun(),
                ReplayStatus.REQUESTED, job.discoveredEvents(), job.replayedEvents(),
                job.attemptCount(), job.requestedBy(), job.correlationId(), reason,
                job.requestedAt(), job.startedAt(), null, null, null);
        return jobStore.update(requested);
    }

    private Mono<ReplayJob> validateIdempotentRetry(
            ReplayJob existing,
            LocalDate fromDate,
            LocalDate toDate,
            boolean dryRun,
            String requestedBy) {
        boolean sameRequest = existing.fromDate().equals(fromDate)
                && existing.toDate().equals(toDate)
                && existing.dryRun() == dryRun
                && existing.requestedBy().equals(requestedBy);
        return sameRequest
                ? Mono.just(existing)
                : Mono.error(new ConflictException(
                        "Idempotency-Key is already associated with a different replay request"));
    }

    private Mono<ReplayJob> dispatchBestEffort(ReplayJob job) {
        return dispatch(job)
                .thenReturn(job)
                .onErrorResume(exception -> {
                    log.warn("Replay job {} stored; command recovery will retry publication",
                            job.jobId(), exception);
                    return Mono.just(job);
                });
    }

    private Mono<ReplayJob> finish(ReplayJob running, ReplayExecutionResult result) {
        Mono<Void> repairAndValidate = running.dryRun()
                ? Mono.empty()
                : dates(running.fromDate(), running.toDate())
                        .concatMap(date -> repairStore.repair(date)
                                .then(reconciliationService.reconcile(
                                        date,
                                        TriggerType.REPLAY_VALIDATION,
                                        running.correlationId())))
                        .then();
        return repairAndValidate.then(Mono.defer(() -> {
            Instant now = clock.instant();
            ReplayJob completed = new ReplayJob(
                    running.jobId(), running.idempotencyKey(), running.fromDate(), running.toDate(),
                    running.dryRun(), ReplayStatus.COMPLETED, result.discoveredEvents(),
                    result.replayedEvents(), running.attemptCount(), running.requestedBy(),
                    running.correlationId(), null, running.requestedAt(), running.startedAt(), now,
                    running.commandPublishedAt(), now);
            return jobStore.update(completed);
        }));
    }

    private Mono<ReplayJob> fail(ReplayJob running, Throwable exception) {
        failureCounter.increment();
        String message = exception.getMessage() == null
                ? exception.getClass().getSimpleName()
                : exception.getMessage();
        log.error("Replay job {} failed", running.jobId(), exception);
        return jobStore.findById(running.jobId())
                .defaultIfEmpty(running)
                .flatMap(current -> {
                    Instant now = clock.instant();
                    ReplayJob failed = new ReplayJob(
                            current.jobId(), current.idempotencyKey(), current.fromDate(),
                            current.toDate(), current.dryRun(), ReplayStatus.FAILED,
                            current.discoveredEvents(), current.replayedEvents(),
                            current.attemptCount(), current.requestedBy(), current.correlationId(),
                            truncate(message, 2_000), current.requestedAt(), current.startedAt(), now,
                            current.commandPublishedAt(), now);
                    return jobStore.update(failed);
                });
    }

    private ReplayJob transitionRunning(ReplayJob job) {
        Instant now = clock.instant();
        return new ReplayJob(
                job.jobId(), job.idempotencyKey(), job.fromDate(), job.toDate(), job.dryRun(),
                ReplayStatus.RUNNING, job.discoveredEvents(), job.replayedEvents(),
                job.attemptCount() + 1, job.requestedBy(), job.correlationId(), null,
                job.requestedAt(), now, null, job.commandPublishedAt(), now);
    }

    private void validateRequest(
            String idempotencyKey,
            LocalDate fromDate,
            LocalDate toDate,
            String requestedBy) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || idempotencyKey.length() > 200) {
            throw new IllegalArgumentException("Idempotency-Key must contain 1 to 200 characters");
        }
        if (requestedBy == null || requestedBy.isBlank() || requestedBy.length() > 200) {
            throw new IllegalArgumentException("X-Requested-By must contain 1 to 200 characters");
        }
        if (fromDate == null || toDate == null || fromDate.isAfter(toDate)) {
            throw new IllegalArgumentException("fromDate must not be after toDate");
        }
        if (toDate.isAfter(LocalDate.now(clock.withZone(ZoneOffset.UTC)))) {
            throw new IllegalArgumentException("Replay range must not extend into the future");
        }
        long days = ChronoUnit.DAYS.between(fromDate, toDate) + 1;
        if (days > properties.reconciliation().maximumReplayDays()) {
            throw new IllegalArgumentException(
                    "Replay range exceeds the configured maximum of "
                            + properties.reconciliation().maximumReplayDays() + " days");
        }
    }

    private Flux<LocalDate> dates(LocalDate fromDate, LocalDate toDate) {
        long count = ChronoUnit.DAYS.between(fromDate, toDate) + 1;
        return Flux.range(0, Math.toIntExact(count)).map(fromDate::plusDays);
    }

    private record RequestResolution(ReplayJob job, boolean created) { }

    private String truncate(String value, int maximumLength) {
        return value.length() <= maximumLength ? value : value.substring(0, maximumLength);
    }
}
