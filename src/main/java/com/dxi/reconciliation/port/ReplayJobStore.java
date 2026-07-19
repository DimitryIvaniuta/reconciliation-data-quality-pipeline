package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.ReplayJob;
import java.time.Instant;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Stores durable replay jobs, command-dispatch state, and execution heartbeats. */
public interface ReplayJobStore {

    /** Creates a new replay job. */
    Mono<ReplayJob> create(ReplayJob job);

    /** Finds a job by identifier. */
    Mono<ReplayJob> findById(UUID jobId);

    /** Finds the existing job for an idempotency key. */
    Mono<ReplayJob> findByIdempotencyKey(String idempotencyKey);

    /** Finds requested jobs whose command needs initial dispatch or recovery. */
    Flux<ReplayJob> findDispatchable(Instant staleBefore, int limit);

    /** Finds running jobs whose worker heartbeat became stale. */
    Flux<ReplayJob> findStaleRunning(Instant staleBefore, int maximumAttempts, int limit);

    /** Records successful command publication. */
    Mono<Void> markCommandPublished(UUID jobId, Instant publishedAt);

    /** Updates counters and heartbeat without changing replay lifecycle state. */
    Mono<Void> updateProgress(
            UUID jobId,
            int executionAttempt,
            long discoveredEvents,
            long replayedEvents,
            Instant heartbeatAt);

    /** Persists a lifecycle transition and execution counters. */
    Mono<ReplayJob> update(ReplayJob job);
}
