package com.dxi.reconciliation.domain;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/** Durable and idempotently requested replay/backfill job. */
public record ReplayJob(
        UUID jobId,
        String idempotencyKey,
        LocalDate fromDate,
        LocalDate toDate,
        boolean dryRun,
        ReplayStatus status,
        long discoveredEvents,
        long replayedEvents,
        int attemptCount,
        String requestedBy,
        String correlationId,
        String errorMessage,
        Instant requestedAt,
        Instant startedAt,
        Instant completedAt,
        Instant commandPublishedAt,
        Instant heartbeatAt) { }
