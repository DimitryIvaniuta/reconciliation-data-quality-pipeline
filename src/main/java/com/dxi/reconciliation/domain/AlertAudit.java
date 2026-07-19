package com.dxi.reconciliation.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Auditable alert-delivery record and retry state. */
public record AlertAudit(
        UUID alertId,
        UUID reportId,
        String channel,
        AlertStatus status,
        int attemptCount,
        String errorMessage,
        Instant createdAt,
        Instant updatedAt) {

    /** Validates durable alert identity, channel, attempt count, and timestamps. */
    public AlertAudit {
        Objects.requireNonNull(alertId, "alertId must not be null");
        Objects.requireNonNull(reportId, "reportId must not be null");
        Objects.requireNonNull(channel, "channel must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (channel.isBlank()) {
            throw new IllegalArgumentException("channel must not be blank");
        }
        if (attemptCount < 0) {
            throw new IllegalArgumentException("attemptCount must not be negative");
        }
        if (updatedAt.isBefore(createdAt)) {
            throw new IllegalArgumentException("updatedAt must not precede createdAt");
        }
    }
}
