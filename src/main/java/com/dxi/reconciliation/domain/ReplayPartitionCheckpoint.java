package com.dxi.reconciliation.domain;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Durable replay progress for one bounded Kafka partition range. */
public record ReplayPartitionCheckpoint(
        UUID jobId,
        String sourceTopic,
        int sourcePartition,
        long startOffset,
        long endOffset,
        long nextOffset,
        long discoveredEvents,
        long replayedEvents,
        ReplayCheckpointStatus status,
        Instant updatedAt) {

    /** Validates checkpoint range, counters, status, and durable identity. */
    public ReplayPartitionCheckpoint {
        Objects.requireNonNull(jobId, "jobId must not be null");
        Objects.requireNonNull(sourceTopic, "sourceTopic must not be null");
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (sourceTopic.isBlank()) {
            throw new IllegalArgumentException("sourceTopic must not be blank");
        }
        if (sourcePartition < 0 || startOffset < 0 || endOffset < startOffset
                || nextOffset < startOffset || nextOffset > endOffset) {
            throw new IllegalArgumentException("Invalid replay checkpoint offset range");
        }
        if (discoveredEvents < 0 || replayedEvents < 0
                || replayedEvents > discoveredEvents) {
            throw new IllegalArgumentException("Invalid replay checkpoint counters");
        }
        if (status == ReplayCheckpointStatus.COMPLETED && nextOffset != endOffset) {
            throw new IllegalArgumentException(
                    "Completed replay checkpoint must end at the snapshot offset");
        }
    }

    /** Returns a copy with changed progress and lifecycle values. */
    public ReplayPartitionCheckpoint progress(
            long newNextOffset,
            long newDiscoveredEvents,
            long newReplayedEvents,
            ReplayCheckpointStatus newStatus,
            Instant now) {
        return new ReplayPartitionCheckpoint(
                jobId,
                sourceTopic,
                sourcePartition,
                startOffset,
                endOffset,
                newNextOffset,
                newDiscoveredEvents,
                newReplayedEvents,
                newStatus,
                now);
    }
}
