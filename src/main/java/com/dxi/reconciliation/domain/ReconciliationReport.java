package com.dxi.reconciliation.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/** Immutable audit report for one reconciliation execution. */
public record ReconciliationReport(
        UUID reportId,
        LocalDate businessDate,
        TriggerType triggerType,
        ReconciliationStatus status,
        long kafkaEventCount,
        long consumedEventCount,
        long uniqueEventCount,
        long databaseRecordCount,
        long aggregateRecordCount,
        BigDecimal databaseAmount,
        BigDecimal aggregateAmount,
        List<KafkaPartitionRange> sourceOffsets,
        List<ReconciliationIssue> issues,
        String correlationId,
        Instant createdAt) {

    /** Copies collection values to protect immutable audit evidence. */
    public ReconciliationReport {
        sourceOffsets = List.copyOf(sourceOffsets);
        issues = List.copyOf(issues);
    }
}
