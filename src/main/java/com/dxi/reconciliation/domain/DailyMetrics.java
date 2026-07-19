package com.dxi.reconciliation.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

/** Compact independently maintained daily database counters and monetary totals. */
public record DailyMetrics(
        LocalDate businessDate,
        long consumedEventCount,
        long uniqueEventCount,
        long databaseRecordCount,
        long aggregateRecordCount,
        BigDecimal databaseAmount,
        BigDecimal aggregateAmount,
        Instant updatedAt) {

    /** Creates a zero-value metrics snapshot for a day with no persisted state. */
    public static DailyMetrics empty(LocalDate date) {
        return new DailyMetrics(
                date,
                0,
                0,
                0,
                0,
                BigDecimal.ZERO,
                BigDecimal.ZERO,
                Instant.EPOCH);
    }
}
