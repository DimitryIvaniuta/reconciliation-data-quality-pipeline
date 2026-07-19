package com.dxi.reconciliation.support;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.BusinessEvent;
import com.dxi.reconciliation.domain.DailyMetrics;
import com.dxi.reconciliation.domain.ReplayJob;
import com.dxi.reconciliation.domain.ReplayStatus;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

/** Shared deterministic test object factory. */
public final class TestFixtures {

    /** Stable test instant. */
    public static final Instant NOW = Instant.parse("2026-07-18T10:00:00Z");

    private TestFixtures() {
    }

    /** Returns production-like validated application properties. */
    public static AppProperties properties() {
        return new AppProperties(
                new AppProperties.Security("test-api-key"),
                new AppProperties.Reconciliation(
                        "0 15 2 * * *", "UTC", Duration.ofHours(1), Duration.ofSeconds(30),
                        Duration.ofHours(2), Duration.ofMinutes(5), Duration.ofMinutes(5),
                        Duration.ofMinutes(10), new BigDecimal("0.0001"), 31, 5, 100),
                new AppProperties.Kafka(
                        "events", "replay", "alerts", "dlt", 3, 1,
                        Duration.ofDays(45), Duration.ofSeconds(10), Duration.ofMillis(100)),
                new AppProperties.Alert(null));
    }

    /** Returns a valid business event. */
    public static BusinessEvent event() {
        return new BusinessEvent(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                "ORDER-1001",
                Instant.parse("2026-07-17T12:00:00Z"),
                new BigDecimal("125.50"),
                Map.of("channel", "WEB"));
    }

    /** Returns a daily metrics snapshot. */
    public static DailyMetrics metrics(long consumed, long database, long aggregate) {
        return new DailyMetrics(
                LocalDate.parse("2026-07-17"), consumed, consumed, database, aggregate,
                new BigDecimal("125.50"), new BigDecimal("125.50"), NOW);
    }

    /** Returns a fully specified daily metrics snapshot. */
    public static DailyMetrics metrics(
            long consumed,
            long unique,
            long database,
            long aggregate,
            String databaseAmount,
            String aggregateAmount) {
        return new DailyMetrics(
                LocalDate.parse("2026-07-17"), consumed, unique, database, aggregate,
                new BigDecimal(databaseAmount), new BigDecimal(aggregateAmount), NOW);
    }

    /** Returns a requested replay job. */
    public static ReplayJob requestedJob(boolean dryRun) {
        return new ReplayJob(
                UUID.fromString("22222222-2222-2222-2222-222222222222"),
                "idempotency-1",
                LocalDate.parse("2026-07-17"),
                LocalDate.parse("2026-07-17"),
                dryRun,
                ReplayStatus.REQUESTED,
                0,
                0,
                0,
                "operator@example.test",
                "correlation-1",
                null,
                NOW,
                null,
                null,
                null,
                null);
    }
}
