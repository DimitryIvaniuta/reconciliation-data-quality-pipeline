package com.dxi.reconciliation.service;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.DailyMetrics;
import com.dxi.reconciliation.domain.KafkaDaySnapshot;
import com.dxi.reconciliation.domain.MismatchType;
import com.dxi.reconciliation.domain.ReconciliationIssue;
import com.dxi.reconciliation.domain.ReconciliationReport;
import com.dxi.reconciliation.domain.ReconciliationStatus;
import com.dxi.reconciliation.domain.TriggerType;
import com.dxi.reconciliation.port.AlertPublisher;
import com.dxi.reconciliation.port.DailyMetricsStore;
import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.port.KafkaEventCountProvider;
import com.dxi.reconciliation.port.ReportStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

/** Coordinates low-load daily reconciliation and immutable mismatch reporting. */
@Slf4j
public class ReconciliationService {

    private final KafkaEventCountProvider kafkaCounts;
    private final DailyMetricsStore metricsStore;
    private final ReportStore reportStore;
    private final AlertPublisher alertPublisher;
    private final DistributedLockService lockService;
    private final AppProperties properties;
    private final Clock clock;
    private final Counter mismatchCounter;
    private final Timer runTimer;

    /** Creates the reconciliation coordinator. */
    public ReconciliationService(
            KafkaEventCountProvider kafkaCounts,
            DailyMetricsStore metricsStore,
            ReportStore reportStore,
            AlertPublisher alertPublisher,
            DistributedLockService lockService,
            AppProperties properties,
            Clock clock,
            MeterRegistry meterRegistry) {
        this.kafkaCounts = kafkaCounts;
        this.metricsStore = metricsStore;
        this.reportStore = reportStore;
        this.alertPublisher = alertPublisher;
        this.lockService = lockService;
        this.properties = properties;
        this.clock = clock;
        this.mismatchCounter = meterRegistry.counter("reconciliation.mismatches");
        this.runTimer = meterRegistry.timer("reconciliation.run.duration");
    }

    /** Runs one reconciliation under a per-day distributed lock. */
    public Mono<ReconciliationReport> reconcile(
            LocalDate businessDate,
            TriggerType triggerType,
            String correlationId) {
        Mono<ReconciliationReport> action = Mono.zip(
                        kafkaCounts.snapshot(businessDate),
                        metricsStore.find(businessDate)
                                .defaultIfEmpty(DailyMetrics.empty(businessDate)))
                .map(values -> createReport(
                        businessDate,
                        triggerType,
                        correlationId,
                        values.getT1(),
                        values.getT2()))
                .flatMap(reportStore::save)
                .flatMap(this::publishAlertIfNeeded)
                .timeout(properties.reconciliation().queryTimeout());

        Mono<ReconciliationReport> timedAction = Mono.defer(() -> {
            long startedAt = System.nanoTime();
            return action.doFinally(signal -> runTimer.record(
                    System.nanoTime() - startedAt, TimeUnit.NANOSECONDS));
        });
        return lockService.withLock(
                "reconciliation:" + businessDate,
                properties.reconciliation().lockTtl(),
                timedAction);
    }

    private ReconciliationReport createReport(
            LocalDate date,
            TriggerType trigger,
            String correlationId,
            KafkaDaySnapshot snapshot,
            DailyMetrics metrics) {
        List<ReconciliationIssue> issues = new ArrayList<>();
        addCountIssue(
                issues,
                MismatchType.KAFKA_VS_SOURCE_OBSERVATIONS,
                snapshot.totalCount(),
                metrics.consumedEventCount(),
                "Inspect consumer lag, rejected events, and the dead-letter topic; then replay the date.");
        addCountIssue(
                issues,
                MismatchType.SOURCE_OBSERVATIONS_VS_UNIQUE_EVENTS,
                metrics.consumedEventCount(),
                metrics.uniqueEventCount(),
                "Inspect duplicated event IDs and producer retry behavior using the stored source offsets.");
        addCountIssue(
                issues,
                MismatchType.UNIQUE_EVENTS_VS_DATABASE,
                metrics.uniqueEventCount(),
                metrics.databaseRecordCount(),
                "Replay the date to rebuild missing projections and inspect mutation audit entries.");
        addCountIssue(
                issues,
                MismatchType.DATABASE_VS_AGGREGATE_COUNT,
                metrics.databaseRecordCount(),
                metrics.aggregateRecordCount(),
                "Run aggregate repair and inspect mutation audit entries for the affected date.");
        addAmountIssue(
                issues,
                metrics.databaseAmount(),
                metrics.aggregateAmount(),
                properties.reconciliation().amountTolerance());

        ReconciliationStatus status = issues.isEmpty()
                ? ReconciliationStatus.MATCHED
                : ReconciliationStatus.MISMATCH;
        return new ReconciliationReport(
                UUID.randomUUID(),
                date,
                trigger,
                status,
                snapshot.totalCount(),
                metrics.consumedEventCount(),
                metrics.uniqueEventCount(),
                metrics.databaseRecordCount(),
                metrics.aggregateRecordCount(),
                metrics.databaseAmount(),
                metrics.aggregateAmount(),
                snapshot.partitions(),
                issues,
                correlationId,
                clock.instant());
    }

    private void addCountIssue(
            List<ReconciliationIssue> issues,
            MismatchType type,
            long expected,
            long actual,
            String action) {
        if (expected != actual) {
            issues.add(new ReconciliationIssue(
                    type,
                    Long.toString(expected),
                    Long.toString(actual),
                    Long.toString(actual - expected),
                    action));
        }
    }

    private void addAmountIssue(
            List<ReconciliationIssue> issues,
            BigDecimal expected,
            BigDecimal actual,
            BigDecimal tolerance) {
        BigDecimal delta = actual.subtract(expected);
        if (delta.abs().compareTo(tolerance) > 0) {
            issues.add(new ReconciliationIssue(
                    MismatchType.DATABASE_VS_AGGREGATE_AMOUNT,
                    expected.toPlainString(),
                    actual.toPlainString(),
                    delta.toPlainString(),
                    "Run aggregate repair and investigate unauthorized aggregate changes."));
        }
    }

    private Mono<ReconciliationReport> publishAlertIfNeeded(ReconciliationReport report) {
        if (report.status() == ReconciliationStatus.MATCHED) {
            return Mono.just(report);
        }
        mismatchCounter.increment();
        return alertPublisher.publish(report)
                .onErrorResume(exception -> {
                    log.error(
                            "Report {} persisted but alert publication failed",
                            report.reportId(),
                            exception);
                    return Mono.empty();
                })
                .thenReturn(report);
    }
}
