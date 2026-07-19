package com.dxi.reconciliation.scheduler;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.TriggerType;
import com.dxi.reconciliation.service.ReconciliationService;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

/** Starts the settled previous-day reconciliation on the configured schedule. */
@Slf4j
public class DailyReconciliationScheduler {

    private final ReconciliationService reconciliationService;
    private final AppProperties properties;
    private final Clock clock;

    /** Creates the daily scheduler. */
    public DailyReconciliationScheduler(
            ReconciliationService reconciliationService,
            AppProperties properties,
            Clock clock) {
        this.reconciliationService = reconciliationService;
        this.properties = properties;
        this.clock = clock;
    }

    /** Runs the previous settled ingestion day without overlapping the same date. */
    @Scheduled(
            cron = "${app.reconciliation.schedule-cron}",
            zone = "${app.reconciliation.zone}")
    public void run() {
        ZoneId zone = ZoneId.of(properties.reconciliation().zone());
        LocalDate date = ZonedDateTime.now(clock.withZone(zone))
                .minus(properties.reconciliation().settlementDelay())
                .toLocalDate()
                .minusDays(1);
        reconciliationService.reconcile(date, TriggerType.SCHEDULED, UUID.randomUUID().toString())
                .doOnError(exception -> log.error("Scheduled reconciliation failed for {}", date, exception))
                .subscribe(report -> log.info(
                        "Scheduled reconciliation {} completed with status {}",
                        report.reportId(), report.status()));
    }
}
