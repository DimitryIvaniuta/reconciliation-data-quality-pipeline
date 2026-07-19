package com.dxi.reconciliation.config;

import com.dxi.reconciliation.port.AlertAuditStore;
import com.dxi.reconciliation.port.AlertPublisher;
import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.port.ReplayJobStore;
import com.dxi.reconciliation.port.ReportStore;
import com.dxi.reconciliation.scheduler.AlertRecoveryScheduler;
import com.dxi.reconciliation.scheduler.DailyReconciliationScheduler;
import com.dxi.reconciliation.scheduler.ReplayCommandRecoveryScheduler;
import com.dxi.reconciliation.service.ReconciliationService;
import com.dxi.reconciliation.service.ReplayService;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Wires scheduled orchestration components. */
@Configuration(proxyBeanMethods = false)
public class SchedulerConfiguration {

    /** Provides the daily reconciliation scheduler. */
    @Bean
    public DailyReconciliationScheduler dailyReconciliationScheduler(
            ReconciliationService service,
            AppProperties properties,
            Clock clock) {
        return new DailyReconciliationScheduler(service, properties, clock);
    }

    /** Provides replay command recovery. */
    @Bean
    public ReplayCommandRecoveryScheduler replayCommandRecoveryScheduler(
            ReplayJobStore jobStore,
            ReplayService replayService,
            AppProperties properties,
            Clock clock) {
        return new ReplayCommandRecoveryScheduler(jobStore, replayService, properties, clock);
    }
    /** Provides durable alert-outbox recovery. */
    @Bean
    public AlertRecoveryScheduler alertRecoveryScheduler(
            AlertAuditStore alertStore,
            ReportStore reportStore,
            AlertPublisher alertPublisher,
            DistributedLockService lockService,
            AppProperties properties,
            Clock clock) {
        return new AlertRecoveryScheduler(
                alertStore, reportStore, alertPublisher, lockService, properties, clock);
    }

}
