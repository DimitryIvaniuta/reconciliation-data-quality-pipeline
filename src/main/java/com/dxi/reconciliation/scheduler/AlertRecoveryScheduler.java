package com.dxi.reconciliation.scheduler;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.port.AlertAuditStore;
import com.dxi.reconciliation.port.AlertPublisher;
import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.port.ReportStore;
import java.time.Clock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Mono;

/** Retries durable mismatch-alert outbox entries after transient delivery failures. */
@Slf4j
public class AlertRecoveryScheduler {

    private static final int BATCH_SIZE = 100;

    private final AlertAuditStore alertStore;
    private final ReportStore reportStore;
    private final AlertPublisher alertPublisher;
    private final DistributedLockService lockService;
    private final AppProperties properties;
    private final Clock clock;

    /** Creates the alert outbox recovery scheduler. */
    public AlertRecoveryScheduler(
            AlertAuditStore alertStore,
            ReportStore reportStore,
            AlertPublisher alertPublisher,
            DistributedLockService lockService,
            AppProperties properties,
            Clock clock) {
        this.alertStore = alertStore;
        this.reportStore = reportStore;
        this.alertPublisher = alertPublisher;
        this.lockService = lockService;
        this.properties = properties;
        this.clock = clock;
    }

    /** Retries stale pending and failed outbox entries in a bounded batch. */
    @Scheduled(fixedDelayString = "${app.reconciliation.alert-recovery-delay:PT30S}")
    public void recover() {
        alertStore.findRecoverable(
                        clock.instant().minus(properties.reconciliation()
                                .alertRedispatchInterval()),
                        BATCH_SIZE)
                .concatMap(audit -> lockService.withLock(
                                "alert:" + audit.alertId(),
                                properties.reconciliation().lockTtl(),
                                reportStore.findById(audit.reportId())
                                        .switchIfEmpty(Mono.error(new IllegalStateException(
                                                "Alert references a missing report: "
                                                        + audit.reportId())))
                                        .flatMap(report -> alertPublisher.retry(audit, report)))
                        .onErrorResume(exception -> {
                            log.warn("Unable to recover alert {} for report {}",
                                    audit.alertId(), audit.reportId(), exception);
                            return Mono.empty();
                        }))
                .then()
                .doOnError(exception -> log.error("Alert recovery scan failed", exception))
                .onErrorResume(exception -> Mono.empty())
                .subscribe();
    }
}
