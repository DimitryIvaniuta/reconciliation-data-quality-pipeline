package com.dxi.reconciliation.scheduler;

import static com.dxi.reconciliation.support.TestFixtures.NOW;
import static com.dxi.reconciliation.support.TestFixtures.properties;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dxi.reconciliation.domain.AlertAudit;
import com.dxi.reconciliation.domain.AlertStatus;
import com.dxi.reconciliation.domain.ReconciliationReport;
import com.dxi.reconciliation.domain.ReconciliationStatus;
import com.dxi.reconciliation.domain.TriggerType;
import com.dxi.reconciliation.port.AlertAuditStore;
import com.dxi.reconciliation.port.AlertPublisher;
import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.port.ReportStore;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class AlertRecoverySchedulerTest {

    @Mock private AlertAuditStore alertStore;
    @Mock private ReportStore reportStore;
    @Mock private AlertPublisher alertPublisher;
    @Mock private DistributedLockService lockService;
    private AlertRecoveryScheduler scheduler;

    @BeforeEach
    void setUp() {
        lenient().when(lockService.withLock(anyString(), any(), any()))
                .thenAnswer(invocation -> invocation.getArgument(2));
        scheduler = new AlertRecoveryScheduler(
                alertStore,
                reportStore,
                alertPublisher,
                lockService,
                properties(),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void retriesEveryRecoverableAlertAndIsolatesPerEntryFailures() {
        ReconciliationReport report = report();
        AlertAudit first = audit(UUID.fromString("33333333-3333-3333-3333-333333333333"));
        AlertAudit second = audit(UUID.fromString("44444444-4444-4444-4444-444444444444"));
        when(alertStore.findRecoverable(any(), eq(100))).thenReturn(Flux.just(first, second));
        when(reportStore.findById(report.reportId())).thenReturn(Mono.just(report));
        when(alertPublisher.retry(first, report))
                .thenReturn(Mono.error(new IllegalStateException("temporary failure")));
        when(alertPublisher.retry(second, report)).thenReturn(Mono.empty());

        scheduler.recover();

        verify(alertPublisher).retry(first, report);
        verify(alertPublisher).retry(second, report);
    }

    @Test
    void absorbsARecoveryScanFailure() {
        when(alertStore.findRecoverable(any(), eq(100)))
                .thenReturn(Flux.error(new IllegalStateException("database unavailable")));

        scheduler.recover();

        verify(alertStore).findRecoverable(any(), eq(100));
    }

    private AlertAudit audit(UUID alertId) {
        return new AlertAudit(
                alertId,
                report().reportId(),
                "KAFKA",
                AlertStatus.FAILED,
                1,
                "offline",
                NOW.minusSeconds(600),
                NOW.minusSeconds(600));
    }

    private ReconciliationReport report() {
        return new ReconciliationReport(
                UUID.fromString("55555555-5555-5555-5555-555555555555"),
                LocalDate.parse("2026-07-17"),
                TriggerType.SCHEDULED,
                ReconciliationStatus.MISMATCH,
                10,
                9,
                9,
                9,
                9,
                new BigDecimal("10.0000"),
                new BigDecimal("10.0000"),
                List.of(),
                List.of(),
                "correlation-1",
                NOW);
    }
}
