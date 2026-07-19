package com.dxi.reconciliation.service;

import static com.dxi.reconciliation.support.TestFixtures.NOW;
import static com.dxi.reconciliation.support.TestFixtures.metrics;
import static com.dxi.reconciliation.support.TestFixtures.properties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dxi.reconciliation.domain.KafkaDaySnapshot;
import com.dxi.reconciliation.domain.KafkaPartitionRange;
import com.dxi.reconciliation.domain.MismatchType;
import com.dxi.reconciliation.domain.ReconciliationStatus;
import com.dxi.reconciliation.domain.TriggerType;
import com.dxi.reconciliation.port.AlertPublisher;
import com.dxi.reconciliation.port.DailyMetricsStore;
import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.port.KafkaEventCountProvider;
import com.dxi.reconciliation.port.ReportStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ReconciliationServiceTest {

    @Mock private KafkaEventCountProvider kafkaCounts;
    @Mock private DailyMetricsStore metricsStore;
    @Mock private ReportStore reportStore;
    @Mock private AlertPublisher alertPublisher;
    @Mock private DistributedLockService lockService;
    private ReconciliationService service;

    @BeforeEach
    void setUp() {
        when(lockService.withLock(anyString(), any(), any()))
                .thenAnswer(invocation -> invocation.getArgument(2));
        when(reportStore.save(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        service = new ReconciliationService(
                kafkaCounts,
                metricsStore,
                reportStore,
                alertPublisher,
                lockService,
                properties(),
                Clock.fixed(NOW, ZoneOffset.UTC),
                new SimpleMeterRegistry());
    }

    @Test
    void createsMatchedReportWithoutAlert() {
        LocalDate date = LocalDate.parse("2026-07-17");
        when(kafkaCounts.snapshot(date)).thenReturn(Mono.just(snapshot(date, 10)));
        when(metricsStore.find(date)).thenReturn(Mono.just(metrics(10, 10, 10)));

        StepVerifier.create(service.reconcile(date, TriggerType.MANUAL, "corr"))
                .assertNext(report -> {
                    assertThat(report.status()).isEqualTo(ReconciliationStatus.MATCHED);
                    assertThat(report.issues()).isEmpty();
                    assertThat(report.createdAt()).isEqualTo(NOW);
                })
                .verifyComplete();

        verify(alertPublisher, never()).publish(any());
    }

    @Test
    void createsClearActionableMismatchReportAndAlerts() {
        LocalDate date = LocalDate.parse("2026-07-17");
        when(kafkaCounts.snapshot(date)).thenReturn(Mono.just(snapshot(date, 12)));
        when(metricsStore.find(date)).thenReturn(Mono.just(metrics(11, 10, 9)));
        when(alertPublisher.publish(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.reconcile(date, TriggerType.SCHEDULED, "corr"))
                .assertNext(report -> {
                    assertThat(report.status()).isEqualTo(ReconciliationStatus.MISMATCH);
                    assertThat(report.issues()).hasSize(3);
                    assertThat(report.issues()).allSatisfy(issue -> {
                        assertThat(issue.action()).isNotBlank();
                        assertThat(Long.parseLong(issue.delta())).isEqualTo(
                                Long.parseLong(issue.actual()) - Long.parseLong(issue.expected()));
                    });
                })
                .verifyComplete();

        verify(alertPublisher).publish(any());
    }

    @Test
    void retainsPersistedReportWhenAlertTransportFails() {
        LocalDate date = LocalDate.parse("2026-07-17");
        when(kafkaCounts.snapshot(date)).thenReturn(Mono.just(snapshot(date, 2)));
        when(metricsStore.find(date)).thenReturn(Mono.just(metrics(1, 1, 1)));
        when(alertPublisher.publish(any())).thenReturn(Mono.error(new IllegalStateException("offline")));

        StepVerifier.create(service.reconcile(date, TriggerType.MANUAL, "corr"))
                .assertNext(report -> assertThat(report.status())
                        .isEqualTo(ReconciliationStatus.MISMATCH))
                .verifyComplete();
    }
    @Test
    void distinguishesDuplicateEventsProjectionLossAndAmountDrift() {
        LocalDate date = LocalDate.parse("2026-07-17");
        when(kafkaCounts.snapshot(date)).thenReturn(Mono.just(snapshot(date, 10)));
        when(metricsStore.find(date)).thenReturn(Mono.just(
                metrics(10, 9, 8, 8, "100.0000", "99.5000")));
        when(alertPublisher.publish(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.reconcile(date, TriggerType.MANUAL, "corr"))
                .assertNext(report -> assertThat(report.issues())
                        .extracting(issue -> issue.type())
                        .containsExactly(
                                MismatchType.SOURCE_OBSERVATIONS_VS_UNIQUE_EVENTS,
                                MismatchType.UNIQUE_EVENTS_VS_DATABASE,
                                MismatchType.DATABASE_VS_AGGREGATE_AMOUNT))
                .verifyComplete();
    }

    private KafkaDaySnapshot snapshot(LocalDate date, long count) {
        return new KafkaDaySnapshot(
                "events", date, java.util.List.of(new KafkaPartitionRange(0, 0, count)));
    }
}
