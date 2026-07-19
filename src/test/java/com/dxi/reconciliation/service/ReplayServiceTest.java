package com.dxi.reconciliation.service;

import static com.dxi.reconciliation.support.TestFixtures.NOW;
import static com.dxi.reconciliation.support.TestFixtures.properties;
import static com.dxi.reconciliation.support.TestFixtures.requestedJob;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dxi.reconciliation.domain.ReplayExecutionResult;
import com.dxi.reconciliation.domain.ReplayJob;
import com.dxi.reconciliation.domain.ReplayStatus;
import com.dxi.reconciliation.domain.TriggerType;
import com.dxi.reconciliation.port.DailyAggregateRepairStore;
import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.port.ReplayCheckpointStore;
import com.dxi.reconciliation.port.ReplayCommandPublisher;
import com.dxi.reconciliation.port.ReplayExecutor;
import com.dxi.reconciliation.port.ReplayJobStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class ReplayServiceTest {

    @Mock private ReplayJobStore jobStore;
    @Mock private ReplayCheckpointStore checkpointStore;
    @Mock private ReplayCommandPublisher commandPublisher;
    @Mock private ReplayExecutor replayExecutor;
    @Mock private DailyAggregateRepairStore repairStore;
    @Mock private ReconciliationService reconciliationService;
    @Mock private DistributedLockService lockService;
    private ReplayService service;

    @BeforeEach
    void setUp() {
        lenient().when(lockService.withLock(anyString(), any(), any()))
                .thenAnswer(invocation -> invocation.getArgument(2));
        service = new ReplayService(
                jobStore,
                checkpointStore,
                commandPublisher,
                replayExecutor,
                repairStore,
                reconciliationService,
                lockService,
                properties(),
                Clock.fixed(NOW, ZoneOffset.UTC),
                new SimpleMeterRegistry());
    }

    @Test
    void storesRequestBeforePublishingCommand() {
        when(jobStore.create(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(commandPublisher.publish(any())).thenReturn(Mono.empty());
        when(jobStore.markCommandPublished(any(), any())).thenReturn(Mono.empty());

        StepVerifier.create(service.request(
                        "key", LocalDate.parse("2026-07-17"), LocalDate.parse("2026-07-17"),
                        false, "operator", "corr"))
                .assertNext(job -> assertThat(job.status()).isEqualTo(ReplayStatus.REQUESTED))
                .verifyComplete();

        verify(jobStore).create(any());
        verify(commandPublisher).publish(any());
    }

    @Test
    void returnsExistingIdempotentRequestWithoutRedispatching() {
        ReplayJob existing = requestedJob(false);
        when(jobStore.create(any())).thenReturn(Mono.error(new DuplicateKeyException("duplicate")));
        when(jobStore.findByIdempotencyKey(existing.idempotencyKey())).thenReturn(Mono.just(existing));

        StepVerifier.create(service.request(
                        existing.idempotencyKey(), existing.fromDate(), existing.toDate(),
                        existing.dryRun(), existing.requestedBy(), "new-correlation"))
                .expectNext(existing)
                .verifyComplete();

        verify(commandPublisher, never()).publish(any());
        verify(jobStore, never()).markCommandPublished(any(), any());
    }

    @Test
    void rejectsReusedIdempotencyKeyForDifferentRequest() {
        ReplayJob existing = requestedJob(false);
        when(jobStore.create(any())).thenReturn(Mono.error(new DuplicateKeyException("duplicate")));
        when(jobStore.findByIdempotencyKey(existing.idempotencyKey())).thenReturn(Mono.just(existing));

        StepVerifier.create(service.request(
                        existing.idempotencyKey(), existing.fromDate(), existing.toDate(),
                        true, existing.requestedBy(), "new-correlation"))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(ConflictException.class);
                    assertThat(error.getMessage()).contains("different replay request");
                })
                .verify();

        verify(commandPublisher, never()).publish(any());
    }

    @Test
    void leavesDurableRequestForRecoveryWhenKafkaIsUnavailable() {
        when(jobStore.create(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(commandPublisher.publish(any())).thenReturn(Mono.error(new IllegalStateException("offline")));

        StepVerifier.create(service.request(
                        "key", LocalDate.parse("2026-07-17"), LocalDate.parse("2026-07-17"),
                        false, "operator", "corr"))
                .assertNext(job -> assertThat(job.status()).isEqualTo(ReplayStatus.REQUESTED))
                .verifyComplete();
    }

    @Test
    void rejectsReplayRangeInTheFuture() {
        assertThatThrownBy(() -> service.request(
                "key", LocalDate.parse("2026-07-18"), LocalDate.parse("2026-07-19"),
                false, "operator", "corr"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("future");
    }

    @Test
    void rejectsReplayRangeBeyondConfiguredLimit() {
        assertThatThrownBy(() -> service.request(
                "key", LocalDate.parse("2026-01-01"), LocalDate.parse("2026-03-01"),
                false, "operator", "corr"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void executesRealReplayThenRepairsAndValidates() {
        ReplayJob requested = requestedJob(false);
        when(jobStore.findById(requested.jobId())).thenReturn(Mono.just(requested));
        when(jobStore.update(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(replayExecutor.execute(any())).thenReturn(Mono.just(new ReplayExecutionResult(7, 7)));
        when(repairStore.repair(requested.fromDate())).thenReturn(Mono.empty());
        when(reconciliationService.reconcile(
                requested.fromDate(), TriggerType.REPLAY_VALIDATION, requested.correlationId()))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.execute(requested.jobId()))
                .assertNext(job -> {
                    assertThat(job.status()).isEqualTo(ReplayStatus.COMPLETED);
                    assertThat(job.discoveredEvents()).isEqualTo(7);
                    assertThat(job.replayedEvents()).isEqualTo(7);
                })
                .verifyComplete();

        verify(repairStore).repair(requested.fromDate());
    }

    @Test
    void dryRunDoesNotRepairOrValidate() {
        ReplayJob requested = requestedJob(true);
        when(jobStore.findById(requested.jobId())).thenReturn(Mono.just(requested));
        when(jobStore.update(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(replayExecutor.execute(any())).thenReturn(Mono.just(new ReplayExecutionResult(5, 0)));

        StepVerifier.create(service.execute(requested.jobId()))
                .assertNext(job -> assertThat(job.replayedEvents()).isZero())
                .verifyComplete();

        verify(repairStore, never()).repair(any());
        verify(reconciliationService, never()).reconcile(any(), any(), any());
    }

    @Test
    void rejectsStaleCommandsForFailedJobsUntilExplicitRetry() {
        ReplayJob requested = requestedJob(false);
        ReplayJob failed = new ReplayJob(
                requested.jobId(), requested.idempotencyKey(), requested.fromDate(),
                requested.toDate(), requested.dryRun(), ReplayStatus.FAILED, 10, 8, 1,
                requested.requestedBy(), requested.correlationId(), "worker stopped",
                requested.requestedAt(), NOW.minusSeconds(120), NOW.minusSeconds(60),
                NOW.minusSeconds(120), NOW.minusSeconds(60));
        when(jobStore.findById(failed.jobId())).thenReturn(Mono.just(failed));

        StepVerifier.create(service.execute(failed.jobId()))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(ConflictException.class);
                    assertThat(error.getMessage()).contains("explicit retry");
                })
                .verify();

        verify(replayExecutor, never()).execute(any());
    }

    @Test
    void retriesFailedJobWithoutDiscardingProgress() {
        ReplayJob requested = requestedJob(false);
        ReplayJob failed = new ReplayJob(
                requested.jobId(), requested.idempotencyKey(), requested.fromDate(),
                requested.toDate(), requested.dryRun(), ReplayStatus.FAILED, 10, 8, 1,
                requested.requestedBy(), requested.correlationId(), "worker stopped",
                requested.requestedAt(), NOW.minusSeconds(120), NOW.minusSeconds(60),
                NOW.minusSeconds(120), NOW.minusSeconds(60));
        when(jobStore.findById(failed.jobId())).thenReturn(Mono.just(failed));
        when(jobStore.update(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(commandPublisher.publish(any())).thenReturn(Mono.empty());
        when(jobStore.markCommandPublished(any(), any())).thenReturn(Mono.empty());

        StepVerifier.create(service.retry(failed.jobId()))
                .assertNext(job -> {
                    assertThat(job.status()).isEqualTo(ReplayStatus.REQUESTED);
                    assertThat(job.discoveredEvents()).isEqualTo(10);
                    assertThat(job.replayedEvents()).isEqualTo(8);
                    assertThat(job.attemptCount()).isEqualTo(1);
                })
                .verifyComplete();
    }

    @Test
    void staleRunningJobIsRecoveredForCheckpointResume() {
        ReplayJob requested = requestedJob(false);
        ReplayJob stale = new ReplayJob(
                requested.jobId(), requested.idempotencyKey(), requested.fromDate(),
                requested.toDate(), requested.dryRun(), ReplayStatus.RUNNING, 20, 18, 1,
                requested.requestedBy(), requested.correlationId(), null,
                requested.requestedAt(), NOW.minusSeconds(1_200), null,
                NOW.minusSeconds(1_200), NOW.minusSeconds(1_200));
        when(jobStore.findById(stale.jobId())).thenReturn(Mono.just(stale));
        when(jobStore.update(any())).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(commandPublisher.publish(any())).thenReturn(Mono.empty());
        when(jobStore.markCommandPublished(any(), any())).thenReturn(Mono.empty());

        StepVerifier.create(service.recoverStale(stale))
                .assertNext(job -> {
                    assertThat(job.status()).isEqualTo(ReplayStatus.REQUESTED);
                    assertThat(job.discoveredEvents()).isEqualTo(20);
                    assertThat(job.replayedEvents()).isEqualTo(18);
                })
                .verifyComplete();
    }

}
