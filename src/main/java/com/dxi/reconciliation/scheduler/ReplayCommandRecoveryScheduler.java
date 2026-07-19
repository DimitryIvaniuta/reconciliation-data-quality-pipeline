package com.dxi.reconciliation.scheduler;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.port.ReplayJobStore;
import com.dxi.reconciliation.service.ReplayService;
import java.time.Clock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Mono;

/** Recovers undispatched replay requests and stale running workers. */
@Slf4j
public class ReplayCommandRecoveryScheduler {

    private static final int BATCH_SIZE = 100;

    private final ReplayJobStore jobStore;
    private final ReplayService replayService;
    private final AppProperties properties;
    private final Clock clock;

    /** Creates the recovery scheduler. */
    public ReplayCommandRecoveryScheduler(
            ReplayJobStore jobStore,
            ReplayService replayService,
            AppProperties properties,
            Clock clock) {
        this.jobStore = jobStore;
        this.replayService = replayService;
        this.properties = properties;
        this.clock = clock;
    }

    /** Republishes undispatched commands and resumes stale running jobs in bounded batches. */
    @Scheduled(fixedDelayString = "${app.reconciliation.command-recovery-delay:PT30S}")
    public void recover() {
        Mono<Void> requestedRecovery = jobStore.findDispatchable(
                        clock.instant().minus(properties.reconciliation()
                                .commandRedispatchInterval()),
                        BATCH_SIZE)
                .concatMap(job -> replayService.dispatch(job)
                        .onErrorResume(exception -> {
                            log.warn("Unable to redispatch replay command {}", job.jobId(), exception);
                            return Mono.empty();
                        }))
                .then();

        Mono<Void> staleWorkerRecovery = jobStore.findStaleRunning(
                        clock.instant().minus(properties.reconciliation().replayStaleTimeout()),
                        properties.reconciliation().maximumReplayAttempts(),
                        BATCH_SIZE)
                .concatMap(job -> replayService.recoverStale(job)
                        .onErrorResume(exception -> {
                            log.warn("Unable to recover stale replay job {}", job.jobId(), exception);
                            return Mono.empty();
                        }))
                .then();

        requestedRecovery.then(staleWorkerRecovery).subscribe();
    }
}
