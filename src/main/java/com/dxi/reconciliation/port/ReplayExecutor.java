package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.ReplayExecutionResult;
import com.dxi.reconciliation.domain.ReplayJob;
import reactor.core.publisher.Mono;

/** Scans the source topic and optionally reapplies events through the normal projector. */
public interface ReplayExecutor {

    /** Executes the source scan for a job. */
    Mono<ReplayExecutionResult> execute(ReplayJob job);
}
