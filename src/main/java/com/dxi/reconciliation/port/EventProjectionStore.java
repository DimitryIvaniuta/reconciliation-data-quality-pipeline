package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.BusinessEvent;
import com.dxi.reconciliation.domain.ProjectionResult;
import com.dxi.reconciliation.domain.SourceMetadata;
import reactor.core.publisher.Mono;

/** Persists an event and its projection atomically and idempotently. */
public interface EventProjectionStore {

    /** Applies a validated event to the immutable ledger and business projection. */
    Mono<ProjectionResult> project(BusinessEvent event, SourceMetadata source, String canonicalPayload);
}
