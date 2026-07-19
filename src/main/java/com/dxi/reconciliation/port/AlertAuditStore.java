package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.AlertAudit;
import com.dxi.reconciliation.domain.AlertStatus;
import java.time.Instant;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Stores durable alert outbox entries and their delivery state. */
public interface AlertAuditStore {

    /** Creates a pending delivery or returns the existing report/channel entry. */
    Mono<AlertAudit> createOrGet(AlertAudit audit);

    /** Finds stale pending or failed deliveries in deterministic order. */
    Flux<AlertAudit> findRecoverable(Instant staleBefore, int limit);

    /** Completes one delivery attempt and increments its attempt counter. */
    Mono<Void> complete(UUID alertId, AlertStatus status, String errorMessage, Instant updatedAt);
}
