package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.AlertAudit;
import com.dxi.reconciliation.domain.ReconciliationReport;
import reactor.core.publisher.Mono;

/** Publishes mismatch alerts through durable configured channels. */
public interface AlertPublisher {

    /** Delivers all configured outbox entries for one mismatch report. */
    Mono<Void> publish(ReconciliationReport report);

    /** Retries one existing pending or failed outbox entry. */
    Mono<Void> retry(AlertAudit audit, ReconciliationReport report);
}
