package com.dxi.reconciliation.port;

import java.time.LocalDate;
import reactor.core.publisher.Mono;

/** Rebuilds exact aggregate and metric state from indexed persisted rows. */
public interface DailyAggregateRepairStore {

    /** Repairs one ingestion day in a transaction. */
    Mono<Void> repair(LocalDate businessDate);
}
