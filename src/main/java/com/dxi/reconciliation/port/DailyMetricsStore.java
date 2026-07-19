package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.DailyMetrics;
import java.time.LocalDate;
import reactor.core.publisher.Mono;

/** Reads compact exact daily database counters. */
public interface DailyMetricsStore {

    /** Returns metrics for a day, or a zero snapshot when no state exists. */
    Mono<DailyMetrics> find(LocalDate businessDate);
}
