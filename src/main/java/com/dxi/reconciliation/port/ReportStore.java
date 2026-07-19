package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.ReconciliationReport;
import com.dxi.reconciliation.domain.ReconciliationStatus;
import java.time.LocalDate;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Persists and queries immutable reconciliation reports. */
public interface ReportStore {

    /** Appends a new report. */
    Mono<ReconciliationReport> save(ReconciliationReport report);

    /** Finds a report by identifier. */
    Mono<ReconciliationReport> findById(UUID reportId);

    /** Finds reports using optional filters and bounded pagination. */
    Flux<ReconciliationReport> find(
            LocalDate fromDate,
            LocalDate toDate,
            ReconciliationStatus status,
            int page,
            int size);
}
