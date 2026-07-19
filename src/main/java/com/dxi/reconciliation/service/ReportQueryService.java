package com.dxi.reconciliation.service;

import com.dxi.reconciliation.domain.ReconciliationReport;
import com.dxi.reconciliation.domain.ReconciliationStatus;
import com.dxi.reconciliation.port.ReportStore;
import java.time.LocalDate;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Provides bounded read access to immutable reconciliation reports. */
public class ReportQueryService {

    private static final int MAX_PAGE_SIZE = 200;
    private final ReportStore reportStore;

    /** Creates the report query service. */
    public ReportQueryService(ReportStore reportStore) {
        this.reportStore = reportStore;
    }

    /** Returns one report or a domain-specific not-found error. */
    public Mono<ReconciliationReport> get(UUID reportId) {
        return reportStore.findById(reportId)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException(
                        "Reconciliation report not found: " + reportId)));
    }

    /** Searches reports after validating filters and pagination bounds. */
    public Flux<ReconciliationReport> find(
            LocalDate fromDate,
            LocalDate toDate,
            ReconciliationStatus status,
            int page,
            int size) {
        if (page < 0 || size < 1 || size > MAX_PAGE_SIZE) {
            return Flux.error(new IllegalArgumentException(
                    "page must be non-negative and size must be between 1 and " + MAX_PAGE_SIZE));
        }
        if (fromDate != null && toDate != null && fromDate.isAfter(toDate)) {
            return Flux.error(new IllegalArgumentException("fromDate must not be after toDate"));
        }
        return reportStore.find(fromDate, toDate, status, page, size);
    }
}
