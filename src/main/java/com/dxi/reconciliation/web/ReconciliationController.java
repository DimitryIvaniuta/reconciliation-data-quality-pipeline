package com.dxi.reconciliation.web;

import com.dxi.reconciliation.domain.ReconciliationReport;
import com.dxi.reconciliation.domain.ReconciliationStatus;
import com.dxi.reconciliation.domain.TriggerType;
import com.dxi.reconciliation.service.ReconciliationService;
import com.dxi.reconciliation.service.ReportQueryService;
import java.time.LocalDate;
import java.util.UUID;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Manual execution and immutable report query API. */
@RestController
@RequestMapping("/api/v1/reconciliations")
public class ReconciliationController {

    private final ReconciliationService reconciliationService;
    private final ReportQueryService reportQueryService;

    /** Creates the reconciliation controller. */
    public ReconciliationController(
            ReconciliationService reconciliationService,
            ReportQueryService reportQueryService) {
        this.reconciliationService = reconciliationService;
        this.reportQueryService = reportQueryService;
    }

    /** Runs a manual reconciliation for one UTC date. */
    @PostMapping("/{date}/runs")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ReconciliationReport> run(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            ServerWebExchange exchange) {
        return reconciliationService.reconcile(
                date, TriggerType.MANUAL, RequestContext.correlationId(exchange));
    }

    /** Returns one immutable report. */
    @GetMapping("/{reportId}")
    public Mono<ReconciliationReport> get(@PathVariable UUID reportId) {
        return reportQueryService.get(reportId);
    }

    /** Searches immutable reports with optional date and status filters. */
    @GetMapping
    public Flux<ReconciliationReport> find(
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
            @RequestParam(required = false) ReconciliationStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {
        return reportQueryService.find(fromDate, toDate, status, page, size);
    }
}
