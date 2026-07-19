package com.dxi.reconciliation.web;

import com.dxi.reconciliation.domain.DailyMetrics;
import com.dxi.reconciliation.port.DailyMetricsStore;
import java.time.LocalDate;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/** Read-only daily metric API. */
@RestController
@RequestMapping("/api/v1/daily-metrics")
public class MetricsController {

    private final DailyMetricsStore metricsStore;

    /** Creates the metrics controller. */
    public MetricsController(DailyMetricsStore metricsStore) {
        this.metricsStore = metricsStore;
    }

    /** Returns compact exact counters for one UTC ingestion day. */
    @GetMapping("/{date}")
    public Mono<DailyMetrics> get(
            @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        return metricsStore.find(date).defaultIfEmpty(DailyMetrics.empty(date));
    }
}
