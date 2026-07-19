package com.dxi.reconciliation.adapter.db;

import com.dxi.reconciliation.domain.DailyMetrics;
import com.dxi.reconciliation.port.DailyMetricsStore;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

/** Reads O(1) daily metric snapshots through R2DBC. */
public class R2dbcDailyMetricsStore implements DailyMetricsStore {

    private final DatabaseClient client;

    /** Creates the metrics adapter. */
    public R2dbcDailyMetricsStore(DatabaseClient client) {
        this.client = client;
    }

    /** Reads one exact compact snapshot. */
    @Override
    public Mono<DailyMetrics> find(LocalDate businessDate) {
        return client.sql("""
                        SELECT business_date, consumed_event_count, unique_event_count,
                               db_record_count, aggregate_record_count, db_total_amount,
                               aggregate_amount, updated_at
                        FROM daily_metrics
                        WHERE business_date = :businessDate
                        """)
                .bind("businessDate", businessDate)
                .map((row, metadata) -> new DailyMetrics(
                        row.get("business_date", LocalDate.class),
                        number(row.get("consumed_event_count")),
                        number(row.get("unique_event_count")),
                        number(row.get("db_record_count")),
                        number(row.get("aggregate_record_count")),
                        decimal(row.get("db_total_amount", BigDecimal.class)),
                        decimal(row.get("aggregate_amount", BigDecimal.class)),
                        row.get("updated_at", Instant.class)))
                .one()
                .defaultIfEmpty(DailyMetrics.empty(businessDate));
    }

    private long number(Object value) {
        return value == null ? 0 : ((Number) value).longValue();
    }

    private BigDecimal decimal(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }
}
