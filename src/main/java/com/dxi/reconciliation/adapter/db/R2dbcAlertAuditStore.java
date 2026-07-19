package com.dxi.reconciliation.adapter.db;

import com.dxi.reconciliation.domain.AlertAudit;
import com.dxi.reconciliation.domain.AlertStatus;
import com.dxi.reconciliation.port.AlertAuditStore;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.time.Instant;
import java.util.UUID;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** PostgreSQL durable alert-outbox repository. */
public class R2dbcAlertAuditStore implements AlertAuditStore {

    private static final String COLUMNS = """
            alert_id, report_id, channel, status, attempt_count,
            error_message, created_at, updated_at
            """;

    private final DatabaseClient client;

    /** Creates the alert audit adapter. */
    public R2dbcAlertAuditStore(DatabaseClient client) {
        this.client = client;
    }

    /** Creates a pending outbox entry or returns the existing channel entry. */
    @Override
    public Mono<AlertAudit> createOrGet(AlertAudit audit) {
        return client.sql("""
                        INSERT INTO reconciliation_alerts (
                            alert_id, report_id, channel, status, attempt_count,
                            error_message, created_at, updated_at)
                        VALUES (:alertId, :reportId, :channel, :status, :attemptCount,
                                :errorMessage, :createdAt, :updatedAt)
                        ON CONFLICT (report_id, channel) DO NOTHING
                        RETURNING alert_id, report_id, channel, status, attempt_count,
                                  error_message, created_at, updated_at
                        """)
                .bind("alertId", audit.alertId())
                .bind("reportId", audit.reportId())
                .bind("channel", audit.channel())
                .bind("status", audit.status().name())
                .bind("attemptCount", audit.attemptCount())
                .bindNull("errorMessage", String.class)
                .bind("createdAt", audit.createdAt())
                .bind("updatedAt", audit.updatedAt())
                .map(this::mapAudit)
                .one()
                .switchIfEmpty(findByReportAndChannel(audit.reportId(), audit.channel()));
    }

    /** Finds stale pending and failed entries for bounded retry. */
    @Override
    public Flux<AlertAudit> findRecoverable(Instant staleBefore, int limit) {
        return client.sql("SELECT " + COLUMNS + " FROM reconciliation_alerts "
                        + "WHERE status IN ('PENDING', 'FAILED') AND updated_at < :staleBefore "
                        + "ORDER BY updated_at ASC, created_at ASC LIMIT :limit")
                .bind("staleBefore", staleBefore)
                .bind("limit", limit)
                .map(this::mapAudit)
                .all();
    }

    /** Records one success or failure and increments the attempt count. */
    @Override
    public Mono<Void> complete(
            UUID alertId,
            AlertStatus status,
            String errorMessage,
            Instant updatedAt) {
        DatabaseClient.GenericExecuteSpec spec = client.sql("""
                        UPDATE reconciliation_alerts
                        SET status = :status,
                            attempt_count = attempt_count + 1,
                            error_message = :errorMessage,
                            updated_at = :updatedAt
                        WHERE alert_id = :alertId
                        """)
                .bind("status", status.name())
                .bind("updatedAt", updatedAt)
                .bind("alertId", alertId);
        spec = errorMessage == null
                ? spec.bindNull("errorMessage", String.class)
                : spec.bind("errorMessage", errorMessage);
        return spec.fetch().rowsUpdated().then();
    }

    private Mono<AlertAudit> findByReportAndChannel(UUID reportId, String channel) {
        return client.sql("SELECT " + COLUMNS + " FROM reconciliation_alerts "
                        + "WHERE report_id = :reportId AND channel = :channel")
                .bind("reportId", reportId)
                .bind("channel", channel)
                .map(this::mapAudit)
                .one();
    }

    private AlertAudit mapAudit(Row row, RowMetadata metadata) {
        Object attempts = row.get("attempt_count");
        return new AlertAudit(
                row.get("alert_id", UUID.class),
                row.get("report_id", UUID.class),
                row.get("channel", String.class),
                AlertStatus.valueOf(row.get("status", String.class)),
                attempts == null ? 0 : ((Number) attempts).intValue(),
                row.get("error_message", String.class),
                row.get("created_at", Instant.class),
                row.get("updated_at", Instant.class));
    }
}
