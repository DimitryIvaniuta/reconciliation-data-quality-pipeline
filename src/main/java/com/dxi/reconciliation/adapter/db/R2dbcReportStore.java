package com.dxi.reconciliation.adapter.db;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.KafkaPartitionRange;
import com.dxi.reconciliation.domain.ReconciliationIssue;
import com.dxi.reconciliation.domain.ReconciliationReport;
import com.dxi.reconciliation.domain.ReconciliationStatus;
import com.dxi.reconciliation.domain.TriggerType;
import com.dxi.reconciliation.port.ReportStore;
import com.dxi.reconciliation.service.JsonCodec;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** PostgreSQL append-only reconciliation report repository. */
public class R2dbcReportStore implements ReportStore {

    private static final String COLUMNS = """
            report_id, business_date, trigger_type, status, kafka_event_count,
            consumed_event_count, unique_event_count, db_record_count,
            aggregate_record_count, db_total_amount, aggregate_amount,
            source_offsets, issues, correlation_id, created_at
            """;

    private final DatabaseClient client;
    private final JsonCodec jsonCodec;
    private final TransactionalOperator transactions;
    private final AppProperties properties;

    /** Creates the report and durable alert-outbox adapter. */
    public R2dbcReportStore(
            DatabaseClient client,
            JsonCodec jsonCodec,
            TransactionalOperator transactions,
            AppProperties properties) {
        this.client = client;
        this.jsonCodec = jsonCodec;
        this.transactions = transactions;
        this.properties = properties;
    }

    /** Appends a report without updating prior evidence. */
    @Override
    public Mono<ReconciliationReport> save(ReconciliationReport report) {
        Mono<Void> insertReport = client.sql("""
                        INSERT INTO reconciliation_reports (
                            report_id, business_date, trigger_type, status, kafka_event_count,
                            consumed_event_count, unique_event_count, db_record_count,
                            aggregate_record_count, db_total_amount, aggregate_amount,
                            source_offsets, issues, correlation_id, created_at)
                        VALUES (:reportId, :businessDate, :triggerType, :status, :kafkaCount,
                                :consumedCount, :uniqueCount, :databaseCount, :aggregateCount,
                                :databaseAmount, :aggregateAmount, CAST(:sourceOffsets AS JSONB),
                                CAST(:issues AS JSONB), :correlationId, :createdAt)
                        """)
                .bind("reportId", report.reportId())
                .bind("businessDate", report.businessDate())
                .bind("triggerType", report.triggerType().name())
                .bind("status", report.status().name())
                .bind("kafkaCount", report.kafkaEventCount())
                .bind("consumedCount", report.consumedEventCount())
                .bind("uniqueCount", report.uniqueEventCount())
                .bind("databaseCount", report.databaseRecordCount())
                .bind("aggregateCount", report.aggregateRecordCount())
                .bind("databaseAmount", report.databaseAmount())
                .bind("aggregateAmount", report.aggregateAmount())
                .bind("sourceOffsets", jsonCodec.write(report.sourceOffsets()))
                .bind("issues", jsonCodec.write(report.issues()))
                .bind("correlationId", report.correlationId())
                .bind("createdAt", report.createdAt())
                .fetch()
                .rowsUpdated()
                .then();
        Mono<Void> outbox = report.status() == ReconciliationStatus.MISMATCH
                ? Flux.fromIterable(configuredAlertChannels())
                        .concatMap(channel -> insertAlertOutbox(report, channel))
                        .then()
                : Mono.empty();
        return transactions.transactional(insertReport.then(outbox).thenReturn(report));
    }

    /** Finds one report by identifier. */
    @Override
    public Mono<ReconciliationReport> findById(UUID reportId) {
        return client.sql("SELECT " + COLUMNS
                        + " FROM reconciliation_reports WHERE report_id = :reportId")
                .bind("reportId", reportId)
                .map(this::mapReport)
                .one();
    }

    /** Finds reports using optional filters and stable newest-first ordering. */
    @Override
    public Flux<ReconciliationReport> find(
            LocalDate fromDate,
            LocalDate toDate,
            ReconciliationStatus status,
            int page,
            int size) {
        StringBuilder sql = new StringBuilder("SELECT ").append(COLUMNS)
                .append(" FROM reconciliation_reports WHERE TRUE");
        if (fromDate != null) {
            sql.append(" AND business_date >= :fromDate");
        }
        if (toDate != null) {
            sql.append(" AND business_date <= :toDate");
        }
        if (status != null) {
            sql.append(" AND status = :status");
        }
        sql.append(" ORDER BY created_at DESC, report_id DESC LIMIT :limit OFFSET :offset");
        DatabaseClient.GenericExecuteSpec spec = client.sql(sql.toString());
        if (fromDate != null) {
            spec = spec.bind("fromDate", fromDate);
        }
        if (toDate != null) {
            spec = spec.bind("toDate", toDate);
        }
        if (status != null) {
            spec = spec.bind("status", status.name());
        }
        return spec.bind("limit", size)
                .bind("offset", Math.multiplyExact(page, size))
                .map(this::mapReport)
                .all();
    }

    private ReconciliationReport mapReport(
            io.r2dbc.spi.Row row,
            io.r2dbc.spi.RowMetadata metadata) {
        ReconciliationIssue[] issues = jsonCodec.read(
                String.valueOf(row.get("issues")), ReconciliationIssue[].class);
        KafkaPartitionRange[] offsets = jsonCodec.read(
                String.valueOf(row.get("source_offsets")), KafkaPartitionRange[].class);
        return new ReconciliationReport(
                row.get("report_id", UUID.class),
                row.get("business_date", LocalDate.class),
                TriggerType.valueOf(row.get("trigger_type", String.class)),
                ReconciliationStatus.valueOf(row.get("status", String.class)),
                number(row.get("kafka_event_count")),
                number(row.get("consumed_event_count")),
                number(row.get("unique_event_count")),
                number(row.get("db_record_count")),
                number(row.get("aggregate_record_count")),
                decimal(row.get("db_total_amount", BigDecimal.class)),
                decimal(row.get("aggregate_amount", BigDecimal.class)),
                List.copyOf(Arrays.asList(offsets)),
                List.copyOf(Arrays.asList(issues)),
                row.get("correlation_id", String.class),
                row.get("created_at", Instant.class));
    }

    private Mono<Void> insertAlertOutbox(ReconciliationReport report, String channel) {
        return client.sql("""
                        INSERT INTO reconciliation_alerts (
                            alert_id, report_id, channel, status, attempt_count,
                            error_message, created_at, updated_at)
                        VALUES (:alertId, :reportId, :channel, 'PENDING', 0,
                                NULL, :createdAt, :updatedAt)
                        ON CONFLICT (report_id, channel) DO NOTHING
                        """)
                .bind("alertId", UUID.randomUUID())
                .bind("reportId", report.reportId())
                .bind("channel", channel)
                .bind("createdAt", report.createdAt())
                .bind("updatedAt", report.createdAt())
                .fetch()
                .rowsUpdated()
                .then();
    }

    private List<String> configuredAlertChannels() {
        return properties.alert().webhookUrl() == null
                ? List.of("KAFKA")
                : List.of("KAFKA", "WEBHOOK");
    }

    private long number(Object value) {
        return value == null ? 0 : ((Number) value).longValue();
    }

    private BigDecimal decimal(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }
}
