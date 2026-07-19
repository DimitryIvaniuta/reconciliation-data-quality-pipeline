package com.dxi.reconciliation.adapter.db;

import com.dxi.reconciliation.domain.BusinessEvent;
import com.dxi.reconciliation.domain.ProjectionResult;
import com.dxi.reconciliation.domain.SourceMetadata;
import com.dxi.reconciliation.port.EventProjectionStore;
import com.dxi.reconciliation.service.ConflictException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

/** PostgreSQL R2DBC implementation of atomic source observation and projection. */
public class R2dbcEventProjectionStore implements EventProjectionStore {

    private static final String INSERT_LEDGER = """
            INSERT INTO business_event_ledger (
                event_id, business_date, event_time, source_topic, source_partition,
                source_offset, source_timestamp, payload)
            VALUES (:eventId, :businessDate, :eventTime, :topic, :partition,
                    :offset, :sourceTimestamp, CAST(:payload AS JSONB))
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """;

    private static final String LEDGER_PAYLOAD_MATCHES = """
            SELECT payload = CAST(:payload AS JSONB) AS matches
            FROM business_event_ledger
            WHERE event_id = :eventId
            """;

    private static final String INSERT_OBSERVATION = """
            INSERT INTO source_event_observations (
                source_topic, source_partition, source_offset, event_id,
                business_date, source_timestamp, payload_hash)
            VALUES (:topic, :partition, :offset, :eventId, :businessDate,
                    :sourceTimestamp,
                    encode(sha256(convert_to(:payload, 'UTF8')), 'hex'))
            ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING
            RETURNING event_id
            """;

    private static final String OBSERVATION_MATCHES = """
            SELECT event_id = :eventId AS matches
            FROM source_event_observations
            WHERE source_topic = :topic
              AND source_partition = :partition
              AND source_offset = :offset
            """;

    private static final String INSERT_RECORD = """
            INSERT INTO business_records (
                event_id, business_key, business_date, event_time, amount, payload_hash)
            VALUES (:eventId, :businessKey, :businessDate, :eventTime, :amount,
                    encode(sha256(convert_to(:payload, 'UTF8')), 'hex'))
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """;

    private static final String UPSERT_AGGREGATE = """
            INSERT INTO daily_aggregates (business_date, record_count, total_amount)
            VALUES (:businessDate, 1, :amount)
            ON CONFLICT (business_date) DO UPDATE
            SET record_count = daily_aggregates.record_count + 1,
                total_amount = daily_aggregates.total_amount + EXCLUDED.total_amount,
                updated_at = clock_timestamp()
            """;

    private final DatabaseClient client;
    private final TransactionalOperator transactions;

    /** Creates the PostgreSQL projection adapter. */
    public R2dbcEventProjectionStore(DatabaseClient client, TransactionalOperator transactions) {
        this.client = client;
        this.transactions = transactions;
    }

    /** Applies one source record atomically and verifies every idempotency collision. */
    @Override
    public Mono<ProjectionResult> project(
            BusinessEvent event,
            SourceMetadata source,
            String canonicalPayload) {
        LocalDate date = source.recordTimestamp().atZone(ZoneOffset.UTC).toLocalDate();
        Mono<ProjectionResult> work = insertLedger(event, source, canonicalPayload, date)
                .flatMap(uniqueInserted -> ensureImmutableEvent(
                                event.eventId(), canonicalPayload, uniqueInserted)
                        .then(insertObservation(event, source, canonicalPayload, date))
                        .flatMap(observationInserted -> ensureImmutableObservation(
                                        event.eventId(), source, observationInserted)
                                .then(insertBusinessRecord(event, canonicalPayload, date))
                                .flatMap(recordInserted -> incrementAggregateIfNeeded(
                                                date, event, recordInserted)
                                        .thenReturn(new ProjectionResult(
                                                event.eventId(),
                                                observationInserted,
                                                uniqueInserted,
                                                recordInserted)))));
        return transactions.transactional(work);
    }

    private Mono<Boolean> insertLedger(
            BusinessEvent event,
            SourceMetadata source,
            String payload,
            LocalDate date) {
        return client.sql(INSERT_LEDGER)
                .bind("eventId", event.eventId())
                .bind("businessDate", date)
                .bind("eventTime", event.eventTime())
                .bind("topic", source.topic())
                .bind("partition", source.partition())
                .bind("offset", source.offset())
                .bind("sourceTimestamp", source.recordTimestamp())
                .bind("payload", payload)
                .map((row, metadata) -> row.get("event_id", UUID.class))
                .one()
                .map(ignored -> true)
                .defaultIfEmpty(false);
    }

    private Mono<Void> ensureImmutableEvent(UUID eventId, String payload, boolean inserted) {
        if (inserted) {
            return Mono.empty();
        }
        return client.sql(LEDGER_PAYLOAD_MATCHES)
                .bind("eventId", eventId)
                .bind("payload", payload)
                .map((row, metadata) -> Boolean.TRUE.equals(row.get("matches", Boolean.class)))
                .one()
                .switchIfEmpty(Mono.error(new ConflictException(
                        "Event ID collision could not be resolved: " + eventId)))
                .flatMap(matches -> matches
                        ? Mono.empty()
                        : Mono.error(new ConflictException(
                                "Event ID already exists with a different immutable payload: "
                                        + eventId)));
    }

    private Mono<Boolean> insertObservation(
            BusinessEvent event,
            SourceMetadata source,
            String payload,
            LocalDate date) {
        return client.sql(INSERT_OBSERVATION)
                .bind("topic", source.topic())
                .bind("partition", source.partition())
                .bind("offset", source.offset())
                .bind("eventId", event.eventId())
                .bind("businessDate", date)
                .bind("sourceTimestamp", source.recordTimestamp())
                .bind("payload", payload)
                .map((row, metadata) -> row.get("event_id", UUID.class))
                .one()
                .map(ignored -> true)
                .defaultIfEmpty(false);
    }

    private Mono<Void> ensureImmutableObservation(
            UUID eventId,
            SourceMetadata source,
            boolean inserted) {
        if (inserted) {
            return Mono.empty();
        }
        return client.sql(OBSERVATION_MATCHES)
                .bind("eventId", eventId)
                .bind("topic", source.topic())
                .bind("partition", source.partition())
                .bind("offset", source.offset())
                .map((row, metadata) -> Boolean.TRUE.equals(row.get("matches", Boolean.class)))
                .one()
                .switchIfEmpty(Mono.error(new ConflictException(
                        "Kafka source position collision could not be resolved")))
                .flatMap(matches -> matches
                        ? Mono.empty()
                        : Mono.error(new ConflictException(
                                "Kafka source position is associated with another event or payload")));
    }

    private Mono<Boolean> insertBusinessRecord(
            BusinessEvent event,
            String payload,
            LocalDate date) {
        return client.sql(INSERT_RECORD)
                .bind("eventId", event.eventId())
                .bind("businessKey", event.businessKey())
                .bind("businessDate", date)
                .bind("eventTime", event.eventTime())
                .bind("amount", event.amount())
                .bind("payload", payload)
                .map((row, metadata) -> row.get("event_id", UUID.class))
                .one()
                .map(ignored -> true)
                .defaultIfEmpty(false);
    }

    private Mono<Void> incrementAggregateIfNeeded(
            LocalDate date,
            BusinessEvent event,
            boolean recordInserted) {
        if (!recordInserted) {
            return Mono.empty();
        }
        return client.sql(UPSERT_AGGREGATE)
                .bind("businessDate", date)
                .bind("amount", event.amount())
                .fetch()
                .rowsUpdated()
                .then();
    }
}
