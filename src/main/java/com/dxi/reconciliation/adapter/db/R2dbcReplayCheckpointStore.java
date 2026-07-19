package com.dxi.reconciliation.adapter.db;

import com.dxi.reconciliation.domain.ReplayCheckpointStatus;
import com.dxi.reconciliation.domain.ReplayPartitionCheckpoint;
import com.dxi.reconciliation.port.ReplayCheckpointStore;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.time.Instant;
import java.util.UUID;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** PostgreSQL repository for resumable per-partition replay checkpoints. */
public class R2dbcReplayCheckpointStore implements ReplayCheckpointStore {

    private static final String COLUMNS = """
            job_id, source_topic, source_partition, start_offset, end_offset,
            next_offset, discovered_events, replayed_events, status, updated_at
            """;

    private final DatabaseClient client;

    /** Creates the replay checkpoint adapter. */
    public R2dbcReplayCheckpointStore(DatabaseClient client) {
        this.client = client;
    }

    /** Returns checkpoints in source partition order. */
    @Override
    public Flux<ReplayPartitionCheckpoint> findByJobId(UUID jobId) {
        return client.sql("SELECT " + COLUMNS
                        + " FROM replay_partition_checkpoints"
                        + " WHERE job_id = :jobId ORDER BY source_partition")
                .bind("jobId", jobId)
                .map(this::mapCheckpoint)
                .all();
    }

    /** Creates a checkpoint or returns the existing immutable range. */
    @Override
    public Mono<ReplayPartitionCheckpoint> createIfAbsent(
            ReplayPartitionCheckpoint checkpoint) {
        return client.sql("""
                        INSERT INTO replay_partition_checkpoints (
                            job_id, source_topic, source_partition, start_offset, end_offset,
                            next_offset, discovered_events, replayed_events, status, updated_at)
                        VALUES (:jobId, :topic, :partition, :startOffset, :endOffset,
                                :nextOffset, :discovered, :replayed, :status, :updatedAt)
                        ON CONFLICT (job_id, source_topic, source_partition) DO NOTHING
                        """)
                .bind("jobId", checkpoint.jobId())
                .bind("topic", checkpoint.sourceTopic())
                .bind("partition", checkpoint.sourcePartition())
                .bind("startOffset", checkpoint.startOffset())
                .bind("endOffset", checkpoint.endOffset())
                .bind("nextOffset", checkpoint.nextOffset())
                .bind("discovered", checkpoint.discoveredEvents())
                .bind("replayed", checkpoint.replayedEvents())
                .bind("status", checkpoint.status().name())
                .bind("updatedAt", checkpoint.updatedAt())
                .fetch()
                .rowsUpdated()
                .then(findOne(
                        checkpoint.jobId(),
                        checkpoint.sourceTopic(),
                        checkpoint.sourcePartition()));
    }

    /** Updates mutable progress while preserving the original bounded range. */
    @Override
    public Mono<ReplayPartitionCheckpoint> update(ReplayPartitionCheckpoint checkpoint) {
        return client.sql("""
                        UPDATE replay_partition_checkpoints
                        SET next_offset = :nextOffset,
                            discovered_events = :discovered,
                            replayed_events = :replayed,
                            status = :status,
                            updated_at = :updatedAt
                        WHERE job_id = :jobId
                          AND source_topic = :topic
                          AND source_partition = :partition
                          AND start_offset = :startOffset
                          AND end_offset = :endOffset
                        """)
                .bind("nextOffset", checkpoint.nextOffset())
                .bind("discovered", checkpoint.discoveredEvents())
                .bind("replayed", checkpoint.replayedEvents())
                .bind("status", checkpoint.status().name())
                .bind("updatedAt", checkpoint.updatedAt())
                .bind("jobId", checkpoint.jobId())
                .bind("topic", checkpoint.sourceTopic())
                .bind("partition", checkpoint.sourcePartition())
                .bind("startOffset", checkpoint.startOffset())
                .bind("endOffset", checkpoint.endOffset())
                .fetch()
                .rowsUpdated()
                .flatMap(updated -> updated == 1
                        ? Mono.just(checkpoint)
                        : Mono.error(new IllegalStateException(
                                "Replay checkpoint range changed or disappeared")));
    }

    private Mono<ReplayPartitionCheckpoint> findOne(
            UUID jobId,
            String topic,
            int partition) {
        return client.sql("SELECT " + COLUMNS
                        + " FROM replay_partition_checkpoints"
                        + " WHERE job_id = :jobId AND source_topic = :topic"
                        + " AND source_partition = :partition")
                .bind("jobId", jobId)
                .bind("topic", topic)
                .bind("partition", partition)
                .map(this::mapCheckpoint)
                .one();
    }

    private ReplayPartitionCheckpoint mapCheckpoint(Row row, RowMetadata metadata) {
        return new ReplayPartitionCheckpoint(
                row.get("job_id", UUID.class),
                row.get("source_topic", String.class),
                number(row.get("source_partition")).intValue(),
                number(row.get("start_offset")).longValue(),
                number(row.get("end_offset")).longValue(),
                number(row.get("next_offset")).longValue(),
                number(row.get("discovered_events")).longValue(),
                number(row.get("replayed_events")).longValue(),
                ReplayCheckpointStatus.valueOf(row.get("status", String.class)),
                row.get("updated_at", Instant.class));
    }

    private Number number(Object value) {
        return value == null ? 0L : (Number) value;
    }
}
