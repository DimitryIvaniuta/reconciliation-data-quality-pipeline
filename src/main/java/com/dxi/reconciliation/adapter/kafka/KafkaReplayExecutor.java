package com.dxi.reconciliation.adapter.kafka;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.ReplayCheckpointStatus;
import com.dxi.reconciliation.domain.ReplayExecutionResult;
import com.dxi.reconciliation.domain.ReplayJob;
import com.dxi.reconciliation.domain.ReplayPartitionCheckpoint;
import com.dxi.reconciliation.domain.SourceMetadata;
import com.dxi.reconciliation.port.ReplayCheckpointStore;
import com.dxi.reconciliation.port.ReplayExecutor;
import com.dxi.reconciliation.port.ReplayJobStore;
import com.dxi.reconciliation.service.BusinessEventProcessor;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Manually assigned Kafka source scanner with durable per-partition checkpoints. */
public class KafkaReplayExecutor implements ReplayExecutor {

    private final Consumer<String, String> consumer;
    private final BusinessEventProcessor processor;
    private final ReplayCheckpointStore checkpointStore;
    private final ReplayJobStore jobStore;
    private final AppProperties properties;
    private final Clock clock;

    /** Creates the resumable replay source scanner. */
    public KafkaReplayExecutor(
            Consumer<String, String> consumer,
            BusinessEventProcessor processor,
            ReplayCheckpointStore checkpointStore,
            ReplayJobStore jobStore,
            AppProperties properties,
            Clock clock) {
        this.consumer = consumer;
        this.processor = processor;
        this.checkpointStore = checkpointStore;
        this.jobStore = jobStore;
        this.properties = properties;
        this.clock = clock;
    }

    /** Scans a stable source-offset snapshot and synchronously repairs missing projections. */
    @Override
    public Mono<ReplayExecutionResult> execute(ReplayJob job) {
        return Mono.fromCallable(() -> executeBlocking(job))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private synchronized ReplayExecutionResult executeBlocking(ReplayJob job) {
        List<ReplayPartitionCheckpoint> checkpoints = loadOrCreateCheckpoints(job);
        long discovered = checkpoints.stream()
                .mapToLong(ReplayPartitionCheckpoint::discoveredEvents)
                .sum();
        long replayed = checkpoints.stream()
                .mapToLong(ReplayPartitionCheckpoint::replayedEvents)
                .sum();
        try {
            for (ReplayPartitionCheckpoint checkpoint : checkpoints) {
                if (checkpoint.status() == ReplayCheckpointStatus.COMPLETED) {
                    continue;
                }
                PartitionProgress progress = processPartition(job, checkpoint, discovered, replayed);
                discovered = progress.totalDiscovered();
                replayed = progress.totalReplayed();
            }
            return new ReplayExecutionResult(discovered, replayed);
        } finally {
            consumer.unsubscribe();
        }
    }

    private List<ReplayPartitionCheckpoint> loadOrCreateCheckpoints(ReplayJob job) {
        List<ReplayPartitionCheckpoint> existing = checkpointStore.findByJobId(job.jobId())
                .collectList()
                .block(properties.kafka().adminTimeout());
        if (existing != null && !existing.isEmpty()) {
            return existing;
        }

        String topic = properties.kafka().sourceTopic();
        List<TopicPartition> partitions = consumer.partitionsFor(
                        topic, properties.kafka().adminTimeout())
                .stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .sorted((left, right) -> Integer.compare(left.partition(), right.partition()))
                .toList();
        if (partitions.isEmpty()) {
            return List.of();
        }

        long fromTimestamp = job.fromDate().atStartOfDay(ZoneOffset.UTC)
                .toInstant().toEpochMilli();
        long toTimestamp = job.toDate().plusDays(1).atStartOfDay(ZoneOffset.UTC)
                .toInstant().toEpochMilli();
        Map<TopicPartition, Long> snapshotEnds = consumer.endOffsets(
                partitions, properties.kafka().adminTimeout());
        Map<TopicPartition, Long> starts = timestampOffsets(
                partitions, fromTimestamp, snapshotEnds);
        Map<TopicPartition, Long> ends = timestampOffsets(
                partitions, toTimestamp, snapshotEnds);

        List<ReplayPartitionCheckpoint> created = new ArrayList<>();
        for (TopicPartition partition : partitions) {
            long start = starts.get(partition);
            long end = Math.max(start, ends.get(partition));
            ReplayCheckpointStatus status = start == end
                    ? ReplayCheckpointStatus.COMPLETED
                    : ReplayCheckpointStatus.PENDING;
            ReplayPartitionCheckpoint checkpoint = new ReplayPartitionCheckpoint(
                    job.jobId(), topic, partition.partition(), start, end, start,
                    0, 0, status, clock.instant());
            ReplayPartitionCheckpoint persisted = checkpointStore.createIfAbsent(checkpoint)
                    .block(properties.kafka().adminTimeout());
            if (persisted != null) {
                created.add(persisted);
            }
        }
        return created;
    }

    private PartitionProgress processPartition(
            ReplayJob job,
            ReplayPartitionCheckpoint initial,
            long priorDiscovered,
            long priorReplayed) {
        TopicPartition partition = new TopicPartition(
                initial.sourceTopic(), initial.sourcePartition());
        consumer.assign(List.of(partition));
        consumer.seek(partition, initial.nextOffset());

        long partitionDiscovered = initial.discoveredEvents();
        long partitionReplayed = initial.replayedEvents();
        long discoveredBase = priorDiscovered - initial.discoveredEvents();
        long replayedBase = priorReplayed - initial.replayedEvents();
        long recordsSinceCheckpoint = 0;
        ReplayPartitionCheckpoint current = initial.progress(
                initial.nextOffset(), partitionDiscovered, partitionReplayed,
                ReplayCheckpointStatus.RUNNING, clock.instant());
        persistCheckpoint(job, current, discoveredBase + partitionDiscovered,
                replayedBase + partitionReplayed);

        while (consumer.position(partition, properties.kafka().adminTimeout())
                < initial.endOffset()) {
            ConsumerRecords<String, String> records = consumer.poll(
                    properties.kafka().replayPollTimeout());
            if (records.isEmpty()) {
                updateHeartbeat(job, discoveredBase + partitionDiscovered,
                        replayedBase + partitionReplayed);
            }
            for (ConsumerRecord<String, String> record : records.records(partition)) {
                if (record.offset() >= initial.endOffset()) {
                    break;
                }
                partitionDiscovered++;
                if (!job.dryRun()) {
                    SourceMetadata source = new SourceMetadata(
                            record.topic(), record.partition(), record.offset(),
                            Instant.ofEpochMilli(record.timestamp()));
                    processor.process(record.value(), source)
                            .block(properties.kafka().adminTimeout());
                    partitionReplayed++;
                }
                recordsSinceCheckpoint++;
                long nextOffset = record.offset() + 1;
                if (recordsSinceCheckpoint >= properties.reconciliation()
                        .replayCheckpointInterval()) {
                    current = initial.progress(
                            nextOffset, partitionDiscovered, partitionReplayed,
                            ReplayCheckpointStatus.RUNNING, clock.instant());
                    persistCheckpoint(job, current, discoveredBase + partitionDiscovered,
                            replayedBase + partitionReplayed);
                    recordsSinceCheckpoint = 0;
                }
            }
        }

        ReplayPartitionCheckpoint completed = initial.progress(
                initial.endOffset(), partitionDiscovered, partitionReplayed,
                ReplayCheckpointStatus.COMPLETED, clock.instant());
        long totalDiscovered = discoveredBase + partitionDiscovered;
        long totalReplayed = replayedBase + partitionReplayed;
        persistCheckpoint(job, completed, totalDiscovered, totalReplayed);
        return new PartitionProgress(totalDiscovered, totalReplayed);
    }

    private void persistCheckpoint(
            ReplayJob job,
            ReplayPartitionCheckpoint checkpoint,
            long discoveredEvents,
            long replayedEvents) {
        jobStore.updateProgress(
                        checkpoint.jobId(), job.attemptCount(), discoveredEvents,
                        replayedEvents, clock.instant())
                .block(properties.kafka().adminTimeout());
        checkpointStore.update(checkpoint).block(properties.kafka().adminTimeout());
    }

    private void updateHeartbeat(
            ReplayJob job,
            long discoveredEvents,
            long replayedEvents) {
        jobStore.updateProgress(
                        job.jobId(), job.attemptCount(), discoveredEvents,
                        replayedEvents, clock.instant())
                .block(properties.kafka().adminTimeout());
    }

    private Map<TopicPartition, Long> timestampOffsets(
            List<TopicPartition> partitions,
            long timestamp,
            Map<TopicPartition, Long> fallbackEnds) {
        Map<TopicPartition, Long> timestamps = new HashMap<>();
        partitions.forEach(partition -> timestamps.put(partition, timestamp));
        Map<TopicPartition, OffsetAndTimestamp> resolved = consumer.offsetsForTimes(
                timestamps, properties.kafka().adminTimeout());
        Map<TopicPartition, Long> offsets = new HashMap<>();
        partitions.forEach(partition -> {
            OffsetAndTimestamp value = resolved.get(partition);
            long snapshotEnd = fallbackEnds.get(partition);
            offsets.put(partition, value == null
                    ? snapshotEnd
                    : Math.min(value.offset(), snapshotEnd));
        });
        return offsets;
    }

    private record PartitionProgress(long totalDiscovered, long totalReplayed) { }
}
