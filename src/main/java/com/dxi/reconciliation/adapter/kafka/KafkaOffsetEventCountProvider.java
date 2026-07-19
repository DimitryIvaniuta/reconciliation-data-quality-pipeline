package com.dxi.reconciliation.adapter.kafka;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.KafkaDaySnapshot;
import com.dxi.reconciliation.domain.KafkaPartitionRange;
import com.dxi.reconciliation.port.KafkaEventCountProvider;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/** Resolves source Kafka day boundaries with timestamp-to-offset lookups. */
public class KafkaOffsetEventCountProvider implements KafkaEventCountProvider {

    private final Admin admin;
    private final AppProperties properties;

    /** Creates the offset snapshot provider. */
    public KafkaOffsetEventCountProvider(Admin admin, AppProperties properties) {
        this.admin = admin;
        this.properties = properties;
    }

    /** Returns an immutable per-partition offset snapshot for one UTC day. */
    @Override
    public Mono<KafkaDaySnapshot> snapshot(LocalDate businessDate) {
        return Mono.fromCallable(() -> snapshotBlocking(businessDate))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private KafkaDaySnapshot snapshotBlocking(LocalDate date) throws Exception {
        String topic = properties.kafka().sourceTopic();
        long timeoutMillis = properties.kafka().adminTimeout().toMillis();
        List<TopicPartition> partitions = admin.describeTopics(List.of(topic))
                .allTopicNames()
                .get(timeoutMillis, TimeUnit.MILLISECONDS)
                .get(topic)
                .partitions()
                .stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .sorted((left, right) -> Integer.compare(left.partition(), right.partition()))
                .toList();
        if (partitions.isEmpty()) {
            return new KafkaDaySnapshot(topic, date, List.of());
        }

        long startTimestamp = date.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long endTimestamp = date.plusDays(1).atStartOfDay(ZoneOffset.UTC)
                .toInstant()
                .toEpochMilli();
        Map<TopicPartition, Long> latest = offsets(partitions, OffsetSpec.latest(), timeoutMillis);
        Map<TopicPartition, Long> starts = timestampOffsets(
                partitions, startTimestamp, latest, timeoutMillis);
        Map<TopicPartition, Long> ends = timestampOffsets(
                partitions, endTimestamp, latest, timeoutMillis);

        List<KafkaPartitionRange> ranges = partitions.stream()
                .map(partition -> {
                    long start = Math.min(starts.get(partition), latest.get(partition));
                    long end = Math.min(ends.get(partition), latest.get(partition));
                    return new KafkaPartitionRange(
                            partition.partition(), start, Math.max(start, end));
                })
                .toList();
        return new KafkaDaySnapshot(topic, date, ranges);
    }

    private Map<TopicPartition, Long> timestampOffsets(
            List<TopicPartition> partitions,
            long timestamp,
            Map<TopicPartition, Long> fallback,
            long timeoutMillis) throws Exception {
        Map<TopicPartition, OffsetSpec> requested = new HashMap<>();
        partitions.forEach(partition -> requested.put(partition, OffsetSpec.forTimestamp(timestamp)));
        Map<TopicPartition, ListOffsetsResultInfo> results = admin.listOffsets(requested)
                .all()
                .get(timeoutMillis, TimeUnit.MILLISECONDS);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        partitions.forEach(partition -> {
            long resolved = results.get(partition).offset();
            offsets.put(partition, resolved < 0 ? fallback.get(partition) : resolved);
        });
        return offsets;
    }

    private Map<TopicPartition, Long> offsets(
            List<TopicPartition> partitions,
            OffsetSpec specification,
            long timeoutMillis) throws Exception {
        Map<TopicPartition, OffsetSpec> requested = new HashMap<>();
        partitions.forEach(partition -> requested.put(partition, specification));
        Map<TopicPartition, ListOffsetsResultInfo> results = admin.listOffsets(requested)
                .all()
                .get(timeoutMillis, TimeUnit.MILLISECONDS);
        Map<TopicPartition, Long> offsets = new HashMap<>();
        partitions.forEach(partition -> offsets.put(partition, results.get(partition).offset()));
        return offsets;
    }
}
