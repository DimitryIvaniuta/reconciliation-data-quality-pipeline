package com.dxi.reconciliation.domain;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

/** Auditable Kafka partition-offset snapshot for one UTC ingestion day. */
public record KafkaDaySnapshot(
        String topic,
        LocalDate ingestionDate,
        List<KafkaPartitionRange> partitions) {

    /** Validates identity and defensively copies partition evidence. */
    public KafkaDaySnapshot {
        Objects.requireNonNull(topic, "topic must not be null");
        Objects.requireNonNull(ingestionDate, "ingestionDate must not be null");
        Objects.requireNonNull(partitions, "partitions must not be null");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        partitions = List.copyOf(partitions);
    }

    /** Returns the total number of source records in all partition ranges. */
    public long totalCount() {
        return partitions.stream().mapToLong(KafkaPartitionRange::count).sum();
    }
}
