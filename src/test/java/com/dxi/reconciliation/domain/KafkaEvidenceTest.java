package com.dxi.reconciliation.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class KafkaEvidenceTest {

    @Test
    void totalsHalfOpenRangesAndDefensivelyCopiesEvidence() {
        List<KafkaPartitionRange> ranges = new ArrayList<>(List.of(
                new KafkaPartitionRange(0, 10, 13),
                new KafkaPartitionRange(1, 4, 9)));

        KafkaDaySnapshot snapshot = new KafkaDaySnapshot(
                "events", LocalDate.parse("2026-07-18"), ranges);
        ranges.clear();

        assertThat(snapshot.totalCount()).isEqualTo(8);
        assertThat(snapshot.partitions()).hasSize(2);
        assertThatThrownBy(() -> snapshot.partitions().add(
                new KafkaPartitionRange(2, 0, 1)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsInvalidOffsetEvidence() {
        assertThatThrownBy(() -> new KafkaPartitionRange(0, 5, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid Kafka partition offset range");
    }

    @Test
    void rejectsInvalidReplayCheckpointProgress() {
        assertThatThrownBy(() -> new ReplayPartitionCheckpoint(
                java.util.UUID.randomUUID(), "events", 0, 10, 20, 15,
                2, 3, ReplayCheckpointStatus.RUNNING,
                java.time.Instant.parse("2026-07-18T10:00:00Z")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("counters");

        assertThatThrownBy(() -> new ReplayPartitionCheckpoint(
                java.util.UUID.randomUUID(), "events", 0, 10, 20, 19,
                9, 9, ReplayCheckpointStatus.COMPLETED,
                java.time.Instant.parse("2026-07-18T10:00:00Z")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot offset");
    }

}
