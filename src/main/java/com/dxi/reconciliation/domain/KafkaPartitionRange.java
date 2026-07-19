package com.dxi.reconciliation.domain;

/** Immutable Kafka offset range used as reconciliation evidence for one partition. */
public record KafkaPartitionRange(int partition, long startOffset, long endOffset) {

    /** Validates that the range is non-negative and ordered. */
    public KafkaPartitionRange {
        if (partition < 0 || startOffset < 0 || endOffset < startOffset) {
            throw new IllegalArgumentException("Invalid Kafka partition offset range");
        }
    }

    /** Returns the number of records represented by the half-open offset range. */
    public long count() {
        return endOffset - startOffset;
    }
}
