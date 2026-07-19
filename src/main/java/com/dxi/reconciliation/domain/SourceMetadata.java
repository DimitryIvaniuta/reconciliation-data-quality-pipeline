package com.dxi.reconciliation.domain;

import java.time.Instant;

/** Original Kafka location retained for lineage and idempotency auditing. */
public record SourceMetadata(String topic, int partition, long offset, Instant recordTimestamp) { }
