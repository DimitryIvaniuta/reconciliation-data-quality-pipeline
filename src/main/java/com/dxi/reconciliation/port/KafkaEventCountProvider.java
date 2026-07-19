package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.KafkaDaySnapshot;
import java.time.LocalDate;
import reactor.core.publisher.Mono;

/** Resolves a low-cost Kafka partition-offset snapshot for a UTC ingestion day. */
public interface KafkaEventCountProvider {

    /** Returns immutable per-partition start and end offsets for the day. */
    Mono<KafkaDaySnapshot> snapshot(LocalDate businessDate);
}
