package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.BusinessEvent;
import reactor.core.publisher.Mono;

/** Publishes externally accepted business events to Kafka. */
public interface EventPublisher {

    /** Publishes one event while the broker assigns the authoritative ingestion timestamp. */
    Mono<Void> publish(BusinessEvent event);
}
