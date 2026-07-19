package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.ReplayCommand;
import reactor.core.publisher.Mono;

/** Publishes durable replay wake-up commands to Kafka. */
public interface ReplayCommandPublisher {

    /** Publishes one replay command. */
    Mono<Void> publish(ReplayCommand command);
}
