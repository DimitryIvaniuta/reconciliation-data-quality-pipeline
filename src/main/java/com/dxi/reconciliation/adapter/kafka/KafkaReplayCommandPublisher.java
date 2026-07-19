package com.dxi.reconciliation.adapter.kafka;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.ReplayCommand;
import com.dxi.reconciliation.port.ReplayCommandPublisher;
import com.dxi.reconciliation.service.JsonCodec;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Mono;

/** Kafka replay command publisher. */
public class KafkaReplayCommandPublisher implements ReplayCommandPublisher {

    private final KafkaTemplate<String, String> template;
    private final JsonCodec jsonCodec;
    private final AppProperties properties;

    /** Creates the command publisher. */
    public KafkaReplayCommandPublisher(
            KafkaTemplate<String, String> template,
            JsonCodec jsonCodec,
            AppProperties properties) {
        this.template = template;
        this.jsonCodec = jsonCodec;
        this.properties = properties;
    }

    /** Publishes one job wake-up command keyed by job identifier. */
    @Override
    public Mono<Void> publish(ReplayCommand command) {
        return Mono.fromFuture(template.send(
                        properties.kafka().replayCommandTopic(),
                        command.jobId().toString(),
                        jsonCodec.write(command)))
                .then();
    }
}
