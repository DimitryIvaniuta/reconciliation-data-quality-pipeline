package com.dxi.reconciliation.adapter.kafka;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.BusinessEvent;
import com.dxi.reconciliation.port.EventPublisher;
import com.dxi.reconciliation.service.JsonCodec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Mono;

/** Kafka implementation of the public business-event publisher. */
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, String> template;
    private final JsonCodec jsonCodec;
    private final AppProperties properties;

    /** Creates the event publisher. */
    public KafkaEventPublisher(
            KafkaTemplate<String, String> template,
            JsonCodec jsonCodec,
            AppProperties properties) {
        this.template = template;
        this.jsonCodec = jsonCodec;
        this.properties = properties;
    }

    /** Publishes an event and lets the broker assign the authoritative ingestion timestamp. */
    @Override
    public Mono<Void> publish(BusinessEvent event) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                properties.kafka().sourceTopic(),
                event.businessKey(),
                jsonCodec.write(event));
        return Mono.fromFuture(template.send(record)).then();
    }
}
