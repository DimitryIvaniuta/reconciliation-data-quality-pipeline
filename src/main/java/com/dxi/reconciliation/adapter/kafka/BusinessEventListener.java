package com.dxi.reconciliation.adapter.kafka;

import com.dxi.reconciliation.config.AppProperties;
import com.dxi.reconciliation.domain.SourceMetadata;
import com.dxi.reconciliation.service.BusinessEventProcessor;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

/** Applies source-topic records to the idempotent reactive projection. */
public class BusinessEventListener {

    private final BusinessEventProcessor processor;
    private final AppProperties properties;

    /** Creates the source event listener. */
    public BusinessEventListener(BusinessEventProcessor processor, AppProperties properties) {
        this.processor = processor;
        this.properties = properties;
    }

    /** Processes one Kafka record and returns only after the database transaction completes. */
    @KafkaListener(topics = "${app.kafka.source-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(ConsumerRecord<String, String> record) {
        SourceMetadata source = new SourceMetadata(
                record.topic(),
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()));
        processor.process(record.value(), source).block(properties.kafka().adminTimeout());
    }
}
