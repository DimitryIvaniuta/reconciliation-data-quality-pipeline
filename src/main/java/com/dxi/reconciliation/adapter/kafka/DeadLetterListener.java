package com.dxi.reconciliation.adapter.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

/** Observes dead-letter records without logging sensitive payloads. */
@Slf4j
public class DeadLetterListener {

    /** Logs safe source coordinates for operational visibility. */
    @KafkaListener(
            topics = "${app.kafka.dead-letter-topic}",
            groupId = "reconciliation-dlt-observer")
    public void listen(ConsumerRecord<String, String> record) {
        log.error(
                "Dead-letter record observed at topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());
    }
}
