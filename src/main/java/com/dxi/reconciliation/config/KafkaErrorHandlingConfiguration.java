package com.dxi.reconciliation.config;

import com.dxi.reconciliation.service.ConflictException;
import com.dxi.reconciliation.service.InvalidEventException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/** Configures bounded retries and dead-letter publication for listener failures. */
@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfiguration {

    /** Creates the shared record error handler. */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> template,
            AppProperties properties) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, exception) -> new TopicPartition(
                        properties.kafka().deadLetterTopic(), record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer, new FixedBackOff(1_000L, 3L));
        handler.addNotRetryableExceptions(InvalidEventException.class, ConflictException.class);
        return handler;
    }
}
