package com.dxi.reconciliation.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declares all required Kafka topics as code. */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicConfiguration {

    /** Declares the source event topic with broker-authoritative ingestion timestamps. */
    @Bean
    public NewTopic sourceTopic(AppProperties properties) {
        return TopicBuilder.name(properties.kafka().sourceTopic())
                .partitions(properties.kafka().partitions())
                .replicas(properties.kafka().replicationFactor())
                .config(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
                .config(TopicConfig.RETENTION_MS_CONFIG,
                        Long.toString(properties.kafka().sourceRetention().toMillis()))
                .build();
    }

    /** Declares the durable replay-command topic. */
    @Bean
    public NewTopic replayCommandTopic(AppProperties properties) {
        return topic(properties.kafka().replayCommandTopic(), properties);
    }

    /** Declares the mismatch alert topic. */
    @Bean
    public NewTopic alertTopic(AppProperties properties) {
        return topic(properties.kafka().alertTopic(), properties);
    }

    /** Declares the dead-letter topic. */
    @Bean
    public NewTopic deadLetterTopic(AppProperties properties) {
        return topic(properties.kafka().deadLetterTopic(), properties);
    }

    private NewTopic topic(String name, AppProperties properties) {
        return TopicBuilder.name(name)
                .partitions(properties.kafka().partitions())
                .replicas(properties.kafka().replicationFactor())
                .build();
    }
}
