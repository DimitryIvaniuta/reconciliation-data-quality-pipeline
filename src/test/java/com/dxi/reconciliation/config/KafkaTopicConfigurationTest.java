package com.dxi.reconciliation.config;

import static com.dxi.reconciliation.support.TestFixtures.properties;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

class KafkaTopicConfigurationTest {

    @Test
    void sourceTopicUsesBrokerTimestampAndBoundedReplayRetention() {
        NewTopic topic = new KafkaTopicConfiguration().sourceTopic(properties());

        assertThat(topic.name()).isEqualTo("events");
        assertThat(topic.configs())
                .containsEntry(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime")
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, "3888000000");
    }
}
