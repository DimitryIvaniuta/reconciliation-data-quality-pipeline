package com.dxi.reconciliation.adapter.kafka;

import static com.dxi.reconciliation.support.TestFixtures.properties;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class KafkaOffsetEventCountProviderTest {

    @Test
    void sumsBoundaryOffsetDifferencesAcrossPartitions() {
        Admin admin = mock(Admin.class);
        DescribeTopicsResult topics = mock(DescribeTopicsResult.class);
        TopicDescription description = mock(TopicDescription.class);
        TopicPartitionInfo partitionZero = mock(TopicPartitionInfo.class);
        TopicPartitionInfo partitionOne = mock(TopicPartitionInfo.class);
        when(partitionZero.partition()).thenReturn(0);
        when(partitionOne.partition()).thenReturn(1);
        when(description.partitions()).thenReturn(List.of(partitionZero, partitionOne));
        when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of("events", description)));
        when(admin.describeTopics(anyList())).thenReturn(topics);

        TopicPartition zero = new TopicPartition("events", 0);
        TopicPartition one = new TopicPartition("events", 1);
        ListOffsetsResult latest = offsets(Map.of(zero, info(100), one, info(200)));
        ListOffsetsResult start = offsets(Map.of(zero, info(10), one, info(20)));
        ListOffsetsResult end = offsets(Map.of(zero, info(15), one, info(27)));
        when(admin.listOffsets(anyMap())).thenReturn(latest, start, end);

        KafkaOffsetEventCountProvider provider = new KafkaOffsetEventCountProvider(
                admin, properties());
        StepVerifier.create(provider.snapshot(LocalDate.parse("2026-07-17")))
                .assertNext(snapshot -> {
                    org.assertj.core.api.Assertions.assertThat(snapshot.totalCount()).isEqualTo(12L);
                    org.assertj.core.api.Assertions.assertThat(snapshot.partitions()).hasSize(2);
                })
                .verifyComplete();
    }

    private ListOffsetsResult offsets(Map<TopicPartition, ListOffsetsResultInfo> values) {
        ListOffsetsResult result = mock(ListOffsetsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(values));
        return result;
    }

    private ListOffsetsResultInfo info(long offset) {
        ListOffsetsResultInfo info = mock(ListOffsetsResultInfo.class);
        when(info.offset()).thenReturn(offset);
        return info;
    }
}
