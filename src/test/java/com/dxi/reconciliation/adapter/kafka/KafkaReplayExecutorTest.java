package com.dxi.reconciliation.adapter.kafka;

import static com.dxi.reconciliation.support.TestFixtures.properties;
import static com.dxi.reconciliation.support.TestFixtures.requestedJob;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dxi.reconciliation.domain.ProjectionResult;
import com.dxi.reconciliation.domain.ReplayCheckpointStatus;
import com.dxi.reconciliation.domain.ReplayPartitionCheckpoint;
import com.dxi.reconciliation.port.ReplayCheckpointStore;
import com.dxi.reconciliation.port.ReplayJobStore;
import com.dxi.reconciliation.service.BusinessEventProcessor;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class KafkaReplayExecutorTest {

    @Test
    void checkpointsBoundedPartitionAndProcessesEveryRecord() {
        @SuppressWarnings("unchecked")
        Consumer<String, String> consumer = mock(Consumer.class);
        BusinessEventProcessor processor = mock(BusinessEventProcessor.class);
        ReplayCheckpointStore checkpoints = mock(ReplayCheckpointStore.class);
        ReplayJobStore jobs = mock(ReplayJobStore.class);
        TopicPartition partition = new TopicPartition("events", 0);
        PartitionInfo partitionInfo = new PartitionInfo("events", 0, null, new Node[0], new Node[0]);

        when(checkpoints.findByJobId(any())).thenReturn(Flux.empty());
        when(checkpoints.createIfAbsent(any()))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(checkpoints.update(any()))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(jobs.updateProgress(any(), anyInt(), anyLong(), anyLong(), any()))
                .thenReturn(Mono.empty());
        when(consumer.partitionsFor("events", properties().kafka().adminTimeout()))
                .thenReturn(List.of(partitionInfo));
        when(consumer.endOffsets(List.of(partition), properties().kafka().adminTimeout()))
                .thenReturn(Map.of(partition, 20L));
        when(consumer.offsetsForTimes(any(), any()))
                .thenReturn(Map.of(partition, new OffsetAndTimestamp(2, 1L)))
                .thenReturn(Map.of(partition, new OffsetAndTimestamp(5, 2L)));
        when(consumer.position(partition, properties().kafka().adminTimeout()))
                .thenReturn(2L, 5L);

        String payload = "{\"eventId\":\"11111111-1111-1111-1111-111111111111\","
                + "\"businessKey\":\"ORDER-1001\","
                + "\"eventTime\":\"2026-07-17T12:00:00Z\","
                + "\"amount\":125.50,\"attributes\":{}}";
        List<ConsumerRecord<String, String>> records = List.of(
                record(2, payload), record(3, payload), record(4, payload));
        when(consumer.poll(properties().kafka().replayPollTimeout()))
                .thenReturn(new ConsumerRecords<>(Map.of(partition, records)));
        when(processor.process(any(), any())).thenReturn(Mono.just(new ProjectionResult(
                UUID.fromString("11111111-1111-1111-1111-111111111111"),
                true, true, true)));

        KafkaReplayExecutor executor = new KafkaReplayExecutor(
                consumer,
                processor,
                checkpoints,
                jobs,
                properties(),
                Clock.fixed(Instant.parse("2026-07-18T10:00:00Z"), ZoneOffset.UTC));

        StepVerifier.create(executor.execute(requestedJob(false)))
                .assertNext(result -> {
                    assertThat(result.discoveredEvents()).isEqualTo(3);
                    assertThat(result.replayedEvents()).isEqualTo(3);
                })
                .verifyComplete();

        ArgumentCaptor<ReplayPartitionCheckpoint> captor =
                ArgumentCaptor.forClass(ReplayPartitionCheckpoint.class);
        verify(checkpoints, org.mockito.Mockito.atLeast(2)).update(captor.capture());
        ReplayPartitionCheckpoint completed = captor.getAllValues().getLast();
        assertThat(completed.status()).isEqualTo(ReplayCheckpointStatus.COMPLETED);
        assertThat(completed.nextOffset()).isEqualTo(5);
        assertThat(completed.discoveredEvents()).isEqualTo(3);
    }

    private ConsumerRecord<String, String> record(long offset, String payload) {
        return new ConsumerRecord<>(
                "events", 0, offset, 1_000L + offset,
                org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME,
                0, 0, null, payload, new org.apache.kafka.common.header.internals.RecordHeaders(),
                java.util.Optional.empty());
    }
}
