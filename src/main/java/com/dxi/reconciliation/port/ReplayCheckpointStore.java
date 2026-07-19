package com.dxi.reconciliation.port;

import com.dxi.reconciliation.domain.ReplayPartitionCheckpoint;
import java.util.UUID;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Stores resumable replay progress independently for every Kafka partition. */
public interface ReplayCheckpointStore {

    /** Returns checkpoints in deterministic partition order. */
    Flux<ReplayPartitionCheckpoint> findByJobId(UUID jobId);

    /** Creates a checkpoint unless the same partition checkpoint already exists. */
    Mono<ReplayPartitionCheckpoint> createIfAbsent(ReplayPartitionCheckpoint checkpoint);

    /** Persists a checkpoint's offset, counters, status, and heartbeat timestamp. */
    Mono<ReplayPartitionCheckpoint> update(ReplayPartitionCheckpoint checkpoint);
}
