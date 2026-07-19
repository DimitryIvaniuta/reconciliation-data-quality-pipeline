package com.dxi.reconciliation.port;

import java.time.Duration;
import reactor.core.publisher.Mono;

/** Executes work under a token-owned distributed lock. */
public interface DistributedLockService {

    /** Executes action only when the named lock is acquired. */
    <T> Mono<T> withLock(String lockName, Duration ttl, Mono<T> action);
}
