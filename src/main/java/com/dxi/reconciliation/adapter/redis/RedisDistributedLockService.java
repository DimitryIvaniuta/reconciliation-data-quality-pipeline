package com.dxi.reconciliation.adapter.redis;

import com.dxi.reconciliation.port.DistributedLockService;
import com.dxi.reconciliation.service.ConflictException;
import java.time.Duration;
import java.util.UUID;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

/** Redis token-owned lock implementation with atomic compare-and-delete release. */
public class RedisDistributedLockService implements DistributedLockService {

    private final ReactiveStringRedisTemplate redis;

    /** Creates the Redis lock adapter. */
    public RedisDistributedLockService(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    /** Acquires a lock with a random owner token and releases only that token. */
    @Override
    public <T> Mono<T> withLock(String lockName, Duration ttl, Mono<T> action) {
        String key = "reconciliation:lock:" + lockName;
        String token = UUID.randomUUID().toString();
        Mono<String> acquisition = redis.opsForValue()
                .setIfAbsent(key, token, ttl)
                .flatMap(acquired -> Boolean.TRUE.equals(acquired)
                        ? Mono.just(token)
                        : Mono.error(new ConflictException(
                                "Another operation already owns lock: " + lockName)));
        return Mono.usingWhen(
                acquisition,
                ignored -> action,
                ignored -> release(key, token),
                (ignored, error) -> release(key, token),
                ignored -> release(key, token));
    }

    private Mono<Void> release(String key, String token) {
        return redis.compareAndDelete(key, token).then();
    }
}
