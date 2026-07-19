package com.dxi.reconciliation.adapter.redis;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dxi.reconciliation.service.ConflictException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class RedisDistributedLockServiceTest {

    @Mock private ReactiveStringRedisTemplate redis;
    @Mock private ReactiveValueOperations<String, String> values;
    private RedisDistributedLockService service;

    @BeforeEach
    void setUp() {
        when(redis.opsForValue()).thenReturn(values);
        service = new RedisDistributedLockService(redis);
    }

    @Test
    void releasesOwnedLockWhenActionCompletesEmpty() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenReturn(Mono.just(true));
        when(redis.compareAndDelete(anyString(), anyString())).thenReturn(Mono.just(true));

        StepVerifier.create(service.withLock("daily", Duration.ofMinutes(5), Mono.empty()))
                .verifyComplete();

        verify(redis).compareAndDelete(anyString(), anyString());
    }

    @Test
    void rejectsLockConflictWithoutDeletingAnotherOwnersToken() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenReturn(Mono.just(false));

        StepVerifier.create(service.withLock("daily", Duration.ofMinutes(5), Mono.just("result")))
                .expectError(ConflictException.class)
                .verify();

        verify(redis, never()).compareAndDelete(anyString(), anyString());
    }
}
