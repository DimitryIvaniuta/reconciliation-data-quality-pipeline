package com.dxi.reconciliation.service;

import static com.dxi.reconciliation.support.TestFixtures.NOW;
import static com.dxi.reconciliation.support.TestFixtures.event;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dxi.reconciliation.domain.BusinessEvent;
import com.dxi.reconciliation.domain.ProjectionResult;
import com.dxi.reconciliation.domain.SourceMetadata;
import com.dxi.reconciliation.port.EventProjectionStore;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ExtendWith(MockitoExtension.class)
class BusinessEventProcessorTest {

    @Mock private EventProjectionStore projectionStore;
    @Mock private JsonCodec jsonCodec;
    private BusinessEventProcessor processor;
    private SourceMetadata source;

    @BeforeEach
    void setUp() {
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        processor = new BusinessEventProcessor(
                validator,
                projectionStore,
                jsonCodec,
                Clock.fixed(NOW, ZoneOffset.UTC));
        source = new SourceMetadata("events", 0, 10, NOW);
    }

    @Test
    void validatesAndProjectsValidEvent() {
        BusinessEvent event = event();
        when(jsonCodec.write(event)).thenReturn("{}");
        when(projectionStore.project(event, source, "{}"))
                .thenReturn(Mono.just(new ProjectionResult(event.eventId(), true, true, true)));

        StepVerifier.create(processor.process(event, source))
                .expectNext(new ProjectionResult(event.eventId(), true, true, true))
                .verifyComplete();

        verify(projectionStore).project(event, source, "{}");
    }

    @Test
    void rejectsConstraintViolationsBeforeDatabaseAccess() {
        BusinessEvent invalid = new BusinessEvent(
                null, "", NOW, new BigDecimal("-1"), Map.of());

        StepVerifier.create(processor.process(invalid, source))
                .expectError(InvalidEventException.class)
                .verify();
    }

    @Test
    void rejectsEventsTooFarInFuture() {
        BusinessEvent future = new BusinessEvent(
                UUID.randomUUID(), "key", NOW.plusSeconds(301), BigDecimal.ONE, Map.of());

        StepVerifier.create(processor.process(future, source))
                .expectErrorMatches(error -> error instanceof InvalidEventException
                        && error.getMessage().contains("future"))
                .verify();
    }

    @Test
    void parsesRawPayloadBeforeProjection() {
        BusinessEvent event = event();
        when(jsonCodec.read(anyString(), any())).thenReturn(event);
        when(jsonCodec.write(event)).thenReturn("{}");
        when(projectionStore.project(any(), any(), anyString()))
                .thenReturn(Mono.just(new ProjectionResult(event.eventId(), true, true, true)));

        StepVerifier.create(processor.process("payload", source))
                .expectNextCount(1)
                .verifyComplete();
    }
}
