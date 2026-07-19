package com.dxi.reconciliation.service;

import com.dxi.reconciliation.domain.BusinessEvent;
import com.dxi.reconciliation.domain.ProjectionResult;
import com.dxi.reconciliation.domain.SourceMetadata;
import com.dxi.reconciliation.port.EventProjectionStore;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;

/** Validates and applies business events through the idempotent projection port. */
public class BusinessEventProcessor {

    private static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(5);

    private final Validator validator;
    private final EventProjectionStore projectionStore;
    private final JsonCodec jsonCodec;
    private final Clock clock;

    /** Creates the event processor. */
    public BusinessEventProcessor(
            Validator validator,
            EventProjectionStore projectionStore,
            JsonCodec jsonCodec,
            Clock clock) {
        this.validator = validator;
        this.projectionStore = projectionStore;
        this.jsonCodec = jsonCodec;
        this.clock = clock;
    }

    /** Parses, validates, and projects a raw Kafka JSON event. */
    public Mono<ProjectionResult> process(String payload, SourceMetadata source) {
        return Mono.fromCallable(() -> jsonCodec.read(payload, BusinessEvent.class))
                .flatMap(event -> process(event, source));
    }

    /** Validates and projects an already parsed event. */
    public Mono<ProjectionResult> process(BusinessEvent event, SourceMetadata source) {
        return Mono.defer(() -> {
            validate(event, source);
            return projectionStore.project(event, source, jsonCodec.write(event));
        });
    }

    private void validate(BusinessEvent event, SourceMetadata source) {
        Set<ConstraintViolation<BusinessEvent>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String details = violations.stream()
                    .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new InvalidEventException("Invalid business event: " + details);
        }
        Instant latestAllowed = clock.instant().plus(MAX_FUTURE_SKEW);
        if (event.eventTime().isAfter(latestAllowed)) {
            throw new InvalidEventException("eventTime is more than five minutes in the future");
        }
        if (source.partition() < 0 || source.offset() < 0 || source.topic() == null
                || source.topic().isBlank()) {
            throw new InvalidEventException("Invalid Kafka source metadata");
        }
    }
}
