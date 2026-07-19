package com.dxi.reconciliation.web;

import com.dxi.reconciliation.domain.BusinessEvent;
import com.dxi.reconciliation.port.EventPublisher;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/** HTTP ingestion facade that publishes validated events to the source topic. */
@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventPublisher eventPublisher;

    /** Creates the event controller. */
    public EventController(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    /** Accepts one structurally valid event for asynchronous Kafka processing. */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<BusinessEvent> publish(@Valid @RequestBody BusinessEvent event) {
        return eventPublisher.publish(event).thenReturn(event);
    }
}
