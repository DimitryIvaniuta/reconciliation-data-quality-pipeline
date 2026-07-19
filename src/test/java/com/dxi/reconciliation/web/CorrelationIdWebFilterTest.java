package com.dxi.reconciliation.web;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class CorrelationIdWebFilterTest {

    private final CorrelationIdWebFilter filter = new CorrelationIdWebFilter();

    @Test
    void preservesValidCallerCorrelationId() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/events")
                        .header(RequestContext.CORRELATION_HEADER, "caller-correlation")
                        .build());
        WebFilterChain chain = ignored -> Mono.deferContextual(context -> {
            assertThat(RequestContext.correlationId(context)).isEqualTo("caller-correlation");
            return Mono.empty();
        });

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();
        assertThat(RequestContext.correlationId(exchange)).isEqualTo("caller-correlation");
        assertThat(exchange.getResponse().getHeaders().getFirst(RequestContext.CORRELATION_HEADER))
                .isEqualTo("caller-correlation");
    }

    @Test
    void replacesOversizedCorrelationId() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/events")
                        .header(RequestContext.CORRELATION_HEADER, "x".repeat(129))
                        .build());

        StepVerifier.create(filter.filter(exchange, ignored -> Mono.empty())).verifyComplete();
        assertThat(RequestContext.correlationId(exchange)).hasSize(36);
    }
}
