package com.dxi.reconciliation.web;

import static com.dxi.reconciliation.support.TestFixtures.properties;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class ApiKeyWebFilterTest {

    @Test
    void rejectsProtectedApiWithoutKey() {
        ApiKeyWebFilter filter = new ApiKeyWebFilter(properties());
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/reconciliations").build());
        WebFilterChain chain = ignored -> Mono.error(new AssertionError("chain must not run"));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        String body = exchange.getResponse().getBodyAsString().block();
        assertThat(body).contains("Missing or invalid API key");
    }

    @Test
    void allowsCorrectKey() {
        ApiKeyWebFilter filter = new ApiKeyWebFilter(properties());
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/reconciliations")
                        .header("X-API-Key", "test-api-key")
                        .build());
        WebFilterChain chain = ignored -> Mono.empty();

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    void doesNotProtectActuatorHealth() {
        ApiKeyWebFilter filter = new ApiKeyWebFilter(properties());
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/actuator/health").build());
        WebFilterChain chain = ignored -> Mono.empty();

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();
        assertThat(exchange.getResponse().getBodyAsString().block()).isNullOrEmpty();
    }
}
