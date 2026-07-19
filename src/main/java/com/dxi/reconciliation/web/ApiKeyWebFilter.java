package com.dxi.reconciliation.web;

import com.dxi.reconciliation.config.AppProperties;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/** Protects business APIs with a constant-time compared deployment secret. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class ApiKeyWebFilter implements WebFilter {

    private static final String API_KEY_HEADER = "X-API-Key";
    private final byte[] expected;

    /** Creates the API key filter from validated configuration. */
    public ApiKeyWebFilter(AppProperties properties) {
        this.expected = properties.security().apiKey().getBytes(StandardCharsets.UTF_8);
    }

    /** Rejects unauthenticated `/api/**` requests without exposing secret details. */
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!exchange.getRequest().getPath().value().startsWith("/api/")) {
            return chain.filter(exchange);
        }
        String supplied = exchange.getRequest().getHeaders().getFirst(API_KEY_HEADER);
        byte[] provided = supplied == null
                ? new byte[0]
                : supplied.getBytes(StandardCharsets.UTF_8);
        if (MessageDigest.isEqual(expected, provided)) {
            return chain.filter(exchange);
        }
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        byte[] body = ("{\"type\":\"urn:problem:unauthorized\","
                + "\"title\":\"Unauthorized\",\"status\":401,"
                + "\"detail\":\"Missing or invalid API key\"}")
                .getBytes(StandardCharsets.UTF_8);
        return exchange.getResponse().writeWith(Mono.just(
                exchange.getResponse().bufferFactory().wrap(body)));
    }
}
