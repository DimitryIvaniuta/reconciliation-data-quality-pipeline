package com.dxi.reconciliation.web;

import java.util.UUID;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/** Validates or creates a correlation ID and propagates it through the reactive context. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdWebFilter implements WebFilter {

    private static final int MAXIMUM_LENGTH = 128;

    /** Adds a safe correlation identifier to the request, response, and Reactor context. */
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String supplied = exchange.getRequest().getHeaders().getFirst(RequestContext.CORRELATION_HEADER);
        String correlationId = supplied == null || supplied.isBlank() || supplied.length() > MAXIMUM_LENGTH
                ? UUID.randomUUID().toString()
                : supplied;
        exchange.getAttributes().put(RequestContext.CORRELATION_KEY, correlationId);
        exchange.getResponse().getHeaders().set(RequestContext.CORRELATION_HEADER, correlationId);
        return chain.filter(exchange).contextWrite(context -> context.put(
                RequestContext.CORRELATION_KEY, correlationId));
    }
}
