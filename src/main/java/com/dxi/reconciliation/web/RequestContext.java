package com.dxi.reconciliation.web;

import org.springframework.web.server.ServerWebExchange;
import reactor.util.context.ContextView;

/** Shared correlation identifier keys for HTTP exchange and Reactor context propagation. */
public final class RequestContext {

    /** Standard correlation header accepted and returned by the API. */
    public static final String CORRELATION_HEADER = "X-Correlation-ID";

    /** Exchange attribute and Reactor context key. */
    public static final String CORRELATION_KEY = "correlationId";

    private RequestContext() {
    }

    /** Returns the correlation identifier attached by the HTTP filter. */
    public static String correlationId(ServerWebExchange exchange) {
        Object value = exchange.getAttribute(CORRELATION_KEY);
        return value == null ? "unknown" : value.toString();
    }

    /** Returns the correlation identifier from a Reactor context when present. */
    public static String correlationId(ContextView contextView) {
        return contextView.getOrDefault(CORRELATION_KEY, "unknown");
    }
}
