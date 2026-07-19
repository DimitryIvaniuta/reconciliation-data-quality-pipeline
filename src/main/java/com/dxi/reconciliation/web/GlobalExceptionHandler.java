package com.dxi.reconciliation.web;

import com.dxi.reconciliation.service.ConflictException;
import com.dxi.reconciliation.service.InvalidEventException;
import com.dxi.reconciliation.service.ResourceNotFoundException;
import jakarta.validation.ConstraintViolationException;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;

/** Converts domain and validation failures into RFC 9457 problem details. */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /** Maps malformed requests and validation failures to HTTP 400. */
    @ExceptionHandler({
        IllegalArgumentException.class,
        InvalidEventException.class,
        ConstraintViolationException.class,
        WebExchangeBindException.class
    })
    public ProblemDetail badRequest(RuntimeException exception, ServerWebExchange exchange) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", exception, exchange);
    }

    /** Maps immutable state and lock conflicts to HTTP 409. */
    @ExceptionHandler(ConflictException.class)
    public ProblemDetail conflict(ConflictException exception, ServerWebExchange exchange) {
        return problem(HttpStatus.CONFLICT, "Conflict", exception, exchange);
    }

    /** Maps missing reports and jobs to HTTP 404. */
    @ExceptionHandler(ResourceNotFoundException.class)
    public ProblemDetail notFound(ResourceNotFoundException exception, ServerWebExchange exchange) {
        return problem(HttpStatus.NOT_FOUND, "Resource not found", exception, exchange);
    }

    /** Maps unexpected failures without leaking implementation details. */
    @ExceptionHandler(Throwable.class)
    public ProblemDetail unexpected(Throwable exception, ServerWebExchange exchange) {
        log.error("Unhandled request failure correlationId={}",
                RequestContext.correlationId(exchange), exception);
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred");
        detail.setTitle("Internal server error");
        detail.setType(URI.create("urn:problem:internal-server-error"));
        detail.setProperty("correlationId", RequestContext.correlationId(exchange));
        return detail;
    }

    private ProblemDetail problem(
            HttpStatus status,
            String title,
            Throwable exception,
            ServerWebExchange exchange) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(status, safeMessage(exception));
        detail.setTitle(title);
        detail.setType(URI.create("urn:problem:" + title.toLowerCase().replace(' ', '-')));
        detail.setProperty("correlationId", RequestContext.correlationId(exchange));
        return detail;
    }

    private String safeMessage(Throwable exception) {
        return exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
    }
}
