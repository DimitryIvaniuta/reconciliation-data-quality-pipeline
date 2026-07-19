package com.dxi.reconciliation.domain;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Canonical immutable business event consumed by the projection pipeline. */
public record BusinessEvent(
        @NotNull UUID eventId,
        @NotBlank String businessKey,
        @NotNull Instant eventTime,
        @NotNull @DecimalMin("0.00") BigDecimal amount,
        @NotNull Map<String, Object> attributes) { }
