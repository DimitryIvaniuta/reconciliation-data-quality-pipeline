package com.dxi.reconciliation.domain;

import java.util.UUID;

/** Outcome of applying one source occurrence to the idempotent database projection. */
public record ProjectionResult(
        UUID eventId,
        boolean sourceObservationInserted,
        boolean uniqueEventInserted,
        boolean businessRowInserted) { }
