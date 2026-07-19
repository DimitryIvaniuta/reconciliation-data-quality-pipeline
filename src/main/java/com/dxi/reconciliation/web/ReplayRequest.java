package com.dxi.reconciliation.web;

import jakarta.validation.constraints.NotNull;
import java.time.LocalDate;

/** Validated replay/backfill request body. */
public record ReplayRequest(
        @NotNull LocalDate fromDate,
        @NotNull LocalDate toDate,
        boolean dryRun) { }
