package com.dxi.reconciliation.domain;

/** One actionable count or monetary mismatch discovered by reconciliation. */
public record ReconciliationIssue(
        MismatchType type,
        String expected,
        String actual,
        String delta,
        String action) { }
