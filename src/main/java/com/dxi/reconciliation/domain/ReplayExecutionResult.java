package com.dxi.reconciliation.domain;

/** Counts returned by replay source scanning. */
public record ReplayExecutionResult(long discoveredEvents, long replayedEvents) { }
