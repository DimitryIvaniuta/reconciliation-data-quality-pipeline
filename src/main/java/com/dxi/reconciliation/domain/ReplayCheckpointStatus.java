package com.dxi.reconciliation.domain;

/** Lifecycle of one resumable replay partition range. */
public enum ReplayCheckpointStatus {
    PENDING,
    RUNNING,
    COMPLETED
}
