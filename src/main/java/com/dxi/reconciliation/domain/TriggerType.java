package com.dxi.reconciliation.domain;

/** Reason a reconciliation run was initiated. */
public enum TriggerType {
    SCHEDULED,
    MANUAL,
    REPLAY_VALIDATION
}
