package com.dxi.reconciliation.domain;

/** Durable replay job lifecycle. */
public enum ReplayStatus {
    REQUESTED,
    RUNNING,
    COMPLETED,
    FAILED
}
