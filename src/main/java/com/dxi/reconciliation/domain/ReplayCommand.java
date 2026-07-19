package com.dxi.reconciliation.domain;

import java.util.UUID;

/** Lightweight Kafka command used to wake a replay worker. */
public record ReplayCommand(UUID jobId) { }
