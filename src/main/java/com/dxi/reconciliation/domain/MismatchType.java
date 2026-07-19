package com.dxi.reconciliation.domain;

/** Supported independently meaningful reconciliation comparisons. */
public enum MismatchType {
    KAFKA_VS_SOURCE_OBSERVATIONS,
    SOURCE_OBSERVATIONS_VS_UNIQUE_EVENTS,
    UNIQUE_EVENTS_VS_DATABASE,
    DATABASE_VS_AGGREGATE_COUNT,
    DATABASE_VS_AGGREGATE_AMOUNT
}
