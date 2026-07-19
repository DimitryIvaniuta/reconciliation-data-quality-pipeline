package com.dxi.reconciliation.service;

/** Indicates a request or event conflicts with already persisted immutable state. */
public class ConflictException extends RuntimeException {

    /** Creates a conflict exception. */
    public ConflictException(String message) {
        super(message);
    }
}
