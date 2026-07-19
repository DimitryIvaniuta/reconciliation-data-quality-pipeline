package com.dxi.reconciliation.service;

/** Indicates that an event failed structural or semantic validation. */
public class InvalidEventException extends RuntimeException {

    /** Creates a validation exception. */
    public InvalidEventException(String message) {
        super(message);
    }

    /** Creates a validation exception with its original cause. */
    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
