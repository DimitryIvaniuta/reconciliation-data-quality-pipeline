package com.dxi.reconciliation.service;

/** Indicates that a requested report or replay job does not exist. */
public class ResourceNotFoundException extends RuntimeException {

    /** Creates a not-found exception. */
    public ResourceNotFoundException(String message) {
        super(message);
    }
}
