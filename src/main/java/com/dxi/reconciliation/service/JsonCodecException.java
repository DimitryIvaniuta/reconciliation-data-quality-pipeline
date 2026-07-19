package com.dxi.reconciliation.service;

/** Indicates that an internal domain object could not be serialized or parsed. */
public class JsonCodecException extends RuntimeException {

    /** Creates a codec exception with its original cause. */
    public JsonCodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
