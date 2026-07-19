package com.dxi.reconciliation.service;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** Central JSON serialization boundary used by persistence and messaging adapters. */
public class JsonCodec {

    private final ObjectMapper objectMapper;

    /** Creates the codec from the application-managed object mapper. */
    public JsonCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** Serializes a value to JSON. */
    public String write(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JacksonException exception) {
            throw new JsonCodecException("Unable to serialize JSON payload", exception);
        }
    }

    /** Parses JSON into the requested type. */
    public <T> T read(String payload, Class<T> targetType) {
        try {
            return objectMapper.readValue(payload, targetType);
        } catch (JacksonException exception) {
            throw new InvalidEventException("Unable to parse JSON payload", exception);
        }
    }
}
