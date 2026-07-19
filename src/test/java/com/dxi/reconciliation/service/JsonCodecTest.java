package com.dxi.reconciliation.service;

import static com.dxi.reconciliation.support.TestFixtures.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dxi.reconciliation.domain.BusinessEvent;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class JsonCodecTest {

    private final JsonCodec codec = new JsonCodec(JsonMapper.builder()
            .findAndAddModules()
            .build());

    @Test
    void roundTripsBusinessEvent() {
        String json = codec.write(event());
        BusinessEvent result = codec.read(json, BusinessEvent.class);
        assertThat(result).isEqualTo(event());
    }

    @Test
    void wrapsMalformedJsonAsInvalidEvent() {
        assertThatThrownBy(() -> codec.read("{broken", BusinessEvent.class))
                .isInstanceOf(InvalidEventException.class);
    }
}
