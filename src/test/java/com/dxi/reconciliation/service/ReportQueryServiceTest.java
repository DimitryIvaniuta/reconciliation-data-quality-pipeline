package com.dxi.reconciliation.service;

import static org.mockito.Mockito.mock;

import com.dxi.reconciliation.port.ReportStore;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class ReportQueryServiceTest {

    @Test
    void rejectsUnsafePagination() {
        ReportQueryService service = new ReportQueryService(mock(ReportStore.class));
        StepVerifier.create(service.find(null, null, null, 0, 201))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void rejectsReversedDateRange() {
        ReportQueryService service = new ReportQueryService(mock(ReportStore.class));
        StepVerifier.create(service.find(
                        LocalDate.parse("2026-07-18"),
                        LocalDate.parse("2026-07-17"),
                        null,
                        0,
                        10))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void mapsMissingReportToDomainError() {
        ReportStore store = mock(ReportStore.class);
        UUID id = UUID.randomUUID();
        org.mockito.Mockito.when(store.findById(id)).thenReturn(Mono.empty());
        ReportQueryService service = new ReportQueryService(store);
        StepVerifier.create(service.get(id))
                .expectError(ResourceNotFoundException.class)
                .verify();
    }
}
