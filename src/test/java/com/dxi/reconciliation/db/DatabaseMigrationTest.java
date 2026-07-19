package com.dxi.reconciliation.db;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class DatabaseMigrationTest {

    @Test
    void migrationsContainAuditableLowLoadAndResumableDesign() throws IOException {
        String initial = migration("/db/migration/V1__create_reconciliation_schema.sql");
        String strengthened = migration("/db/migration/V2__strengthen_reconciliation_evidence.sql");

        assertThat(initial)
                .contains("daily_metrics")
                .contains("business_event_ledger_immutable")
                .contains("reconciliation_reports_immutable")
                .contains("business_records_metric_change")
                .contains("daily_aggregates_metric_change")
                .contains("idx_replay_jobs_dispatch");
        assertThat(strengthened)
                .contains("source_event_observations")
                .contains("unique_event_count")
                .contains("db_total_amount")
                .contains("source_offsets JSONB")
                .contains("replay_partition_checkpoints")
                .contains("heartbeat_at")
                .contains("data_mutation_audit")
                .contains("source_event_observations_immutable")
                .contains("attempt_count")
                .contains("uq_reconciliation_alert_report_channel")
                .contains("idx_reconciliation_alerts_recovery");
    }

    private String migration(String resource) throws IOException {
        try (var input = getClass().getResourceAsStream(resource)) {
            assertThat(input).isNotNull();
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
