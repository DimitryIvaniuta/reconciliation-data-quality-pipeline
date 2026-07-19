package com.dxi.reconciliation.db;

import static com.dxi.reconciliation.support.TestFixtures.event;
import static org.assertj.core.api.Assertions.assertThat;

import com.dxi.reconciliation.adapter.db.R2dbcDailyAggregateRepairStore;
import com.dxi.reconciliation.adapter.db.R2dbcDailyMetricsStore;
import com.dxi.reconciliation.adapter.db.R2dbcEventProjectionStore;
import com.dxi.reconciliation.domain.SourceMetadata;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Instant;
import java.time.LocalDate;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.test.StepVerifier;

@Tag("integration")
class PostgreSqlProjectionIntegrationTest {

    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.4-alpine")
            .withDatabaseName("reconciliation")
            .withUsername("reconciliation")
            .withPassword("reconciliation");
    private static DatabaseClient client;
    private static R2dbcEventProjectionStore projectionStore;
    private static R2dbcDailyMetricsStore metricsStore;
    private static R2dbcDailyAggregateRepairStore repairStore;

    @BeforeAll
    static void startDatabase() {
        POSTGRES.start();
        Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .load()
                .migrate();
        String r2dbcUrl = "r2dbc:postgresql://" + POSTGRES.getHost() + ":"
                + POSTGRES.getMappedPort(5432) + "/" + POSTGRES.getDatabaseName()
                + "?user=" + POSTGRES.getUsername() + "&password=" + POSTGRES.getPassword();
        ConnectionFactory factory = ConnectionFactories.get(r2dbcUrl);
        client = DatabaseClient.create(factory);
        TransactionalOperator transactions = TransactionalOperator.create(
                new R2dbcTransactionManager(factory));
        projectionStore = new R2dbcEventProjectionStore(client, transactions);
        metricsStore = new R2dbcDailyMetricsStore(client);
        repairStore = new R2dbcDailyAggregateRepairStore(client, transactions);
    }

    @AfterAll
    static void stopDatabase() {
        POSTGRES.stop();
    }

    @Test
    void migrationProjectsIdempotentlyAndRepairsAggregateDrift() {
        StepVerifier.create(client.sql("""
                        TRUNCATE reconciliation_alerts, reconciliation_reports, replay_jobs,
                                 source_event_observations, business_records, business_event_ledger,
                                 daily_aggregates, daily_metrics, data_mutation_audit CASCADE
                        """)
                .fetch().rowsUpdated().then())
                .verifyComplete();

        SourceMetadata source = new SourceMetadata(
                "events", 0, 10, Instant.parse("2026-07-18T00:05:00Z"));
        String payload = "{\"eventId\":\"11111111-1111-1111-1111-111111111111\","
                + "\"businessKey\":\"ORDER-1001\","
                + "\"eventTime\":\"2026-07-17T12:00:00Z\","
                + "\"amount\":125.50,\"attributes\":{\"channel\":\"WEB\"}}";

        StepVerifier.create(projectionStore.project(event(), source, payload)
                        .then(projectionStore.project(event(), source, payload)))
                .assertNext(result -> {
                    assertThat(result.uniqueEventInserted()).isFalse();
                    assertThat(result.sourceObservationInserted()).isFalse();
                    assertThat(result.businessRowInserted()).isFalse();
                })
                .verifyComplete();

        LocalDate date = LocalDate.parse("2026-07-18");
        StepVerifier.create(metricsStore.find(LocalDate.parse("2026-07-17")))
                .assertNext(metrics -> assertThat(metrics.databaseRecordCount()).isZero())
                .verifyComplete();
        StepVerifier.create(metricsStore.find(date))
                .assertNext(metrics -> {
                    assertThat(metrics.consumedEventCount()).isEqualTo(1);
                    assertThat(metrics.uniqueEventCount()).isEqualTo(1);
                    assertThat(metrics.databaseRecordCount()).isEqualTo(1);
                    assertThat(metrics.aggregateRecordCount()).isEqualTo(1);
                    assertThat(metrics.databaseAmount()).isEqualByComparingTo("125.5000");
                })
                .verifyComplete();

        StepVerifier.create(client.sql("""
                        UPDATE daily_aggregates
                        SET record_count = 99, total_amount = 9999
                        WHERE business_date = :date
                        """)
                .bind("date", date)
                .fetch().rowsUpdated().then())
                .verifyComplete();
        StepVerifier.create(metricsStore.find(date))
                .assertNext(metrics -> assertThat(metrics.aggregateRecordCount()).isEqualTo(99))
                .verifyComplete();

        StepVerifier.create(repairStore.repair(date)).verifyComplete();
        StepVerifier.create(metricsStore.find(date))
                .assertNext(metrics -> {
                    assertThat(metrics.consumedEventCount()).isEqualTo(1);
                    assertThat(metrics.uniqueEventCount()).isEqualTo(1);
                    assertThat(metrics.databaseRecordCount()).isEqualTo(1);
                    assertThat(metrics.aggregateRecordCount()).isEqualTo(1);
                    assertThat(metrics.databaseAmount()).isEqualByComparingTo("125.5000");
                    assertThat(metrics.aggregateAmount()).isEqualByComparingTo("125.5000");
                })
                .verifyComplete();

        SourceMetadata duplicateSource = new SourceMetadata(
                "events", 0, 11, Instant.parse("2026-07-18T00:06:00Z"));
        StepVerifier.create(projectionStore.project(event(), duplicateSource, payload))
                .assertNext(result -> {
                    assertThat(result.sourceObservationInserted()).isTrue();
                    assertThat(result.uniqueEventInserted()).isFalse();
                    assertThat(result.businessRowInserted()).isFalse();
                })
                .verifyComplete();
        StepVerifier.create(metricsStore.find(date))
                .assertNext(metrics -> {
                    assertThat(metrics.consumedEventCount()).isEqualTo(2);
                    assertThat(metrics.uniqueEventCount()).isEqualTo(1);
                })
                .verifyComplete();
    }
}
