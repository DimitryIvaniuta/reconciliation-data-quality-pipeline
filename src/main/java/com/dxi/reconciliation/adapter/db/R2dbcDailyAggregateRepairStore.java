package com.dxi.reconciliation.adapter.db;

import com.dxi.reconciliation.port.DailyAggregateRepairStore;
import java.time.LocalDate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

/** Explicit low-frequency exact repair for aggregate and compact metric drift. */
public class R2dbcDailyAggregateRepairStore implements DailyAggregateRepairStore {

    private final DatabaseClient client;
    private final TransactionalOperator transactions;

    /** Creates the repair adapter. */
    public R2dbcDailyAggregateRepairStore(
            DatabaseClient client,
            TransactionalOperator transactions) {
        this.client = client;
        this.transactions = transactions;
    }

    /** Rebuilds one day from indexed source observations, immutable events, and business rows. */
    @Override
    public Mono<Void> repair(LocalDate businessDate) {
        Mono<Void> repair = client.sql("""
                        WITH exact AS (
                            SELECT :businessDate::date AS business_date,
                                   COUNT(*) AS record_count,
                                   COALESCE(SUM(amount), 0) AS total_amount
                            FROM business_records
                            WHERE business_date = :businessDate
                        ), previous AS (
                            SELECT to_jsonb(aggregate_row) AS old_row
                            FROM daily_aggregates aggregate_row
                            WHERE business_date = :businessDate
                        ), repaired AS (
                            INSERT INTO daily_aggregates (
                                business_date, record_count, total_amount)
                            SELECT business_date, record_count, total_amount
                            FROM exact
                            ON CONFLICT (business_date) DO UPDATE
                            SET record_count = EXCLUDED.record_count,
                                total_amount = EXCLUDED.total_amount,
                                updated_at = clock_timestamp()
                            RETURNING *
                        )
                        INSERT INTO data_mutation_audit (
                            table_name, operation, business_date, row_key,
                            old_row, new_row, database_user_name, application_name,
                            transaction_id)
                        SELECT 'daily_aggregates', 'REPAIR', repaired.business_date,
                               repaired.business_date::text,
                               (SELECT old_row FROM previous), to_jsonb(repaired),
                               current_user, current_setting('application_name', true),
                               txid_current()
                        FROM repaired
                        """)
                .bind("businessDate", businessDate)
                .fetch()
                .rowsUpdated()
                .then(client.sql("""
                                INSERT INTO daily_metrics (
                                    business_date,
                                    consumed_event_count,
                                    unique_event_count,
                                    db_record_count,
                                    db_total_amount)
                                SELECT :businessDate,
                                       (SELECT COUNT(*) FROM source_event_observations
                                        WHERE business_date = :businessDate),
                                       (SELECT COUNT(*) FROM business_event_ledger
                                        WHERE business_date = :businessDate),
                                       (SELECT COUNT(*) FROM business_records
                                        WHERE business_date = :businessDate),
                                       (SELECT COALESCE(SUM(amount), 0) FROM business_records
                                        WHERE business_date = :businessDate)
                                ON CONFLICT (business_date) DO UPDATE
                                SET consumed_event_count = EXCLUDED.consumed_event_count,
                                    unique_event_count = EXCLUDED.unique_event_count,
                                    db_record_count = EXCLUDED.db_record_count,
                                    db_total_amount = EXCLUDED.db_total_amount,
                                    updated_at = clock_timestamp()
                                """)
                        .bind("businessDate", businessDate)
                        .fetch()
                        .rowsUpdated()
                        .then());
        return transactions.transactional(repair);
    }
}
