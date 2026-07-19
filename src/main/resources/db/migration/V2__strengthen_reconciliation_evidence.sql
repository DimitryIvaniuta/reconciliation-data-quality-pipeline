ALTER TABLE daily_metrics
    ADD COLUMN unique_event_count BIGINT NOT NULL DEFAULT 0
        CHECK (unique_event_count >= 0),
    ADD COLUMN db_total_amount NUMERIC(24, 4) NOT NULL DEFAULT 0
        CHECK (db_total_amount >= 0);

CREATE TABLE source_event_observations (
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    event_id UUID NOT NULL REFERENCES business_event_ledger(event_id),
    business_date DATE NOT NULL,
    source_timestamp TIMESTAMPTZ NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (source_topic, source_partition, source_offset)
);

CREATE INDEX idx_source_event_observations_date
    ON source_event_observations (business_date);

CREATE INDEX idx_source_event_observations_event
    ON source_event_observations (event_id);

ALTER TABLE business_event_ledger
    DROP CONSTRAINT uq_business_event_source;

INSERT INTO source_event_observations (
    source_topic,
    source_partition,
    source_offset,
    event_id,
    business_date,
    source_timestamp,
    payload_hash,
    observed_at)
SELECT source_topic,
       source_partition,
       source_offset,
       event_id,
       business_date,
       source_timestamp,
       encode(sha256(convert_to(payload::text, 'UTF8')), 'hex'),
       first_seen_at
FROM business_event_ledger;

DROP TRIGGER business_event_ledger_metric_insert ON business_event_ledger;
DROP FUNCTION increment_consumed_metric();

CREATE OR REPLACE FUNCTION increment_source_observation_metric()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO daily_metrics (business_date, consumed_event_count)
    VALUES (NEW.business_date, 1)
    ON CONFLICT (business_date) DO UPDATE
    SET consumed_event_count = daily_metrics.consumed_event_count + 1,
        updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER source_event_observation_metric_insert
AFTER INSERT ON source_event_observations
FOR EACH ROW EXECUTE FUNCTION increment_source_observation_metric();

CREATE OR REPLACE FUNCTION increment_unique_event_metric()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO daily_metrics (business_date, unique_event_count)
    VALUES (NEW.business_date, 1)
    ON CONFLICT (business_date) DO UPDATE
    SET unique_event_count = daily_metrics.unique_event_count + 1,
        updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER business_event_ledger_unique_metric_insert
AFTER INSERT ON business_event_ledger
FOR EACH ROW EXECUTE FUNCTION increment_unique_event_metric();

CREATE TRIGGER source_event_observations_immutable
BEFORE UPDATE OR DELETE ON source_event_observations
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();

CREATE OR REPLACE FUNCTION adjust_database_metric()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_date DATE;
    count_adjustment BIGINT;
    amount_adjustment NUMERIC(24, 4);
BEGIN
    target_date := CASE WHEN TG_OP = 'INSERT' THEN NEW.business_date ELSE OLD.business_date END;
    count_adjustment := CASE WHEN TG_OP = 'INSERT' THEN 1 ELSE -1 END;
    amount_adjustment := CASE WHEN TG_OP = 'INSERT' THEN NEW.amount ELSE -OLD.amount END;

    INSERT INTO daily_metrics (business_date, db_record_count, db_total_amount)
    VALUES (target_date, count_adjustment, amount_adjustment)
    ON CONFLICT (business_date) DO UPDATE
    SET db_record_count = daily_metrics.db_record_count + count_adjustment,
        db_total_amount = daily_metrics.db_total_amount + amount_adjustment,
        updated_at = clock_timestamp();

    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    RETURN OLD;
END;
$$;

UPDATE daily_metrics metrics
SET consumed_event_count = source_counts.record_count,
    unique_event_count = source_counts.unique_count,
    db_record_count = source_counts.database_count,
    db_total_amount = source_counts.database_amount,
    updated_at = clock_timestamp()
FROM (
    SELECT dates.business_date,
           COALESCE(observations.record_count, 0) AS record_count,
           COALESCE(events.unique_count, 0) AS unique_count,
           COALESCE(records.database_count, 0) AS database_count,
           COALESCE(records.database_amount, 0) AS database_amount
    FROM (
        SELECT business_date FROM daily_metrics
        UNION
        SELECT business_date FROM source_event_observations
        UNION
        SELECT business_date FROM business_event_ledger
        UNION
        SELECT business_date FROM business_records
    ) dates
    LEFT JOIN (
        SELECT business_date, COUNT(*) AS record_count
        FROM source_event_observations
        GROUP BY business_date
    ) observations USING (business_date)
    LEFT JOIN (
        SELECT business_date, COUNT(*) AS unique_count
        FROM business_event_ledger
        GROUP BY business_date
    ) events USING (business_date)
    LEFT JOIN (
        SELECT business_date,
               COUNT(*) AS database_count,
               COALESCE(SUM(amount), 0) AS database_amount
        FROM business_records
        GROUP BY business_date
    ) records USING (business_date)
) source_counts
WHERE metrics.business_date = source_counts.business_date;

INSERT INTO daily_metrics (
    business_date,
    consumed_event_count,
    unique_event_count,
    db_record_count,
    db_total_amount)
SELECT source_counts.business_date,
       source_counts.record_count,
       source_counts.unique_count,
       source_counts.database_count,
       source_counts.database_amount
FROM (
    SELECT dates.business_date,
           COALESCE(observations.record_count, 0) AS record_count,
           COALESCE(events.unique_count, 0) AS unique_count,
           COALESCE(records.database_count, 0) AS database_count,
           COALESCE(records.database_amount, 0) AS database_amount
    FROM (
        SELECT business_date FROM source_event_observations
        UNION
        SELECT business_date FROM business_event_ledger
        UNION
        SELECT business_date FROM business_records
    ) dates
    LEFT JOIN (
        SELECT business_date, COUNT(*) AS record_count
        FROM source_event_observations
        GROUP BY business_date
    ) observations USING (business_date)
    LEFT JOIN (
        SELECT business_date, COUNT(*) AS unique_count
        FROM business_event_ledger
        GROUP BY business_date
    ) events USING (business_date)
    LEFT JOIN (
        SELECT business_date,
               COUNT(*) AS database_count,
               COALESCE(SUM(amount), 0) AS database_amount
        FROM business_records
        GROUP BY business_date
    ) records USING (business_date)
) source_counts
ON CONFLICT (business_date) DO NOTHING;

ALTER TABLE reconciliation_reports
    ADD COLUMN unique_event_count BIGINT NOT NULL DEFAULT 0
        CHECK (unique_event_count >= 0),
    ADD COLUMN db_total_amount NUMERIC(24, 4) NOT NULL DEFAULT 0
        CHECK (db_total_amount >= 0),
    ADD COLUMN aggregate_amount NUMERIC(24, 4) NOT NULL DEFAULT 0
        CHECK (aggregate_amount >= 0),
    ADD COLUMN source_offsets JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE reconciliation_alerts
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    ADD CONSTRAINT uq_reconciliation_alert_report_channel UNIQUE (report_id, channel);

CREATE INDEX idx_reconciliation_alerts_recovery
    ON reconciliation_alerts (updated_at, created_at)
    WHERE status IN ('PENDING', 'FAILED');

ALTER TABLE replay_jobs
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    ADD COLUMN heartbeat_at TIMESTAMPTZ,
    ADD CONSTRAINT ck_replay_progress CHECK (replayed_events <= discovered_events);

CREATE INDEX idx_replay_jobs_stale_running
    ON replay_jobs (COALESCE(heartbeat_at, started_at, requested_at), requested_at)
    WHERE status = 'RUNNING';

CREATE TABLE replay_partition_checkpoints (
    job_id UUID NOT NULL REFERENCES replay_jobs(job_id) ON DELETE CASCADE,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    start_offset BIGINT NOT NULL CHECK (start_offset >= 0),
    end_offset BIGINT NOT NULL CHECK (end_offset >= start_offset),
    next_offset BIGINT NOT NULL CHECK (
        next_offset >= start_offset AND next_offset <= end_offset),
    discovered_events BIGINT NOT NULL DEFAULT 0 CHECK (discovered_events >= 0),
    replayed_events BIGINT NOT NULL DEFAULT 0 CHECK (
        replayed_events >= 0 AND replayed_events <= discovered_events),
    status VARCHAR(16) NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED')),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (job_id, source_topic, source_partition)
);

CREATE INDEX idx_replay_partition_checkpoints_job
    ON replay_partition_checkpoints (job_id, source_partition);

CREATE TABLE data_mutation_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name VARCHAR(128) NOT NULL,
    operation VARCHAR(16) NOT NULL,
    business_date DATE,
    row_key VARCHAR(300),
    old_row JSONB,
    new_row JSONB,
    database_user_name VARCHAR(128) NOT NULL,
    application_name VARCHAR(200),
    transaction_id BIGINT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_data_mutation_audit_search
    ON data_mutation_audit (business_date, changed_at DESC);

CREATE OR REPLACE FUNCTION audit_sensitive_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_date DATE;
    target_key VARCHAR(300);
BEGIN
    target_date := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.business_date
        ELSE NEW.business_date
    END;
    target_key := CASE
        WHEN TG_TABLE_NAME = 'business_records' THEN
            COALESCE(OLD.event_id::text, NEW.event_id::text)
        ELSE target_date::text
    END;

    INSERT INTO data_mutation_audit (
        table_name,
        operation,
        business_date,
        row_key,
        old_row,
        new_row,
        database_user_name,
        application_name,
        transaction_id)
    VALUES (
        TG_TABLE_NAME,
        TG_OP,
        target_date,
        target_key,
        CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE to_jsonb(OLD) END,
        CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE to_jsonb(NEW) END,
        current_user,
        current_setting('application_name', true),
        txid_current());

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER business_records_delete_audit
AFTER DELETE ON business_records
FOR EACH ROW EXECUTE FUNCTION audit_sensitive_mutation();

CREATE TRIGGER daily_aggregates_delete_audit
AFTER DELETE ON daily_aggregates
FOR EACH ROW EXECUTE FUNCTION audit_sensitive_mutation();

CREATE TRIGGER data_mutation_audit_immutable
BEFORE UPDATE OR DELETE ON data_mutation_audit
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();
