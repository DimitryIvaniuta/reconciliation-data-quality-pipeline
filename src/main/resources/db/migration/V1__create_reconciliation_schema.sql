CREATE TABLE business_event_ledger (
    event_id UUID PRIMARY KEY,
    business_date DATE NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    source_topic VARCHAR(249) NOT NULL,
    source_partition INTEGER NOT NULL CHECK (source_partition >= 0),
    source_offset BIGINT NOT NULL CHECK (source_offset >= 0),
    source_timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_business_event_source UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX idx_business_event_ledger_date
    ON business_event_ledger (business_date);

CREATE TABLE business_records (
    event_id UUID PRIMARY KEY REFERENCES business_event_ledger(event_id),
    business_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    amount NUMERIC(19, 4) NOT NULL CHECK (amount >= 0),
    payload_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_business_records_date
    ON business_records (business_date);

CREATE TABLE daily_aggregates (
    business_date DATE PRIMARY KEY,
    record_count BIGINT NOT NULL CHECK (record_count >= 0),
    total_amount NUMERIC(24, 4) NOT NULL CHECK (total_amount >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE daily_metrics (
    business_date DATE PRIMARY KEY,
    consumed_event_count BIGINT NOT NULL DEFAULT 0 CHECK (consumed_event_count >= 0),
    db_record_count BIGINT NOT NULL DEFAULT 0 CHECK (db_record_count >= 0),
    aggregate_record_count BIGINT NOT NULL DEFAULT 0 CHECK (aggregate_record_count >= 0),
    aggregate_amount NUMERIC(24, 4) NOT NULL DEFAULT 0 CHECK (aggregate_amount >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE reconciliation_reports (
    report_id UUID PRIMARY KEY,
    business_date DATE NOT NULL,
    trigger_type VARCHAR(32) NOT NULL CHECK (
        trigger_type IN ('SCHEDULED', 'MANUAL', 'REPLAY_VALIDATION')),
    status VARCHAR(16) NOT NULL CHECK (status IN ('MATCHED', 'MISMATCH')),
    kafka_event_count BIGINT NOT NULL CHECK (kafka_event_count >= 0),
    consumed_event_count BIGINT NOT NULL CHECK (consumed_event_count >= 0),
    db_record_count BIGINT NOT NULL CHECK (db_record_count >= 0),
    aggregate_record_count BIGINT NOT NULL CHECK (aggregate_record_count >= 0),
    issues JSONB NOT NULL,
    correlation_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_reconciliation_reports_search
    ON reconciliation_reports (business_date, status, created_at DESC);

CREATE TABLE reconciliation_alerts (
    alert_id UUID PRIMARY KEY,
    report_id UUID NOT NULL REFERENCES reconciliation_reports(report_id),
    channel VARCHAR(32) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED')),
    error_message VARCHAR(2000),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_reconciliation_alerts_report
    ON reconciliation_alerts (report_id, created_at);

CREATE TABLE replay_jobs (
    job_id UUID PRIMARY KEY,
    idempotency_key VARCHAR(200) NOT NULL UNIQUE,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    dry_run BOOLEAN NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('REQUESTED', 'RUNNING', 'COMPLETED', 'FAILED')),
    discovered_events BIGINT NOT NULL DEFAULT 0 CHECK (discovered_events >= 0),
    replayed_events BIGINT NOT NULL DEFAULT 0 CHECK (replayed_events >= 0),
    requested_by VARCHAR(200) NOT NULL,
    correlation_id VARCHAR(128) NOT NULL,
    error_message VARCHAR(2000),
    requested_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    command_published_at TIMESTAMPTZ,
    CONSTRAINT ck_replay_date_range CHECK (from_date <= to_date)
);

CREATE INDEX idx_replay_jobs_dispatch
    ON replay_jobs (status, command_published_at, requested_at)
    WHERE status = 'REQUESTED';

CREATE OR REPLACE FUNCTION reject_immutable_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'table % is append-only for operation %', TG_TABLE_NAME, TG_OP;
END;
$$;

CREATE TRIGGER business_event_ledger_immutable
BEFORE UPDATE OR DELETE ON business_event_ledger
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();

CREATE TRIGGER reconciliation_reports_immutable
BEFORE UPDATE OR DELETE ON reconciliation_reports
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();

CREATE TRIGGER business_records_no_update
BEFORE UPDATE ON business_records
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();

CREATE OR REPLACE FUNCTION increment_consumed_metric()
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

CREATE TRIGGER business_event_ledger_metric_insert
AFTER INSERT ON business_event_ledger
FOR EACH ROW EXECUTE FUNCTION increment_consumed_metric();

CREATE OR REPLACE FUNCTION adjust_database_metric()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_date DATE;
    adjustment BIGINT;
BEGIN
    target_date := CASE WHEN TG_OP = 'INSERT' THEN NEW.business_date ELSE OLD.business_date END;
    adjustment := CASE WHEN TG_OP = 'INSERT' THEN 1 ELSE -1 END;
    INSERT INTO daily_metrics (business_date, db_record_count)
    VALUES (target_date, GREATEST(adjustment, 0))
    ON CONFLICT (business_date) DO UPDATE
    SET db_record_count = GREATEST(daily_metrics.db_record_count + adjustment, 0),
        updated_at = clock_timestamp();
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER business_records_metric_change
AFTER INSERT OR DELETE ON business_records
FOR EACH ROW EXECUTE FUNCTION adjust_database_metric();

CREATE OR REPLACE FUNCTION synchronize_aggregate_metric()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_date DATE;
    target_count BIGINT;
    target_amount NUMERIC(24, 4);
BEGIN
    IF TG_OP = 'DELETE' THEN
        target_date := OLD.business_date;
        target_count := 0;
        target_amount := 0;
    ELSE
        target_date := NEW.business_date;
        target_count := NEW.record_count;
        target_amount := NEW.total_amount;
    END IF;
    INSERT INTO daily_metrics (
        business_date, aggregate_record_count, aggregate_amount)
    VALUES (target_date, target_count, target_amount)
    ON CONFLICT (business_date) DO UPDATE
    SET aggregate_record_count = EXCLUDED.aggregate_record_count,
        aggregate_amount = EXCLUDED.aggregate_amount,
        updated_at = clock_timestamp();
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER daily_aggregates_metric_change
AFTER INSERT OR UPDATE OR DELETE ON daily_aggregates
FOR EACH ROW EXECUTE FUNCTION synchronize_aggregate_metric();
