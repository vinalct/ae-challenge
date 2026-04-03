
CREATE NAMESPACE IF NOT EXISTS ae_challenge;

CREATE TABLE IF NOT EXISTS ae_challenge.ops_pipeline_run_log (
    run_id STRING,
    pipeline_name STRING,
    layer_name STRING,
    table_name STRING,
    run_status STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    input_row_count BIGINT,
    output_row_count BIGINT,
    latest_transaction_date DATE,
    latest_snapshot_date DATE,
    error_result_count BIGINT,
    warning_result_count BIGINT,
    details STRING
)
USING iceberg
PARTITIONED BY (days(finished_at));

CREATE TABLE IF NOT EXISTS ae_challenge.ops_data_quality_results (
    run_id STRING,
    pipeline_name STRING,
    layer_name STRING,
    table_name STRING,
    rule_name STRING,
    severity STRING,
    rule_status STRING,
    evaluated_at TIMESTAMP,
    transaction_date DATE,
    snapshot_date DATE,
    metric_value DOUBLE,
    threshold_value DOUBLE,
    impacted_record_count BIGINT,
    impacted_business_keys ARRAY<STRING>,
    description STRING
)
USING iceberg
PARTITIONED BY (days(evaluated_at));

CREATE TABLE IF NOT EXISTS ae_challenge.ops_data_quality_quarantine (
    run_id STRING,
    pipeline_name STRING,
    layer_name STRING,
    table_name STRING,
    rule_name STRING,
    severity STRING,
    captured_at TIMESTAMP,
    transaction_date DATE,
    snapshot_date DATE,
    business_key STRING,
    record_payload STRING
)
USING iceberg
PARTITIONED BY (days(captured_at));
