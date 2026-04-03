"""Contratos das tabelas operacionais de qualidade e observabilidade."""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

OPS_PIPELINE_RUN_LOG_TABLE = 'ops_pipeline_run_log'
OPS_DATA_QUALITY_RESULTS_TABLE = 'ops_data_quality_results'
OPS_DATA_QUALITY_QUARANTINE_TABLE = 'ops_data_quality_quarantine'

RULE_STATUS_PASSED = 'passed'
RULE_STATUS_FAILED = 'failed'

SEVERITY_ERROR = 'error'
SEVERITY_WARNING = 'warning'

RUN_STATUS_SUCCEEDED = 'succeeded'
RUN_STATUS_COMPLETED_WITH_WARNINGS = 'completed_with_warnings'
RUN_STATUS_FAILED = 'failed'

DEFAULT_DATA_QUALITY_PIPELINE_NAME = 'exercise_02_local_data_quality'
DEFAULT_WARNING_FRESHNESS_LAG_DAYS = 7.0
DEFAULT_WARNING_GMV_SWING_RATIO = 1.5
DEFAULT_SAMPLE_LIMIT = 5

OPS_PIPELINE_RUN_LOG_SCHEMA = StructType(
    [
        StructField('run_id', StringType(), False),
        StructField('pipeline_name', StringType(), False),
        StructField('layer_name', StringType(), False),
        StructField('table_name', StringType(), False),
        StructField('run_status', StringType(), False),
        StructField('started_at', TimestampType(), False),
        StructField('finished_at', TimestampType(), False),
        StructField('input_row_count', LongType(), True),
        StructField('output_row_count', LongType(), True),
        StructField('latest_transaction_date', DateType(), True),
        StructField('latest_snapshot_date', DateType(), True),
        StructField('error_result_count', LongType(), False),
        StructField('warning_result_count', LongType(), False),
        StructField('details', StringType(), True),
    ]
)

OPS_DATA_QUALITY_RESULTS_SCHEMA = StructType(
    [
        StructField('run_id', StringType(), False),
        StructField('pipeline_name', StringType(), False),
        StructField('layer_name', StringType(), False),
        StructField('table_name', StringType(), False),
        StructField('rule_name', StringType(), False),
        StructField('severity', StringType(), False),
        StructField('rule_status', StringType(), False),
        StructField('evaluated_at', TimestampType(), False),
        StructField('transaction_date', DateType(), True),
        StructField('snapshot_date', DateType(), True),
        StructField('metric_value', DoubleType(), True),
        StructField('threshold_value', DoubleType(), True),
        StructField('impacted_record_count', LongType(), False),
        StructField('impacted_business_keys', ArrayType(StringType(), containsNull=False), False),
        StructField('description', StringType(), True),
    ]
)

OPS_DATA_QUALITY_QUARANTINE_SCHEMA = StructType(
    [
        StructField('run_id', StringType(), False),
        StructField('pipeline_name', StringType(), False),
        StructField('layer_name', StringType(), False),
        StructField('table_name', StringType(), False),
        StructField('rule_name', StringType(), False),
        StructField('severity', StringType(), False),
        StructField('captured_at', TimestampType(), False),
        StructField('transaction_date', DateType(), True),
        StructField('snapshot_date', DateType(), True),
        StructField('business_key', StringType(), True),
        StructField('record_payload', StringType(), False),
    ]
)
