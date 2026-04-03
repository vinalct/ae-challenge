"""Avaliacao de qualidade e observabilidade para bronze, silver e gold."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from bronze.contracts import BRONZE_TABLE_SPECS
from gold.contracts import (
    GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
    GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
)
from silver.contracts import SILVER_TABLE_SPECS
from silver.standardization import VALID_PURCHASE_STATUSES
from .contracts import (
    DEFAULT_DATA_QUALITY_PIPELINE_NAME,
    DEFAULT_SAMPLE_LIMIT,
    DEFAULT_WARNING_FRESHNESS_LAG_DAYS,
    DEFAULT_WARNING_GMV_SWING_RATIO,
    OPS_DATA_QUALITY_QUARANTINE_SCHEMA,
    OPS_DATA_QUALITY_RESULTS_SCHEMA,
    OPS_PIPELINE_RUN_LOG_SCHEMA,
    RULE_STATUS_FAILED,
    RULE_STATUS_PASSED,
    RUN_STATUS_COMPLETED_WITH_WARNINGS,
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCEEDED,
    SEVERITY_ERROR,
    SEVERITY_WARNING,
)

BRONZE_TO_SOURCE = {
    spec.target_table: source_name for source_name, spec in BRONZE_TABLE_SPECS.items()
}
SILVER_TO_SOURCE = {
    spec.target_table: source_name for source_name, spec in SILVER_TABLE_SPECS.items()
}
TABLE_LAYER = {
    **{table_name: 'bronze' for table_name in BRONZE_TO_SOURCE},
    **{table_name: 'silver' for table_name in SILVER_TO_SOURCE},
    GOLD_PURCHASE_STATE_SNAPSHOT_TABLE: 'gold',
    GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE: 'gold',
}


def _normalize_timestamp(value: datetime | str | None) -> datetime:
    """Normaliza strings ISO ou valores nulos para um timestamp Python."""
    if value is None:
        return datetime.utcnow().replace(microsecond=0)
    if isinstance(value, datetime):
        return value.replace(microsecond=0)
    return datetime.fromisoformat(value).replace(microsecond=0)


def _table_row_count(df: DataFrame) -> int:
    """Retorna a contagem inteira de linhas de um dataframe."""
    return int(df.count())


def _max_date(df: DataFrame, column_name: str) -> datetime | None:
    """Retorna a maior data de uma coluna quando ela existe no dataframe."""
    if column_name not in df.columns:
        return None
    return df.agg(F.max(column_name).alias('max_value')).first()['max_value']


def _business_key_columns(table_name: str) -> tuple[str, ...]:
    """Retorna as colunas canonicas de chave de negocio por tabela publicada."""
    if table_name in BRONZE_TO_SOURCE:
        return SILVER_TABLE_SPECS[BRONZE_TO_SOURCE[table_name]].business_keys
    if table_name in SILVER_TO_SOURCE:
        return SILVER_TABLE_SPECS[SILVER_TO_SOURCE[table_name]].business_keys
    if table_name == GOLD_PURCHASE_STATE_SNAPSHOT_TABLE:
        return ('snapshot_date', 'purchase_id', 'purchase_partition')
    if table_name == GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE:
        return ('snapshot_date', 'gmv_date', 'subsidiary')
    return ()


def _business_key_string(row_dict: dict[str, object], key_columns: Iterable[str]) -> str | None:
    """Monta a chave de negocio como string unica para auditoria."""
    key_columns = tuple(key_columns)
    if not key_columns:
        return None
    return '|'.join('null' if row_dict.get(column_name) is None else str(row_dict[column_name]) for column_name in key_columns)


def _top_impacted_keys(
    failed_df: DataFrame,
    key_columns: tuple[str, ...],
    sample_limit: int,
) -> list[str]:
    """Coleta as principais chaves impactadas para uma regra falhada."""
    if not key_columns or failed_df.limit(1).count() == 0:
        return []
    key_expression = F.concat_ws(
        '|',
        *[
            F.when(F.col(column_name).isNull(), F.lit('null'))
            .otherwise(F.col(column_name).cast('string'))
            for column_name in key_columns
        ],
    )
    return [
        row['business_key']
        for row in failed_df.select(key_expression.alias('business_key'))
        .where(F.col('business_key').isNotNull())
        .distinct()
        .limit(sample_limit)
        .collect()
    ]


def _quarantine_records(
    failed_df: DataFrame,
    *,
    run_id: str,
    pipeline_name: str,
    layer_name: str,
    table_name: str,
    rule_name: str,
    severity: str,
    captured_at: datetime,
    key_columns: tuple[str, ...],
    sample_limit: int,
) -> list[dict[str, object]]:
    """Materializa amostras de linhas falhas para a tabela de quarantine."""
    records: list[dict[str, object]] = []
    for row in failed_df.limit(sample_limit).collect():
        row_dict = row.asDict(recursive=True)
        records.append(
            {
                'run_id': run_id,
                'pipeline_name': pipeline_name,
                'layer_name': layer_name,
                'table_name': table_name,
                'rule_name': rule_name,
                'severity': severity,
                'captured_at': captured_at,
                'transaction_date': row_dict.get('transaction_date'),
                'snapshot_date': row_dict.get('snapshot_date'),
                'business_key': _business_key_string(row_dict, key_columns),
                'record_payload': json.dumps(row_dict, default=str, sort_keys=True),
            }
        )
    return records


def _append_failed_rows_rule(
    results_records: list[dict[str, object]],
    quarantine_records: list[dict[str, object]],
    failed_df: DataFrame,
    *,
    run_id: str,
    pipeline_name: str,
    layer_name: str,
    table_name: str,
    rule_name: str,
    severity: str,
    description: str,
    evaluated_at: datetime,
    key_columns: tuple[str, ...],
    threshold_value: float | None = 0.0,
    metric_value: float | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    transaction_date: datetime | None = None,
    snapshot_date: datetime | None = None,
) -> None:
    """Registra um check baseado em linhas falhas e amostras de quarantine."""
    impacted_record_count = int(failed_df.count())
    rule_status = RULE_STATUS_FAILED if impacted_record_count > 0 else RULE_STATUS_PASSED
    impacted_keys = (
        _top_impacted_keys(failed_df, key_columns, sample_limit)
        if impacted_record_count > 0
        else []
    )
    results_records.append(
        {
            'run_id': run_id,
            'pipeline_name': pipeline_name,
            'layer_name': layer_name,
            'table_name': table_name,
            'rule_name': rule_name,
            'severity': severity,
            'rule_status': rule_status,
            'evaluated_at': evaluated_at,
            'transaction_date': transaction_date,
            'snapshot_date': snapshot_date,
            'metric_value': float(impacted_record_count if metric_value is None else metric_value),
            'threshold_value': threshold_value,
            'impacted_record_count': impacted_record_count,
            'impacted_business_keys': impacted_keys,
            'description': description,
        }
    )
    if impacted_record_count > 0:
        quarantine_records.extend(
            _quarantine_records(
                failed_df,
                run_id=run_id,
                pipeline_name=pipeline_name,
                layer_name=layer_name,
                table_name=table_name,
                rule_name=rule_name,
                severity=severity,
                captured_at=evaluated_at,
                key_columns=key_columns,
                sample_limit=sample_limit,
            )
        )


def _append_scalar_rule(
    results_records: list[dict[str, object]],
    *,
    run_id: str,
    pipeline_name: str,
    layer_name: str,
    table_name: str,
    rule_name: str,
    severity: str,
    description: str,
    evaluated_at: datetime,
    metric_value: float | None,
    threshold_value: float | None,
    failed: bool,
    impacted_record_count: int = 0,
) -> None:
    """Registra um check baseado apenas em metricas agregadas."""
    results_records.append(
        {
            'run_id': run_id,
            'pipeline_name': pipeline_name,
            'layer_name': layer_name,
            'table_name': table_name,
            'rule_name': rule_name,
            'severity': severity,
            'rule_status': RULE_STATUS_FAILED if failed else RULE_STATUS_PASSED,
            'evaluated_at': evaluated_at,
            'transaction_date': None,
            'snapshot_date': None,
            'metric_value': metric_value,
            'threshold_value': threshold_value,
            'impacted_record_count': int(impacted_record_count),
            'impacted_business_keys': [],
            'description': description,
        }
    )


def _bronze_reference_transaction_date(table_dataframes: dict[str, DataFrame]) -> datetime | None:
    """Retorna a maior transaction_date observada na camada bronze."""
    max_dates = [
        _max_date(table_dataframes[spec.target_table], 'transaction_date')
        for spec in BRONZE_TABLE_SPECS.values()
        if spec.target_table in table_dataframes
    ]
    max_dates = [value for value in max_dates if value is not None]
    return max(max_dates) if max_dates else None


def _evaluate_bronze_quality(
    table_dataframes: dict[str, DataFrame],
    *,
    run_id: str,
    pipeline_name: str,
    evaluated_at: datetime,
    sample_limit: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Avalia as regras minimas da camada bronze."""
    results_records: list[dict[str, object]] = []
    quarantine_records: list[dict[str, object]] = []
    reference_transaction_date = _bronze_reference_transaction_date(table_dataframes)

    for source_name, spec in BRONZE_TABLE_SPECS.items():
        table_name = spec.target_table
        df = table_dataframes[table_name]
        expected_columns = list(spec.schema.fieldNames()) + list(spec.metadata_columns)
        missing_columns = [column_name for column_name in expected_columns if column_name not in df.columns]
        unexpected_columns = [column_name for column_name in df.columns if column_name not in expected_columns]
        schema_issue_count = len(missing_columns) + len(unexpected_columns)
        _append_scalar_rule(
            results_records,
            run_id=run_id,
            pipeline_name=pipeline_name,
            layer_name='bronze',
            table_name=table_name,
            rule_name='bronze_schema_contract_match',
            severity=SEVERITY_ERROR,
            description=(
                'Expected columns must match the bronze contract exactly. '
                f'Missing={missing_columns or []}; unexpected={unexpected_columns or []}.'
            ),
            evaluated_at=evaluated_at,
            metric_value=float(schema_issue_count),
            threshold_value=0.0,
            failed=schema_issue_count > 0,
            impacted_record_count=schema_issue_count,
        )

        _append_failed_rows_rule(
            results_records,
            quarantine_records,
            df.where(F.col(spec.partition_field).isNull()),
            run_id=run_id,
            pipeline_name=pipeline_name,
            layer_name='bronze',
            table_name=table_name,
            rule_name='bronze_partition_key_completeness',
            severity=SEVERITY_ERROR,
            description='Rows must always carry the physical partition key transaction_date.',
            evaluated_at=evaluated_at,
            key_columns=_business_key_columns(table_name),
            threshold_value=0.0,
            sample_limit=sample_limit,
        )

        row_count = _table_row_count(df)
        duplicate_window = Window.partitionBy('record_hash').orderBy(F.col('ingestion_ts').asc_nulls_last())
        duplicate_df = (
            df.withColumn('_duplicate_rank', F.row_number().over(duplicate_window))
            .where(F.col('_duplicate_rank') > 1)
            .drop('_duplicate_rank')
        )
        duplicate_count = int(duplicate_df.count())
        duplicate_rate = float(duplicate_count / row_count) if row_count > 0 else 0.0
        _append_failed_rows_rule(
            results_records,
            quarantine_records,
            duplicate_df,
            run_id=run_id,
            pipeline_name=pipeline_name,
            layer_name='bronze',
            table_name=table_name,
            rule_name='bronze_raw_duplicate_rate',
            severity=SEVERITY_WARNING,
            description='Raw duplicate resend rate should remain at zero in bronze monitoring.',
            evaluated_at=evaluated_at,
            key_columns=_business_key_columns(table_name),
            threshold_value=0.0,
            metric_value=duplicate_rate,
            sample_limit=sample_limit,
        )

        latest_transaction_date = _max_date(df, 'transaction_date')
        lag_days = None
        failed_freshness = row_count == 0
        if latest_transaction_date is not None and reference_transaction_date is not None:
            lag_days = float((reference_transaction_date - latest_transaction_date).days)
            failed_freshness = lag_days > DEFAULT_WARNING_FRESHNESS_LAG_DAYS
        _append_scalar_rule(
            results_records,
            run_id=run_id,
            pipeline_name=pipeline_name,
            layer_name='bronze',
            table_name=table_name,
            rule_name='bronze_freshness_lag_days',
            severity=SEVERITY_WARNING,
            description='Freshness is measured by lag in transaction_date versus the freshest bronze table.',
            evaluated_at=evaluated_at,
            metric_value=lag_days,
            threshold_value=DEFAULT_WARNING_FRESHNESS_LAG_DAYS,
            failed=failed_freshness,
            impacted_record_count=row_count if failed_freshness else 0,
        )

    return results_records, quarantine_records


def _evaluate_silver_quality(
    table_dataframes: dict[str, DataFrame],
    *,
    run_id: str,
    pipeline_name: str,
    evaluated_at: datetime,
    sample_limit: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Avalia as regras minimas da camada silver."""
    results_records: list[dict[str, object]] = []
    quarantine_records: list[dict[str, object]] = []

    for source_name, spec in SILVER_TABLE_SPECS.items():
        table_name = spec.target_table
        df = table_dataframes[table_name]
        key_columns = spec.business_keys

        _append_failed_rows_rule(
            results_records,
            quarantine_records,
            df.where((~F.col('is_business_key_complete')) | (~F.col('is_ordering_valid'))),
            run_id=run_id,
            pipeline_name=pipeline_name,
            layer_name='silver',
            table_name=table_name,
            rule_name='silver_blocking_quality_status',
            severity=SEVERITY_ERROR,
            description='Rows used for CDC ordering must have complete business keys and ordering columns.',
            evaluated_at=evaluated_at,
            key_columns=key_columns,
            threshold_value=0.0,
            sample_limit=sample_limit,
        )

        duplicate_group_df = (
            df.groupBy(*key_columns, 'record_hash')
            .count()
            .where(F.col('count') > 1)
        )
        _append_failed_rows_rule(
            results_records,
            quarantine_records,
            duplicate_group_df,
            run_id=run_id,
            pipeline_name=pipeline_name,
            layer_name='silver',
            table_name=table_name,
            rule_name='silver_post_dedup_exact_resend_uniqueness',
            severity=SEVERITY_ERROR,
            description='Silver must keep only one row per business key plus record_hash after deduplication.',
            evaluated_at=evaluated_at,
            key_columns=key_columns,
            threshold_value=0.0,
            sample_limit=sample_limit,
        )

        if source_name == 'purchase':
            _append_failed_rows_rule(
                results_records,
                quarantine_records,
                df.where(
                    F.col('purchase_status').isNull()
                    | (~F.col('purchase_status').isin(*VALID_PURCHASE_STATUSES))
                ),
                run_id=run_id,
                pipeline_name=pipeline_name,
                layer_name='silver',
                table_name=table_name,
                rule_name='silver_purchase_status_contract',
                severity=SEVERITY_ERROR,
                description='purchase_status must be present and belong to the expected enum.',
                evaluated_at=evaluated_at,
                key_columns=key_columns,
                threshold_value=0.0,
                sample_limit=sample_limit,
            )
            negative_amount_df = df.where(F.col('purchase_total_value') < 0)
        elif source_name == 'product_item':
            negative_amount_df = df.where(F.col('purchase_value') < 0)
        elif source_name == 'order_transaction_cost_hist':
            negative_amount_df = df.where(
                (F.col('order_transaction_cost_vat_value') < 0)
                | (F.col('order_transaction_cost_installment_value') < 0)
            )
        else:
            negative_amount_df = df.limit(0)

        _append_failed_rows_rule(
            results_records,
            quarantine_records,
            negative_amount_df,
            run_id=run_id,
            pipeline_name=pipeline_name,
            layer_name='silver',
            table_name=table_name,
            rule_name='silver_negative_amount_contract',
            severity=SEVERITY_ERROR,
            description='Monetary values in silver must not be negative for metric-driving sources.',
            evaluated_at=evaluated_at,
            key_columns=key_columns,
            threshold_value=0.0,
            sample_limit=sample_limit,
        )

        if source_name == 'purchase':
            orphan_df = df.where((~F.col('has_product_item_match')) | (~F.col('has_extra_info_match')))
        else:
            orphan_df = df.where(~F.col('has_purchase_match'))
        _append_failed_rows_rule(
            results_records,
            quarantine_records,
            orphan_df,
            run_id=run_id,
            pipeline_name=pipeline_name,
            layer_name='silver',
            table_name=table_name,
            rule_name='silver_orphan_reference_contract',
            severity=SEVERITY_WARNING,
            description='Reference relationships between source entities should be resolvable in silver.',
            evaluated_at=evaluated_at,
            key_columns=key_columns,
            threshold_value=0.0,
            sample_limit=sample_limit,
        )

    return results_records, quarantine_records


def _evaluate_gold_snapshot_quality(
    table_dataframes: dict[str, DataFrame],
    *,
    run_id: str,
    pipeline_name: str,
    evaluated_at: datetime,
    sample_limit: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Avalia as regras minimas da tabela gold de snapshot."""
    results_records: list[dict[str, object]] = []
    quarantine_records: list[dict[str, object]] = []
    df = table_dataframes[GOLD_PURCHASE_STATE_SNAPSHOT_TABLE]
    key_columns = _business_key_columns(GOLD_PURCHASE_STATE_SNAPSHOT_TABLE)

    duplicate_df = (
        df.groupBy(*key_columns)
        .count()
        .where(F.col('count') > 1)
    )
    _append_failed_rows_rule(
        results_records,
        quarantine_records,
        duplicate_df,
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer_name='gold',
        table_name=GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
        rule_name='gold_snapshot_uniqueness_by_grain',
        severity=SEVERITY_ERROR,
        description='The purchase snapshot must keep a single row per snapshot_date and purchase key.',
        evaluated_at=evaluated_at,
        key_columns=key_columns,
        threshold_value=0.0,
        sample_limit=sample_limit,
    )

    future_leakage_df = df.where(
        (F.col('purchase_source_transaction_date').isNotNull() & (F.col('purchase_source_transaction_date') > F.col('snapshot_date')))
        | (
            F.col('product_item_source_transaction_date').isNotNull()
            & (F.col('product_item_source_transaction_date') > F.col('snapshot_date'))
        )
        | (
            F.col('purchase_extra_info_source_transaction_date').isNotNull()
            & (F.col('purchase_extra_info_source_transaction_date') > F.col('snapshot_date'))
        )
        | (
            F.col('order_transaction_cost_hist_source_transaction_date').isNotNull()
            & (F.col('order_transaction_cost_hist_source_transaction_date') > F.col('snapshot_date'))
        )
    )
    _append_failed_rows_rule(
        results_records,
        quarantine_records,
        future_leakage_df,
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer_name='gold',
        table_name=GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
        rule_name='gold_snapshot_no_future_leakage',
        severity=SEVERITY_ERROR,
        description='Snapshot rows must not reference source records newer than snapshot_date.',
        evaluated_at=evaluated_at,
        key_columns=key_columns,
        threshold_value=0.0,
        sample_limit=sample_limit,
    )

    completeness_flag_df = df.where(
        (F.col('has_purchase') != F.col('purchase_source_record_hash').isNotNull())
        | (F.col('has_product_item') != F.col('product_item_source_record_hash').isNotNull())
        | (F.col('has_extra_info') != F.col('purchase_extra_info_source_record_hash').isNotNull())
        | (
            F.col('has_order_transaction_cost_hist')
            != F.col('order_transaction_cost_hist_source_record_hash').isNotNull()
        )
    )
    _append_failed_rows_rule(
        results_records,
        quarantine_records,
        completeness_flag_df,
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer_name='gold',
        table_name=GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
        rule_name='gold_snapshot_completeness_flag_contract',
        severity=SEVERITY_ERROR,
        description='Completeness flags must match the presence of contributing source rows in the snapshot.',
        evaluated_at=evaluated_at,
        key_columns=key_columns,
        threshold_value=0.0,
        sample_limit=sample_limit,
    )

    expected_metric_eligibility = (
        F.col('has_purchase')
        & F.col('has_product_item')
        & F.col('has_extra_info')
        & F.col('subsidiary').isNotNull()
        & (F.col('purchase_status') == 'APROVADA')
        & F.col('release_date').isNotNull()
        & F.col('purchase_total_value').isNotNull()
        & (F.col('purchase_total_value') >= 0)
        & (F.col('purchase_quality_status') != 'error')
        & (
            F.col('product_item_quality_status').isNull()
            | (F.col('product_item_quality_status') != 'error')
        )
        & (
            F.col('purchase_extra_info_quality_status').isNull()
            | (F.col('purchase_extra_info_quality_status') != 'error')
        )
    )
    eligibility_contract_df = df.where(F.col('is_metric_eligible') != expected_metric_eligibility)
    _append_failed_rows_rule(
        results_records,
        quarantine_records,
        eligibility_contract_df,
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer_name='gold',
        table_name=GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
        rule_name='gold_snapshot_metric_eligibility_contract',
        severity=SEVERITY_ERROR,
        description='is_metric_eligible must be reproducible from the published snapshot columns.',
        evaluated_at=evaluated_at,
        key_columns=key_columns,
        threshold_value=0.0,
        sample_limit=sample_limit,
    )

    return results_records, quarantine_records


def _evaluate_gold_gmv_quality(
    table_dataframes: dict[str, DataFrame],
    *,
    run_id: str,
    pipeline_name: str,
    evaluated_at: datetime,
    sample_limit: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Avalia as regras minimas da tabela gold de GMV agregado."""
    results_records: list[dict[str, object]] = []
    quarantine_records: list[dict[str, object]] = []
    gmv_df = table_dataframes[GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE]
    snapshot_df = table_dataframes[GOLD_PURCHASE_STATE_SNAPSHOT_TABLE]
    key_columns = _business_key_columns(GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE)

    duplicate_df = (
        gmv_df.groupBy(*key_columns)
        .count()
        .where(F.col('count') > 1)
    )
    _append_failed_rows_rule(
        results_records,
        quarantine_records,
        duplicate_df,
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer_name='gold',
        table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
        rule_name='gold_gmv_uniqueness_by_grain',
        severity=SEVERITY_ERROR,
        description='The GMV aggregate must keep a single row per snapshot_date, gmv_date and subsidiary.',
        evaluated_at=evaluated_at,
        key_columns=key_columns,
        threshold_value=0.0,
        sample_limit=sample_limit,
    )

    reconciliation_expected_df = snapshot_df.where(F.col('is_metric_eligible')).groupBy(
        'snapshot_date', 'gmv_date', 'subsidiary'
    ).agg(
        F.sum('purchase_total_value').cast('double').alias('expected_gmv_daily_amount'),
        F.count_distinct('purchase_id', 'purchase_partition').cast('long').alias('expected_gmv_daily_purchase_count'),
        F.sum(F.coalesce(F.col('item_quantity').cast('long'), F.lit(0))).cast('long').alias('expected_gmv_daily_item_quantity'),
    )
    reconciliation_df = (
        reconciliation_expected_df.alias('expected')
        .join(
            gmv_df.alias('actual'),
            on=['snapshot_date', 'gmv_date', 'subsidiary'],
            how='full_outer',
        )
        .where(
            F.col('expected.expected_gmv_daily_amount').isNull()
            | F.col('actual.gmv_daily_amount').isNull()
            | (F.abs(F.col('expected.expected_gmv_daily_amount') - F.col('actual.gmv_daily_amount')) > F.lit(1e-9))
            | (
                F.coalesce(F.col('expected.expected_gmv_daily_purchase_count'), F.lit(-1))
                != F.coalesce(F.col('actual.gmv_daily_purchase_count'), F.lit(-1))
            )
            | (
                F.coalesce(F.col('expected.expected_gmv_daily_item_quantity'), F.lit(-1))
                != F.coalesce(F.col('actual.gmv_daily_item_quantity'), F.lit(-1))
            )
        )
        .select(
            F.coalesce(F.col('expected.snapshot_date'), F.col('actual.snapshot_date')).alias('snapshot_date'),
            F.coalesce(F.col('expected.gmv_date'), F.col('actual.gmv_date')).alias('gmv_date'),
            F.coalesce(F.col('expected.subsidiary'), F.col('actual.subsidiary')).alias('subsidiary'),
            F.col('expected.expected_gmv_daily_amount'),
            F.col('actual.gmv_daily_amount'),
            F.col('expected.expected_gmv_daily_purchase_count'),
            F.col('actual.gmv_daily_purchase_count'),
            F.col('expected.expected_gmv_daily_item_quantity'),
            F.col('actual.gmv_daily_item_quantity'),
        )
    )
    _append_failed_rows_rule(
        results_records,
        quarantine_records,
        reconciliation_df,
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer_name='gold',
        table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
        rule_name='gold_gmv_reconciliation_to_snapshot',
        severity=SEVERITY_ERROR,
        description='GMV aggregates must reconcile with eligible purchase snapshot rows at the same grain.',
        evaluated_at=evaluated_at,
        key_columns=key_columns,
        threshold_value=0.0,
        sample_limit=sample_limit,
    )

    non_negative_df = gmv_df.where(
        (F.col('gmv_daily_amount') < 0)
        | (F.col('gmv_mtd_amount') < 0)
        | (F.col('gmv_daily_purchase_count') < 0)
        | (F.col('gmv_daily_item_quantity') < 0)
    )
    _append_failed_rows_rule(
        results_records,
        quarantine_records,
        non_negative_df,
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer_name='gold',
        table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
        rule_name='gold_gmv_non_negative_amounts',
        severity=SEVERITY_ERROR,
        description='Published GMV metrics must never be negative.',
        evaluated_at=evaluated_at,
        key_columns=key_columns,
        threshold_value=0.0,
        sample_limit=sample_limit,
    )

    anomaly_df = (
        gmv_df.withColumn(
            'previous_gmv_daily_amount',
            F.lag('gmv_daily_amount').over(
                Window.partitionBy('snapshot_date', 'subsidiary').orderBy('gmv_date')
            ),
        )
        .withColumn(
            'gmv_daily_swing_ratio',
            F.when(
                F.col('previous_gmv_daily_amount').isNull()
                | (F.col('previous_gmv_daily_amount') == 0),
                F.lit(None).cast('double'),
            ).otherwise(
                F.abs(F.col('gmv_daily_amount') - F.col('previous_gmv_daily_amount'))
                / F.abs(F.col('previous_gmv_daily_amount'))
            ),
        )
        .where(F.col('gmv_daily_swing_ratio') > F.lit(DEFAULT_WARNING_GMV_SWING_RATIO))
    )
    max_swing_ratio = None
    if anomaly_df.limit(1).count() > 0:
        max_swing_ratio = float(anomaly_df.agg(F.max('gmv_daily_swing_ratio').alias('max_swing_ratio')).first()['max_swing_ratio'])
    _append_failed_rows_rule(
        results_records,
        quarantine_records,
        anomaly_df,
        run_id=run_id,
        pipeline_name=pipeline_name,
        layer_name='gold',
        table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
        rule_name='gold_gmv_daily_swing_anomaly',
        severity=SEVERITY_WARNING,
        description='Large day-over-day GMV swings are published as warnings for owner review.',
        evaluated_at=evaluated_at,
        key_columns=key_columns,
        threshold_value=DEFAULT_WARNING_GMV_SWING_RATIO,
        metric_value=max_swing_ratio,
        sample_limit=sample_limit,
    )

    return results_records, quarantine_records


def _table_stats(table_dataframes: dict[str, DataFrame]) -> dict[str, dict[str, object]]:
    """Coleta estatisticas simples por tabela para o run log."""
    return {
        table_name: {
            'row_count': _table_row_count(df),
            'latest_transaction_date': _max_date(df, 'transaction_date'),
            'latest_snapshot_date': _max_date(df, 'snapshot_date'),
        }
        for table_name, df in table_dataframes.items()
    }


def _input_row_count(table_name: str, stats: dict[str, dict[str, object]]) -> int | None:
    """Deriva a contagem de entrada aproximada por etapa do pipeline."""
    if table_name in BRONZE_TO_SOURCE:
        return stats[table_name]['row_count']
    if table_name in SILVER_TO_SOURCE:
        source_name = SILVER_TO_SOURCE[table_name]
        bronze_table = BRONZE_TABLE_SPECS[source_name].target_table
        return stats[bronze_table]['row_count']
    if table_name == GOLD_PURCHASE_STATE_SNAPSHOT_TABLE:
        return sum(stats[spec.target_table]['row_count'] for spec in SILVER_TABLE_SPECS.values())
    if table_name == GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE:
        return stats[GOLD_PURCHASE_STATE_SNAPSHOT_TABLE]['row_count']
    return None


def _run_status(error_count: int, warning_count: int) -> str:
    """Resume o status de uma etapa com base em erros e warnings falhos."""
    if error_count > 0:
        return RUN_STATUS_FAILED
    if warning_count > 0:
        return RUN_STATUS_COMPLETED_WITH_WARNINGS
    return RUN_STATUS_SUCCEEDED


def _build_pipeline_run_log_records(
    table_dataframes: dict[str, DataFrame],
    quality_results_records: list[dict[str, object]],
    *,
    run_id: str,
    pipeline_name: str,
    run_started_at: datetime,
    evaluated_at: datetime,
) -> list[dict[str, object]]:
    """Resume o status do pipeline por tabela e em nivel geral."""
    stats = _table_stats(table_dataframes)
    records: list[dict[str, object]] = []
    ordered_tables = [
        *[spec.target_table for spec in BRONZE_TABLE_SPECS.values()],
        *[spec.target_table for spec in SILVER_TABLE_SPECS.values()],
        GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
        GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
    ]
    for table_name in ordered_tables:
        table_results = [
            result
            for result in quality_results_records
            if result['table_name'] == table_name and result['rule_status'] == RULE_STATUS_FAILED
        ]
        error_count = sum(1 for result in table_results if result['severity'] == SEVERITY_ERROR)
        warning_count = sum(1 for result in table_results if result['severity'] == SEVERITY_WARNING)
        failed_rules = [result['rule_name'] for result in table_results]
        records.append(
            {
                'run_id': run_id,
                'pipeline_name': pipeline_name,
                'layer_name': TABLE_LAYER[table_name],
                'table_name': table_name,
                'run_status': _run_status(error_count, warning_count),
                'started_at': run_started_at,
                'finished_at': evaluated_at,
                'input_row_count': _input_row_count(table_name, stats),
                'output_row_count': stats[table_name]['row_count'],
                'latest_transaction_date': stats[table_name]['latest_transaction_date'],
                'latest_snapshot_date': stats[table_name]['latest_snapshot_date'],
                'error_result_count': error_count,
                'warning_result_count': warning_count,
                'details': json.dumps({'failed_rules': failed_rules}, sort_keys=True),
            }
        )

    total_error_count = sum(record['error_result_count'] for record in records)
    total_warning_count = sum(record['warning_result_count'] for record in records)
    records.append(
        {
            'run_id': run_id,
            'pipeline_name': pipeline_name,
            'layer_name': 'pipeline',
            'table_name': 'all_tables',
            'run_status': _run_status(total_error_count, total_warning_count),
            'started_at': run_started_at,
            'finished_at': evaluated_at,
            'input_row_count': sum(
                stats[spec.target_table]['row_count'] for spec in BRONZE_TABLE_SPECS.values()
            ),
            'output_row_count': stats[GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE]['row_count'],
            'latest_transaction_date': max(
                (value['latest_transaction_date'] for value in stats.values() if value['latest_transaction_date'] is not None),
                default=None,
            ),
            'latest_snapshot_date': max(
                (value['latest_snapshot_date'] for value in stats.values() if value['latest_snapshot_date'] is not None),
                default=None,
            ),
            'error_result_count': total_error_count,
            'warning_result_count': total_warning_count,
            'details': json.dumps(
                {
                    'tables': [
                        {
                            'table_name': record['table_name'],
                            'run_status': record['run_status'],
                        }
                        for record in records
                    ]
                },
                sort_keys=True,
            ),
        }
    )
    return records


def _create_dataframe(
    spark: SparkSession,
    records: list[dict[str, object]],
    schema,
) -> DataFrame:
    """Cria um dataframe mesmo quando a lista de registros esta vazia."""
    return spark.createDataFrame(records, schema=schema)


def build_data_quality_outputs(
    table_dataframes: dict[str, DataFrame],
    *,
    run_id: str,
    pipeline_name: str = DEFAULT_DATA_QUALITY_PIPELINE_NAME,
    run_started_at: datetime | str | None = None,
    evaluated_at: datetime | str | None = None,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Avalia todas as regras e retorna dataframes prontos para publicacao."""
    spark = next(iter(table_dataframes.values())).sparkSession
    started_at_value = _normalize_timestamp(run_started_at)
    evaluated_at_value = _normalize_timestamp(evaluated_at)

    results_records: list[dict[str, object]] = []
    quarantine_records: list[dict[str, object]] = []

    for rule_results, rule_quarantine in (
        _evaluate_bronze_quality(
            table_dataframes,
            run_id=run_id,
            pipeline_name=pipeline_name,
            evaluated_at=evaluated_at_value,
            sample_limit=sample_limit,
        ),
        _evaluate_silver_quality(
            table_dataframes,
            run_id=run_id,
            pipeline_name=pipeline_name,
            evaluated_at=evaluated_at_value,
            sample_limit=sample_limit,
        ),
        _evaluate_gold_snapshot_quality(
            table_dataframes,
            run_id=run_id,
            pipeline_name=pipeline_name,
            evaluated_at=evaluated_at_value,
            sample_limit=sample_limit,
        ),
        _evaluate_gold_gmv_quality(
            table_dataframes,
            run_id=run_id,
            pipeline_name=pipeline_name,
            evaluated_at=evaluated_at_value,
            sample_limit=sample_limit,
        ),
    ):
        results_records.extend(rule_results)
        quarantine_records.extend(rule_quarantine)

    pipeline_run_log_records = _build_pipeline_run_log_records(
        table_dataframes,
        results_records,
        run_id=run_id,
        pipeline_name=pipeline_name,
        run_started_at=started_at_value,
        evaluated_at=evaluated_at_value,
    )

    return (
        _create_dataframe(spark, pipeline_run_log_records, OPS_PIPELINE_RUN_LOG_SCHEMA),
        _create_dataframe(spark, results_records, OPS_DATA_QUALITY_RESULTS_SCHEMA),
        _create_dataframe(spark, quarantine_records, OPS_DATA_QUALITY_QUARANTINE_SCHEMA),
    )
