"""Carregamento das tabelas operacionais de qualidade e observabilidade."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession

from bronze.contracts import BRONZE_TABLE_SPECS
from common.sql import execute_sql_file
from gold.contracts import (
    GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
    GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
)
from silver.contracts import SILVER_TABLE_SPECS
from .contracts import (
    DEFAULT_DATA_QUALITY_PIPELINE_NAME,
    DEFAULT_SAMPLE_LIMIT,
    OPS_DATA_QUALITY_QUARANTINE_TABLE,
    OPS_DATA_QUALITY_RESULTS_TABLE,
    OPS_PIPELINE_RUN_LOG_TABLE,
    RULE_STATUS_FAILED,
    RUN_STATUS_FAILED,
)
from .quality import build_data_quality_outputs


@dataclass(frozen=True)
class DataQualityLoadSummary:
    """Resumo da publicacao dos artefatos operacionais."""

    run_id: str
    pipeline_status: str
    total_checks: int
    failed_error_checks: int
    failed_warning_checks: int
    quarantine_rows: int


def build_table_name(namespace: str, table_name: str, catalog: str | None = None) -> str:
    """Monta o nome completo da tabela considerando namespace e catalogo."""
    if catalog:
        return f'{catalog}.{namespace}.{table_name}'
    return f'{namespace}.{table_name}'


def prepare_ops_tables(
    spark: SparkSession,
    ddl_path: str | Path,
    namespace: str | None = None,
    catalog: str | None = None,
    reset_tables: bool = False,
) -> None:
    """Executa o DDL operacional e opcionalmente recria as tabelas OPS."""
    ddl_path_obj = Path(ddl_path)
    if not ddl_path_obj.exists():
        raise FileNotFoundError(f'Arquivo DDL nao encontrado: {ddl_path_obj}')

    if reset_tables:
        if not namespace:
            raise ValueError('namespace must be provided when reset_tables=True.')
        for table_name in (
            OPS_PIPELINE_RUN_LOG_TABLE,
            OPS_DATA_QUALITY_RESULTS_TABLE,
            OPS_DATA_QUALITY_QUARANTINE_TABLE,
        ):
            spark.sql(f'DROP TABLE IF EXISTS {build_table_name(namespace, table_name, catalog)}')

    execute_sql_file(spark, ddl_path_obj)


def _read_quality_inputs(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
) -> dict[str, DataFrame]:
    """Le as tabelas bronze, silver e gold usadas na avaliacao de qualidade."""
    table_dataframes: dict[str, DataFrame] = {}
    for spec in BRONZE_TABLE_SPECS.values():
        table_dataframes[spec.target_table] = spark.table(
            build_table_name(namespace, spec.target_table, catalog)
        )
    for spec in SILVER_TABLE_SPECS.values():
        table_dataframes[spec.target_table] = spark.table(
            build_table_name(namespace, spec.target_table, catalog)
        )
    for table_name in (
        GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
        GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
    ):
        table_dataframes[table_name] = spark.table(build_table_name(namespace, table_name, catalog))
    return table_dataframes


def _append_dataframe(
    df: DataFrame,
    *,
    namespace: str,
    table_name: str,
    catalog: str | None = None,
) -> int:
    """Faz append do dataframe em uma tabela operacional."""
    full_table_name = build_table_name(namespace, table_name, catalog)
    row_count = int(df.count())
    df.writeTo(full_table_name).append()
    return row_count


def load_data_quality_outputs(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
    run_id: str | None = None,
    pipeline_name: str = DEFAULT_DATA_QUALITY_PIPELINE_NAME,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> DataQualityLoadSummary:
    """Avalia as regras de qualidade e publica os artefatos operacionais."""
    resolved_run_id = run_id or uuid4().hex
    table_dataframes = _read_quality_inputs(spark, namespace=namespace, catalog=catalog)
    pipeline_run_log_df, quality_results_df, quarantine_df = build_data_quality_outputs(
        table_dataframes,
        run_id=resolved_run_id,
        pipeline_name=pipeline_name,
        sample_limit=sample_limit,
    )
    _append_dataframe(
        pipeline_run_log_df,
        namespace=namespace,
        table_name=OPS_PIPELINE_RUN_LOG_TABLE,
        catalog=catalog,
    )
    _append_dataframe(
        quality_results_df,
        namespace=namespace,
        table_name=OPS_DATA_QUALITY_RESULTS_TABLE,
        catalog=catalog,
    )
    quarantine_rows = _append_dataframe(
        quarantine_df,
        namespace=namespace,
        table_name=OPS_DATA_QUALITY_QUARANTINE_TABLE,
        catalog=catalog,
    )

    overall_row = (
        pipeline_run_log_df.where(pipeline_run_log_df.table_name == 'all_tables').first()
    )
    failed_error_checks = int(
        quality_results_df.where(
            (quality_results_df.severity == 'error')
            & (quality_results_df.rule_status == RULE_STATUS_FAILED)
        ).count()
    )
    failed_warning_checks = int(
        quality_results_df.where(
            (quality_results_df.severity == 'warning')
            & (quality_results_df.rule_status == RULE_STATUS_FAILED)
        ).count()
    )

    return DataQualityLoadSummary(
        run_id=resolved_run_id,
        pipeline_status=overall_row['run_status'] if overall_row is not None else RUN_STATUS_FAILED,
        total_checks=int(quality_results_df.count()),
        failed_error_checks=failed_error_checks,
        failed_warning_checks=failed_warning_checks,
        quarantine_rows=quarantine_rows,
    )
