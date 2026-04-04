"""Funcoes para preparar e carregar a agregacao final de GMV na gold."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.errors.exceptions.captured import UnsupportedOperationException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.load_mode import FULL_REFRESH_MODE, INCREMENTAL_MODE
from common.sql import execute_sql_file
from .contracts import (
    GOLD_GMV_DAILY_BY_SUBSIDIARY_CURRENT_VIEW,
    GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
    GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
)
from .gmv_daily import build_gmv_daily_by_subsidiary_dataframe


def build_table_name(namespace: str, table_name: str, catalog: str | None = None) -> str:
    """Monta o nome completo da tabela considerando namespace e catalogo."""
    if catalog:
        return f'{catalog}.{namespace}.{table_name}'
    return f'{namespace}.{table_name}'


def build_current_gmv_view_sql(namespace: str, catalog: str | None = None) -> str:
    """Gera o SQL da view de consumo do snapshot mais recente."""
    snapshot_table_name = build_table_name(
        namespace=namespace,
        table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
        catalog=catalog,
    )
    current_view_name = build_table_name(
        namespace=namespace,
        table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_CURRENT_VIEW,
        catalog=catalog,
    )
    return f"""
CREATE VIEW {current_view_name} AS
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM {snapshot_table_name}
)
SELECT aggregate_snapshot.*
FROM {snapshot_table_name} AS aggregate_snapshot
CROSS JOIN latest_snapshot
WHERE aggregate_snapshot.snapshot_date = latest_snapshot.snapshot_date
""".strip()


def refresh_current_gmv_view(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
) -> bool:
    """Recria a view de acesso atual quando o catalogo oferece suporte."""
    current_view_name = build_table_name(
        namespace=namespace,
        table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_CURRENT_VIEW,
        catalog=catalog,
    )
    spark.sql(f'DROP VIEW IF EXISTS {current_view_name}')
    try:
        spark.sql(build_current_gmv_view_sql(namespace=namespace, catalog=catalog))
    except UnsupportedOperationException as exc:
        if 'view' not in str(exc).lower():
            raise
        return False
    return True


def prepare_gmv_daily_by_subsidiary_table(
    spark: SparkSession,
    ddl_path: str | Path,
    namespace: str | None = None,
    catalog: str | None = None,
    reset_objects: bool = False,
) -> None:
    """Executa o DDL da agregacao final e opcionalmente recria tabela e view."""
    ddl_path_obj = Path(ddl_path)
    if not ddl_path_obj.exists():
        raise FileNotFoundError(f'Arquivo DDL nao encontrado: {ddl_path_obj}')

    if reset_objects:
        if not namespace:
            raise ValueError('namespace must be provided when reset_objects=True.')
        current_view_name = build_table_name(
            namespace=namespace,
            table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_CURRENT_VIEW,
            catalog=catalog,
        )
        snapshot_table_name = build_table_name(
            namespace=namespace,
            table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
            catalog=catalog,
        )
        spark.sql(f'DROP VIEW IF EXISTS {current_view_name}')
        spark.sql(f'DROP TABLE IF EXISTS {snapshot_table_name}')

    execute_sql_file(spark, ddl_path_obj)


def _read_purchase_snapshot_dataframe(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
    target_snapshot_date: date | None = None,
) -> DataFrame:
    """Le a tabela gold_purchase_state_snapshot usada como base do agregado final."""
    df = spark.table(
        build_table_name(
            namespace=namespace,
            table_name=GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
            catalog=catalog,
        )
    )
    if target_snapshot_date is None:
        return df
    return df.where(F.col('snapshot_date') == F.lit(target_snapshot_date).cast('date'))


def load_gmv_daily_by_subsidiary(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
    load_mode: str = FULL_REFRESH_MODE,
    target_snapshot_date: date | None = None,
) -> tuple[int, bool]:
    """Materializa a tabela final de GMV e tenta recriar a view do snapshot mais recente."""
    if load_mode == INCREMENTAL_MODE and target_snapshot_date is None:
        raise ValueError('target_snapshot_date is required when load_mode=incremental.')

    purchase_snapshot_df = _read_purchase_snapshot_dataframe(
        spark=spark,
        namespace=namespace,
        catalog=catalog,
        target_snapshot_date=(target_snapshot_date if load_mode == INCREMENTAL_MODE else None),
    )
    gmv_df = build_gmv_daily_by_subsidiary_dataframe(purchase_snapshot_df)
    snapshot_table_name = build_table_name(
        namespace=namespace,
        table_name=GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
        catalog=catalog,
    )
    row_count = int(gmv_df.count())
    if load_mode == INCREMENTAL_MODE:
        spark.sql(
            f"DELETE FROM {snapshot_table_name} WHERE snapshot_date = DATE '{target_snapshot_date.isoformat()}'"
        )
    else:
        spark.sql(f'DELETE FROM {snapshot_table_name} WHERE 1 = 1')

    if row_count > 0:
        gmv_df.writeTo(snapshot_table_name).append()
    current_view_created = refresh_current_gmv_view(
        spark=spark,
        namespace=namespace,
        catalog=catalog,
    )
    return row_count, current_view_created
