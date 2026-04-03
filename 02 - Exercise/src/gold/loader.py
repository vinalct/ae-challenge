"""Funcoes para preparar e carregar o snapshot gold de compras."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from common.sql import execute_sql_file
from .contracts import GOLD_PURCHASE_STATE_SNAPSHOT_TABLE, SILVER_INPUT_TABLES
from .purchase_snapshot import build_purchase_state_snapshot_dataframe


def build_table_name(namespace: str, table_name: str, catalog: str | None = None) -> str:
    """Monta o nome completo da tabela considerando namespace e catalogo."""
    if catalog:
        return f'{catalog}.{namespace}.{table_name}'
    return f'{namespace}.{table_name}'


def prepare_purchase_state_snapshot_table(
    spark: SparkSession,
    ddl_path: str | Path,
    namespace: str | None = None,
    catalog: str | None = None,
    reset_table: bool = False,
) -> None:
    """Executa o DDL da gold e opcionalmente recria a tabela do snapshot."""
    ddl_path_obj = Path(ddl_path)
    if not ddl_path_obj.exists():
        raise FileNotFoundError(f'Arquivo DDL nao encontrado: {ddl_path_obj}')

    if reset_table:
        if not namespace:
            raise ValueError('namespace must be provided when reset_table=True.')
        full_table_name = build_table_name(
            namespace=namespace,
            table_name=GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
            catalog=catalog,
        )
        spark.sql(f'DROP TABLE IF EXISTS {full_table_name}')

    execute_sql_file(spark, ddl_path_obj)


def _read_silver_dataframes(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
) -> dict[str, DataFrame]:
    """Le as tabelas silver que alimentam o snapshot gold."""
    silver_frames: dict[str, DataFrame] = {}
    for source_name, table_name in SILVER_INPUT_TABLES.items():
        silver_frames[source_name] = spark.table(
            build_table_name(namespace=namespace, table_name=table_name, catalog=catalog)
        )
    return silver_frames


def load_purchase_state_snapshot(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
    snapshot_created_at: str | None = None,
) -> int:
    """Materializa o snapshot gold de estado da compra a partir da silver."""
    silver_dataframes = _read_silver_dataframes(
        spark=spark,
        namespace=namespace,
        catalog=catalog,
    )
    snapshot_df = build_purchase_state_snapshot_dataframe(
        silver_dataframes=silver_dataframes,
        snapshot_created_at=snapshot_created_at,
    )
    full_table_name = build_table_name(
        namespace=namespace,
        table_name=GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
        catalog=catalog,
    )
    row_count = int(snapshot_df.count())
    spark.sql(f'DELETE FROM {full_table_name} WHERE 1 = 1')
    snapshot_df.writeTo(full_table_name).append()
    return row_count
