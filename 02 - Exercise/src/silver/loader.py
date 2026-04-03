"""Funcoes para preparar e carregar a camada silver CDC."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from common.sql import execute_sql_file
from .contracts import SILVER_TABLE_SPECS, get_silver_table_spec
from .standardization import build_silver_dataframes


def build_table_name(namespace: str, table_name: str, catalog: str | None = None) -> str:
    """Monta o nome completo da tabela considerando namespace e catalogo."""
    if catalog:
        return f'{catalog}.{namespace}.{table_name}'
    return f'{namespace}.{table_name}'


def prepare_silver_tables(
    spark: SparkSession,
    ddl_path: str | Path,
    namespace: str | None = None,
    catalog: str | None = None,
    reset_tables: bool = False,
) -> None:
    """Executa o DDL da silver e opcionalmente recria apenas as tabelas da camada."""
    ddl_path_obj = Path(ddl_path)
    if not ddl_path_obj.exists():
        raise FileNotFoundError(f'Arquivo DDL nao encontrado: {ddl_path_obj}')

    if reset_tables:
        if not namespace:
            raise ValueError('namespace must be provided when reset_tables=True.')
        for spec in SILVER_TABLE_SPECS.values():
            full_table_name = build_table_name(
                namespace=namespace,
                table_name=spec.target_table,
                catalog=catalog,
            )
            spark.sql(f'DROP TABLE IF EXISTS {full_table_name}')

    execute_sql_file(spark, ddl_path_obj)


def _read_bronze_dataframes(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
) -> dict[str, DataFrame]:
    """Le as tabelas bronze usadas como entrada da silver."""
    bronze_frames: dict[str, DataFrame] = {}
    for source_name, spec in SILVER_TABLE_SPECS.items():
        bronze_table_name = build_table_name(
            namespace=namespace,
            table_name=spec.bronze_table,
            catalog=catalog,
        )
        bronze_frames[source_name] = spark.table(bronze_table_name)
    return bronze_frames


def _replace_silver_table(
    spark: SparkSession,
    df: DataFrame,
    source_name: str,
    namespace: str,
    catalog: str | None = None,
) -> int:
    """Substitui o conteudo atual da tabela silver de destino."""
    spec = get_silver_table_spec(source_name)
    full_table_name = build_table_name(
        namespace=namespace,
        table_name=spec.target_table,
        catalog=catalog,
    )
    row_count = int(df.count())
    spark.sql(f'DELETE FROM {full_table_name} WHERE 1 = 1')
    df.writeTo(full_table_name).append()
    return row_count


def load_silver_sources(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
) -> dict[str, int]:
    """Materializa todas as tabelas silver CDC a partir da bronze."""
    bronze_dataframes = _read_bronze_dataframes(
        spark=spark,
        namespace=namespace,
        catalog=catalog,
    )
    silver_dataframes = build_silver_dataframes(bronze_dataframes)
    loaded_counts: dict[str, int] = {}
    for source_name, df in silver_dataframes.items():
        loaded_counts[source_name] = _replace_silver_table(
            spark=spark,
            df=df,
            source_name=source_name,
            namespace=namespace,
            catalog=catalog,
        )
    return loaded_counts
