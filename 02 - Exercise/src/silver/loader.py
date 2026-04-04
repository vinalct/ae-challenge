"""Funcoes para preparar e carregar a camada silver CDC."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.load_mode import FULL_REFRESH_MODE, INCREMENTAL_MODE
from common.sql import execute_sql_file
from .contracts import SILVER_TABLE_SPECS, SilverTableSpec, get_silver_table_spec
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


def _distinct_keys(df: DataFrame, key_columns: tuple[str, ...]) -> DataFrame:
    """Retorna chaves distintas com todos os componentes preenchidos."""
    condition = F.lit(True)
    for column_name in key_columns:
        condition = condition & F.col(column_name).isNotNull()
    return df.where(condition).select(*key_columns).distinct()


def _union_key_dataframes(*dfs: DataFrame) -> DataFrame:
    """Combina dataframes de chaves homogeneos preservando unicidade."""
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.unionByName(df)
    return combined_df.distinct()


def _bronze_rows_for_date(df: DataFrame, target_transaction_date: date) -> DataFrame:
    """Seleciona os eventos bronze do dia incremental processado."""
    return df.where(F.col('transaction_date') == F.lit(target_transaction_date).cast('date'))


def _build_incremental_key_frames(
    bronze_dataframes: dict[str, DataFrame],
    silver_dataframes: dict[str, DataFrame],
    target_transaction_date: date,
) -> dict[str, DataFrame]:
    """Deriva as chaves impactadas em cada tabela silver a partir de D-1."""
    purchase_events_on_date = _bronze_rows_for_date(
        bronze_dataframes['purchase'],
        target_transaction_date,
    )
    product_item_events_on_date = _bronze_rows_for_date(
        bronze_dataframes['product_item'],
        target_transaction_date,
    )
    purchase_extra_info_events_on_date = _bronze_rows_for_date(
        bronze_dataframes['purchase_extra_info'],
        target_transaction_date,
    )
    order_transaction_cost_hist_events_on_date = _bronze_rows_for_date(
        bronze_dataframes['order_transaction_cost_hist'],
        target_transaction_date,
    )

    purchase_keys_on_date = _distinct_keys(
        purchase_events_on_date,
        ('purchase_id', 'purchase_partition'),
    )
    purchase_extra_info_keys_on_date = _distinct_keys(
        purchase_extra_info_events_on_date,
        ('purchase_id', 'purchase_partition'),
    )
    order_transaction_cost_hist_keys_on_date = _distinct_keys(
        order_transaction_cost_hist_events_on_date,
        ('purchase_id', 'purchase_partition'),
    )
    product_item_keys_on_date = _distinct_keys(
        product_item_events_on_date,
        ('prod_item_id', 'prod_item_partition'),
    )
    product_item_keys_from_purchase_on_date = _distinct_keys(
        purchase_events_on_date,
        ('prod_item_id', 'prod_item_partition'),
    )
    purchase_keys_from_product_item_on_date = (
        silver_dataframes['purchase']
        .select(
            'purchase_id',
            'purchase_partition',
            'prod_item_id',
            'prod_item_partition',
        )
        .where(F.col('prod_item_id').isNotNull() & F.col('prod_item_partition').isNotNull())
        .join(product_item_keys_on_date, on=['prod_item_id', 'prod_item_partition'], how='inner')
        .select('purchase_id', 'purchase_partition')
        .distinct()
    )

    return {
        'purchase': _union_key_dataframes(
            purchase_keys_on_date,
            purchase_extra_info_keys_on_date,
            purchase_keys_from_product_item_on_date,
        ),
        'product_item': _union_key_dataframes(
            product_item_keys_on_date,
            product_item_keys_from_purchase_on_date,
        ),
        'purchase_extra_info': _union_key_dataframes(
            purchase_extra_info_keys_on_date,
            purchase_keys_on_date,
        ),
        'order_transaction_cost_hist': _union_key_dataframes(
            order_transaction_cost_hist_keys_on_date,
            purchase_keys_on_date,
        ),
    }


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
    if row_count > 0:
        df.writeTo(full_table_name).append()
    return row_count


def _incremental_partition_refresh_df(
    df: DataFrame,
    spec: SilverTableSpec,
    impacted_keys_df: DataFrame,
    target_transaction_date: date,
) -> DataFrame:
    """Seleciona as particoes silver que precisam ser republicadas no incremental."""
    impacted_rows_df = df.join(impacted_keys_df, on=list(spec.business_keys), how='inner')
    invalid_rows_on_date_df = df.where(
        (F.col('transaction_date') == F.lit(target_transaction_date).cast('date'))
        & (~F.col('is_business_key_complete'))
    )
    impacted_partitions_df = (
        impacted_rows_df.unionByName(invalid_rows_on_date_df)
        .where(F.col('transaction_date').isNotNull())
        .select('transaction_date')
        .distinct()
    )
    return df.join(impacted_partitions_df, on='transaction_date', how='semi')


def _incremental_replace_silver_table(
    df: DataFrame,
    source_name: str,
    namespace: str,
    target_transaction_date: date,
    impacted_keys_df: DataFrame,
    catalog: str | None = None,
) -> int:
    """Republica somente as particoes impactadas pelo recorte incremental."""
    spec = get_silver_table_spec(source_name)
    full_table_name = build_table_name(
        namespace=namespace,
        table_name=spec.target_table,
        catalog=catalog,
    )
    rows_to_publish_df = _incremental_partition_refresh_df(
        df=df,
        spec=spec,
        impacted_keys_df=impacted_keys_df,
        target_transaction_date=target_transaction_date,
    )
    row_count = int(rows_to_publish_df.count())
    if row_count == 0:
        return 0
    rows_to_publish_df.writeTo(full_table_name).overwritePartitions()
    return row_count


def load_silver_sources(
    spark: SparkSession,
    namespace: str,
    catalog: str | None = None,
    load_mode: str = FULL_REFRESH_MODE,
    target_transaction_date: date | None = None,
) -> dict[str, int]:
    """Materializa todas as tabelas silver CDC a partir da bronze."""
    bronze_dataframes = _read_bronze_dataframes(
        spark=spark,
        namespace=namespace,
        catalog=catalog,
    )
    silver_dataframes = build_silver_dataframes(bronze_dataframes)

    incremental_key_frames: dict[str, DataFrame] | None = None
    if load_mode == INCREMENTAL_MODE:
        if target_transaction_date is None:
            raise ValueError('target_transaction_date is required when load_mode=incremental.')
        incremental_key_frames = _build_incremental_key_frames(
            bronze_dataframes=bronze_dataframes,
            silver_dataframes=silver_dataframes,
            target_transaction_date=target_transaction_date,
        )

    loaded_counts: dict[str, int] = {}
    for source_name, df in silver_dataframes.items():
        if load_mode == INCREMENTAL_MODE:
            loaded_counts[source_name] = _incremental_replace_silver_table(
                df=df,
                source_name=source_name,
                namespace=namespace,
                target_transaction_date=target_transaction_date,
                impacted_keys_df=incremental_key_frames[source_name],
                catalog=catalog,
            )
        else:
            loaded_counts[source_name] = _replace_silver_table(
                spark=spark,
                df=df,
                source_name=source_name,
                namespace=namespace,
                catalog=catalog,
            )
    return loaded_counts
