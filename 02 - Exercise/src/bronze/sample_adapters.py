"""Adaptadores da massa de exemplo para o contrato bronze do exercicio."""

from __future__ import annotations

from pathlib import Path
from typing import Final

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.source_schemas import get_source_schema
from .loader import BronzeLoad

SAMPLE_SOURCE_FILES: Final[dict[str, str]] = {
    'purchase': 'purchase.txt',
    'product_item': 'product_item.txt',
    'purchase_extra_info': 'purchase_extra_info.txt',
    'order_transaction_cost_hist': 'order_transaction_cost_hist.txt',
}
OPTIONAL_SAMPLE_SOURCES: Final[frozenset[str]] = frozenset({'order_transaction_cost_hist'})
DEFAULT_SAMPLE_PARTITION: Final[int] = 1


def _read_sample_txt(spark: SparkSession, source_path: Path) -> DataFrame:
    """Le um arquivo txt de exemplo como CSV com cabecalho."""
    return (
        spark.read.option('header', True)
        .option('mode', 'FAILFAST')
        .option('nullValue', 'NULL')
        .csv(str(source_path))
    )


def _align_to_source_schema(
    raw_df: DataFrame,
    source_name: str,
    column_mapping: dict[str, str] | None = None,
    default_values: dict[str, object] | None = None,
) -> DataFrame:
    """Reordena e completa colunas seguindo o schema canonico da origem."""
    schema = get_source_schema(source_name)
    column_mapping = column_mapping or {}
    default_values = default_values or {}

    expressions = []
    for field in schema.fields:
        source_column = column_mapping.get(field.name, field.name)
        if field.name in default_values:
            expression = F.lit(default_values[field.name])
        elif source_column in raw_df.columns:
            expression = F.col(source_column)
        else:
            expression = F.lit(None)
        expressions.append(expression.cast(field.dataType).alias(field.name))

    return raw_df.select(expressions)


def _build_purchase_lookup(purchase_df: DataFrame) -> DataFrame:
    """Mantem o prod_item_id mais recente por purchase_id para a amostra."""
    window = Window.partitionBy('purchase_id').orderBy(
        F.col('transaction_datetime').desc_nulls_last(),
        F.col('transaction_date').desc_nulls_last(),
    )
    return (
        purchase_df.where(F.col('prod_item_id').isNotNull())
        .withColumn('row_number', F.row_number().over(window))
        .where(F.col('row_number') == 1)
        .select('purchase_id', 'prod_item_id')
    )


def _build_purchase_load(data_dir: Path, spark: SparkSession) -> BronzeLoad:
    """Prepara a carga da origem purchase a partir da pasta data."""
    source_path = data_dir / SAMPLE_SOURCE_FILES['purchase']
    if not source_path.exists():
        raise FileNotFoundError(f'Arquivo obrigatorio nao encontrado: {source_path}')

    raw_df = _read_sample_txt(spark, source_path)
    normalized_df = _align_to_source_schema(
        raw_df=raw_df,
        source_name='purchase',
        default_values={
            'purchase_partition': DEFAULT_SAMPLE_PARTITION,
            'prod_item_partition': DEFAULT_SAMPLE_PARTITION,
        },
    )
    return BronzeLoad(source_name='purchase', df=normalized_df, source_file=str(source_path))


def _build_product_item_load(
    data_dir: Path,
    spark: SparkSession,
    purchase_lookup: DataFrame,
) -> BronzeLoad:
    """Prepara a carga da origem product_item usando a amostra simplificada."""
    source_path = data_dir / SAMPLE_SOURCE_FILES['product_item']
    if not source_path.exists():
        raise FileNotFoundError(f'Arquivo obrigatorio nao encontrado: {source_path}')

    raw_df = _read_sample_txt(spark, source_path)
    joined_df = raw_df.join(purchase_lookup, on='purchase_id', how='left')
    normalized_df = _align_to_source_schema(
        raw_df=joined_df,
        source_name='product_item',
        default_values={'prod_item_partition': DEFAULT_SAMPLE_PARTITION},
    )
    if normalized_df.where(F.col('prod_item_id').isNull()).limit(1).count() > 0:
        raise ValueError(
            'Nao foi possivel resolver prod_item_id para todas as linhas de product_item.txt.'
        )
    return BronzeLoad(source_name='product_item', df=normalized_df, source_file=str(source_path))


def _build_purchase_extra_info_load(data_dir: Path, spark: SparkSession) -> BronzeLoad:
    """Prepara a carga da origem purchase_extra_info a partir da amostra."""
    source_path = data_dir / SAMPLE_SOURCE_FILES['purchase_extra_info']
    if not source_path.exists():
        raise FileNotFoundError(f'Arquivo obrigatorio nao encontrado: {source_path}')

    raw_df = _read_sample_txt(spark, source_path)
    normalized_df = _align_to_source_schema(
        raw_df=raw_df,
        source_name='purchase_extra_info',
        default_values={'purchase_partition': DEFAULT_SAMPLE_PARTITION},
    )
    return BronzeLoad(
        source_name='purchase_extra_info',
        df=normalized_df,
        source_file=str(source_path),
    )


def _build_order_transaction_cost_hist_load(
    data_dir: Path,
    spark: SparkSession,
) -> BronzeLoad | None:
    """Prepara a carga opcional de order_transaction_cost_hist."""
    source_path = data_dir / SAMPLE_SOURCE_FILES['order_transaction_cost_hist']
    if not source_path.exists():
        print(
            'Arquivo opcional ausente: '
            f'{source_path}. A carga seguira sem essa origem.'
        )
        return None

    raw_df = _read_sample_txt(spark, source_path)
    normalized_df = _align_to_source_schema(
        raw_df=raw_df,
        source_name='order_transaction_cost_hist',
        default_values={'purchase_partition': DEFAULT_SAMPLE_PARTITION},
    )
    return BronzeLoad(
        source_name='order_transaction_cost_hist',
        df=normalized_df,
        source_file=str(source_path),
    )


def build_sample_bronze_loads(
    spark: SparkSession,
    data_dir: str | Path,
) -> list[BronzeLoad]:
    """Monta as cargas bronze a partir dos arquivos de exemplo da pasta data."""
    data_dir_path = Path(data_dir)
    if not data_dir_path.exists():
        raise FileNotFoundError(f'Pasta de dados nao encontrada: {data_dir_path}')

    purchase_load = _build_purchase_load(data_dir=data_dir_path, spark=spark)
    purchase_lookup = _build_purchase_lookup(purchase_load.df)
    loads = [
        purchase_load,
        _build_product_item_load(
            data_dir=data_dir_path,
            spark=spark,
            purchase_lookup=purchase_lookup,
        ),
        _build_purchase_extra_info_load(data_dir=data_dir_path, spark=spark),
    ]

    optional_load = _build_order_transaction_cost_hist_load(data_dir=data_dir_path, spark=spark)
    if optional_load is not None:
        loads.append(optional_load)

    return loads
