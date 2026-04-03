"""Transformacoes da bronze para a silver CDC padronizada."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from .contracts import BRONZE_METADATA_COLUMNS, SilverTableSpec, get_silver_table_spec

QUALITY_STATUS_VALID = 'valid'
QUALITY_STATUS_WARNING = 'warning'
QUALITY_STATUS_ERROR = 'error'
VALID_PURCHASE_STATUSES = (
    'APROVADA',
    'CANCELADA',
    'INICIADA',
    'REEMBOLSADA',
)


def _and_all(expressions: Iterable[Column]) -> Column:
    """Combina multiplas expressoes booleanas com AND."""
    condition = F.lit(True)
    for expression in expressions:
        condition = condition & expression
    return condition


def _or_all(expressions: Iterable[Column]) -> Column:
    """Combina multiplas expressoes booleanas com OR."""
    condition = F.lit(False)
    for expression in expressions:
        condition = condition | expression
    return condition


def _all_not_null(column_names: Iterable[str]) -> Column:
    """Retorna uma expressao que exige valores nao nulos em todas as colunas."""
    return _and_all(F.col(column_name).isNotNull() for column_name in column_names)


def _select_bronze_columns(df: DataFrame, spec: SilverTableSpec) -> DataFrame:
    """Seleciona apenas payload canonico e metadados bronze necessarios."""
    selected_columns = list(spec.schema.fieldNames()) + list(BRONZE_METADATA_COLUMNS)
    return df.select([F.col(column_name).alias(column_name) for column_name in selected_columns])


def _normalize_string(column_name: str, case: str | None = None) -> Column:
    """Aplica trim e normalizacao simples de casing em colunas string."""
    normalized = F.trim(F.col(column_name))
    normalized = F.when(normalized == '', F.lit(None)).otherwise(normalized)
    if case == 'upper':
        normalized = F.upper(normalized)
    elif case == 'lower':
        normalized = F.lower(normalized)
    return normalized


def _deduplicate_exact_resends(df: DataFrame, spec: SilverTableSpec) -> DataFrame:
    """Mantem apenas a primeira observacao de cada payload identico por chave de negocio."""
    dedupe_partition = list(spec.business_keys) + ['record_hash']
    dedupe_group = Window.partitionBy(*dedupe_partition)
    dedupe_window = dedupe_group.orderBy(
        F.col('ingestion_ts').asc_nulls_last(),
        F.col('batch_id').asc_nulls_first(),
        F.col('source_file').asc_nulls_first(),
    )
    return (
        df.withColumn(
            'resend_duplicate_count',
            F.count(F.lit(1)).over(dedupe_group).cast('long'),
        )
        .withColumn('_resend_row_number', F.row_number().over(dedupe_window))
        .where(F.col('_resend_row_number') == 1)
        .drop('_resend_row_number')
    )


def _add_event_ordering(df: DataFrame, spec: SilverTableSpec) -> DataFrame:
    """Adiciona ranking deterministico de versoes por chave de negocio."""
    key_window = Window.partitionBy(*spec.business_keys)
    latest_window = key_window.orderBy(
        F.col('transaction_datetime').desc_nulls_last(),
        F.col('transaction_date').desc_nulls_last(),
        F.col('ingestion_ts').desc_nulls_last(),
        F.col('record_hash').desc_nulls_last(),
    )
    version_window = key_window.orderBy(
        F.col('transaction_datetime').asc_nulls_first(),
        F.col('transaction_date').asc_nulls_first(),
        F.col('ingestion_ts').asc_nulls_first(),
        F.col('record_hash').asc_nulls_first(),
    )
    return (
        df.withColumn('event_count_for_key', F.count(F.lit(1)).over(key_window).cast('long'))
        .withColumn('event_version_number', F.row_number().over(version_window).cast('long'))
        .withColumn('event_latest_rank', F.row_number().over(latest_window).cast('long'))
    )


def _quality_flags_array(flag_definitions: list[tuple[str, Column]]) -> Column:
    """Materializa um array deterministico de flags de qualidade."""
    raw_flags = F.array(
        *[
            F.when(condition, F.lit(flag_name))
            for flag_name, condition in flag_definitions
        ]
    )
    return F.sort_array(F.array_distinct(F.filter(raw_flags, lambda flag: flag.isNotNull())))


def _apply_quality_columns(
    df: DataFrame,
    business_keys: tuple[str, ...],
    extra_flag_definitions: list[tuple[str, Column]],
) -> DataFrame:
    """Adiciona flags de qualidade e um status resumido para a linha CDC."""
    missing_business_key = _or_all(F.col(column_name).isNull() for column_name in business_keys)
    missing_transaction_datetime = F.col('transaction_datetime').isNull()
    missing_transaction_date = F.col('transaction_date').isNull()
    transaction_date_mismatch = (
        F.col('transaction_datetime').isNotNull()
        & F.col('transaction_date').isNotNull()
        & (F.to_date('transaction_datetime') != F.col('transaction_date'))
    )

    flag_definitions = [
        ('missing_business_key', missing_business_key),
        ('missing_transaction_datetime', missing_transaction_datetime),
        ('missing_transaction_date', missing_transaction_date),
        ('transaction_date_mismatch', transaction_date_mismatch),
        *extra_flag_definitions,
    ]
    quality_flags = _quality_flags_array(flag_definitions)
    blocking_issue = missing_business_key | missing_transaction_datetime | missing_transaction_date

    return (
        df.withColumn('is_business_key_complete', ~missing_business_key)
        .withColumn(
            'is_ordering_valid',
            ~(missing_transaction_datetime | missing_transaction_date),
        )
        .withColumn('quality_flags', quality_flags)
        .withColumn(
            'quality_status',
            F.when(blocking_issue, F.lit(QUALITY_STATUS_ERROR))
            .when(F.size(quality_flags) > 0, F.lit(QUALITY_STATUS_WARNING))
            .otherwise(F.lit(QUALITY_STATUS_VALID)),
        )
    )


def _normalize_purchase_df(bronze_df: DataFrame) -> DataFrame:
    """Padroniza valores textuais e preserva a tipagem da origem purchase."""
    spec = get_silver_table_spec('purchase')
    return _select_bronze_columns(bronze_df, spec).withColumn(
        'purchase_status',
        _normalize_string('purchase_status', case='upper'),
    )


def _normalize_product_item_df(bronze_df: DataFrame) -> DataFrame:
    """Seleciona as colunas canonicas da origem product_item."""
    spec = get_silver_table_spec('product_item')
    return _select_bronze_columns(bronze_df, spec)


def _normalize_purchase_extra_info_df(bronze_df: DataFrame) -> DataFrame:
    """Padroniza valores textuais da origem purchase_extra_info."""
    spec = get_silver_table_spec('purchase_extra_info')
    return _select_bronze_columns(bronze_df, spec).withColumn(
        'subsidiary',
        _normalize_string('subsidiary', case='lower'),
    )


def _normalize_order_transaction_cost_hist_df(bronze_df: DataFrame) -> DataFrame:
    """Seleciona as colunas canonicas da origem order_transaction_cost_hist."""
    spec = get_silver_table_spec('order_transaction_cost_hist')
    return _select_bronze_columns(bronze_df, spec)


def _distinct_reference_keys(
    df: DataFrame,
    key_columns: tuple[str, ...],
    indicator_column: str,
) -> DataFrame:
    """Retorna chaves distintas usadas para marcar a existencia de referencias."""
    return (
        df.where(_all_not_null(key_columns))
        .select(*key_columns)
        .distinct()
        .withColumn(indicator_column, F.lit(True))
    )


def _enrich_reference_columns(
    purchase_df: DataFrame,
    product_item_df: DataFrame,
    purchase_extra_info_df: DataFrame,
    order_transaction_cost_hist_df: DataFrame,
) -> dict[str, DataFrame]:
    """Adiciona colunas booleanas de existencia de referencias entre as fontes."""
    product_item_keys = _distinct_reference_keys(
        product_item_df,
        ('prod_item_id', 'prod_item_partition'),
        'has_product_item_match',
    )
    purchase_item_keys = _distinct_reference_keys(
        purchase_df,
        ('prod_item_id', 'prod_item_partition'),
        'has_purchase_match',
    )
    purchase_keys = _distinct_reference_keys(
        purchase_df,
        ('purchase_id', 'purchase_partition'),
        'has_purchase_match',
    )
    extra_info_keys = _distinct_reference_keys(
        purchase_extra_info_df,
        ('purchase_id', 'purchase_partition'),
        'has_extra_info_match',
    )

    purchase_with_refs = (
        purchase_df.join(product_item_keys, on=['prod_item_id', 'prod_item_partition'], how='left')
        .join(extra_info_keys, on=['purchase_id', 'purchase_partition'], how='left')
        .withColumn('has_product_item_match', F.coalesce(F.col('has_product_item_match'), F.lit(False)))
        .withColumn('has_extra_info_match', F.coalesce(F.col('has_extra_info_match'), F.lit(False)))
    )
    product_item_with_refs = (
        product_item_df.join(purchase_item_keys, on=['prod_item_id', 'prod_item_partition'], how='left')
        .withColumn('has_purchase_match', F.coalesce(F.col('has_purchase_match'), F.lit(False)))
    )
    purchase_extra_info_with_refs = (
        purchase_extra_info_df.join(purchase_keys, on=['purchase_id', 'purchase_partition'], how='left')
        .withColumn('has_purchase_match', F.coalesce(F.col('has_purchase_match'), F.lit(False)))
    )
    order_transaction_cost_hist_with_refs = (
        order_transaction_cost_hist_df.join(
            purchase_keys,
            on=['purchase_id', 'purchase_partition'],
            how='left',
        )
        .withColumn('has_purchase_match', F.coalesce(F.col('has_purchase_match'), F.lit(False)))
    )

    return {
        'purchase': purchase_with_refs,
        'product_item': product_item_with_refs,
        'purchase_extra_info': purchase_extra_info_with_refs,
        'order_transaction_cost_hist': order_transaction_cost_hist_with_refs,
    }


def _finalize_purchase_df(df: DataFrame) -> DataFrame:
    """Materializa a versao silver final da origem purchase."""
    spec = get_silver_table_spec('purchase')
    quality_df = _apply_quality_columns(
        df=df,
        business_keys=spec.business_keys,
        extra_flag_definitions=[
            (
                'missing_prod_item_key',
                F.col('prod_item_id').isNull() | F.col('prod_item_partition').isNull(),
            ),
            ('missing_purchase_status', F.col('purchase_status').isNull()),
            (
                'invalid_purchase_status',
                F.col('purchase_status').isNotNull()
                & (~F.col('purchase_status').isin(*VALID_PURCHASE_STATUSES)),
            ),
            ('missing_purchase_total_value', F.col('purchase_total_value').isNull()),
            (
                'negative_purchase_total_value',
                F.col('purchase_total_value').isNotNull() & (F.col('purchase_total_value') < 0),
            ),
            ('missing_release_date', F.col('release_date').isNull()),
            (
                'missing_product_item_reference',
                _all_not_null(('prod_item_id', 'prod_item_partition'))
                & (~F.col('has_product_item_match')),
            ),
            (
                'missing_purchase_extra_info_reference',
                _all_not_null(('purchase_id', 'purchase_partition'))
                & (~F.col('has_extra_info_match')),
            ),
        ],
    )
    return quality_df.select([F.col(column_name) for column_name in spec.output_columns])


def _finalize_product_item_df(df: DataFrame) -> DataFrame:
    """Materializa a versao silver final da origem product_item."""
    spec = get_silver_table_spec('product_item')
    quality_df = _apply_quality_columns(
        df=df,
        business_keys=spec.business_keys,
        extra_flag_definitions=[
            ('missing_product_id', F.col('product_id').isNull()),
            ('missing_item_quantity', F.col('item_quantity').isNull()),
            (
                'non_positive_item_quantity',
                F.col('item_quantity').isNotNull() & (F.col('item_quantity') <= 0),
            ),
            ('missing_purchase_value', F.col('purchase_value').isNull()),
            (
                'negative_purchase_value',
                F.col('purchase_value').isNotNull() & (F.col('purchase_value') < 0),
            ),
            (
                'orphan_product_item_reference',
                _all_not_null(('prod_item_id', 'prod_item_partition'))
                & (~F.col('has_purchase_match')),
            ),
        ],
    )
    return quality_df.select([F.col(column_name) for column_name in spec.output_columns])


def _finalize_purchase_extra_info_df(df: DataFrame) -> DataFrame:
    """Materializa a versao silver final da origem purchase_extra_info."""
    spec = get_silver_table_spec('purchase_extra_info')
    quality_df = _apply_quality_columns(
        df=df,
        business_keys=spec.business_keys,
        extra_flag_definitions=[
            ('missing_subsidiary', F.col('subsidiary').isNull()),
            (
                'orphan_purchase_extra_info_reference',
                _all_not_null(('purchase_id', 'purchase_partition'))
                & (~F.col('has_purchase_match')),
            ),
        ],
    )
    return quality_df.select([F.col(column_name) for column_name in spec.output_columns])


def _finalize_order_transaction_cost_hist_df(df: DataFrame) -> DataFrame:
    """Materializa a versao silver final da origem order_transaction_cost_hist."""
    spec = get_silver_table_spec('order_transaction_cost_hist')
    quality_df = _apply_quality_columns(
        df=df,
        business_keys=spec.business_keys,
        extra_flag_definitions=[
            ('missing_order_transaction_cost_date', F.col('order_transaction_cost_date').isNull()),
            (
                'negative_order_transaction_cost_vat_value',
                F.col('order_transaction_cost_vat_value').isNotNull()
                & (F.col('order_transaction_cost_vat_value') < 0),
            ),
            (
                'negative_order_transaction_cost_installment_value',
                F.col('order_transaction_cost_installment_value').isNotNull()
                & (F.col('order_transaction_cost_installment_value') < 0),
            ),
            (
                'orphan_order_transaction_cost_hist_reference',
                _all_not_null(('purchase_id', 'purchase_partition'))
                & (~F.col('has_purchase_match')),
            ),
        ],
    )
    return quality_df.select([F.col(column_name) for column_name in spec.output_columns])


def build_silver_dataframes(bronze_dataframes: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Transforma os dataframes bronze em dataframes silver prontos para publicacao."""
    normalized = {
        'purchase': _normalize_purchase_df(bronze_dataframes['purchase']),
        'product_item': _normalize_product_item_df(bronze_dataframes['product_item']),
        'purchase_extra_info': _normalize_purchase_extra_info_df(
            bronze_dataframes['purchase_extra_info']
        ),
        'order_transaction_cost_hist': _normalize_order_transaction_cost_hist_df(
            bronze_dataframes['order_transaction_cost_hist']
        ),
    }
    ordered = {
        source_name: _add_event_ordering(
            _deduplicate_exact_resends(df, get_silver_table_spec(source_name)),
            get_silver_table_spec(source_name),
        )
        for source_name, df in normalized.items()
    }
    with_references = _enrich_reference_columns(
        purchase_df=ordered['purchase'],
        product_item_df=ordered['product_item'],
        purchase_extra_info_df=ordered['purchase_extra_info'],
        order_transaction_cost_hist_df=ordered['order_transaction_cost_hist'],
    )
    return {
        'purchase': _finalize_purchase_df(with_references['purchase']),
        'product_item': _finalize_product_item_df(with_references['product_item']),
        'purchase_extra_info': _finalize_purchase_extra_info_df(
            with_references['purchase_extra_info']
        ),
        'order_transaction_cost_hist': _finalize_order_transaction_cost_hist_df(
            with_references['order_transaction_cost_hist']
        ),
    }
