"""Construcao do snapshot diario de estado da compra na camada gold."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from .contracts import GOLD_PURCHASE_STATE_SNAPSHOT_COLUMNS

QUALITY_STATUS_VALID = 'valid'
QUALITY_STATUS_WARNING = 'warning'
QUALITY_STATUS_ERROR = 'error'


def _empty_string_array() -> Column:
    """Retorna um array<string> vazio sem criar expressoes PySpark no import do modulo."""
    return F.expr('CAST(array() AS array<string>)')


def _valid_snapshot_input(df: DataFrame) -> DataFrame:
    """Mantem somente registros aptos para ordenacao as-of no snapshot."""
    return df.where(F.col('is_business_key_complete') & F.col('is_ordering_valid'))


def _prefix_columns(df: DataFrame, prefix: str) -> DataFrame:
    """Aplica um prefixo para evitar colisao de nomes em joins sucessivos."""
    return df.select([F.col(column_name).alias(f'{prefix}{column_name}') for column_name in df.columns])


def _build_snapshot_calendar(silver_dataframes: dict[str, DataFrame]) -> DataFrame:
    """Cria o calendario diario entre a menor e a maior transaction_date disponiveis."""
    date_dataframes = [
        _valid_snapshot_input(df)
        .where(F.col('transaction_date').isNotNull())
        .select('transaction_date')
        for df in silver_dataframes.values()
    ]
    all_dates_df = reduce(lambda left, right: left.unionByName(right), date_dataframes)
    bounds_df = all_dates_df.agg(
        F.min('transaction_date').alias('min_snapshot_date'),
        F.max('transaction_date').alias('max_snapshot_date'),
    )
    return bounds_df.select(
        F.explode(F.sequence('min_snapshot_date', 'max_snapshot_date')).alias('snapshot_date')
    )


def _build_purchase_snapshot_base(
    purchase_df: DataFrame,
    snapshot_calendar_df: DataFrame,
) -> DataFrame:
    """Expande cada compra conhecida para todos os snapshot_dates a partir do seu primeiro dia."""
    purchase_first_seen_df = purchase_df.groupBy('purchase_id', 'purchase_partition').agg(
        F.min('transaction_date').alias('first_snapshot_date')
    )
    return (
        purchase_first_seen_df.crossJoin(snapshot_calendar_df)
        .where(F.col('snapshot_date') >= F.col('first_snapshot_date'))
        .select('snapshot_date', 'purchase_id', 'purchase_partition')
    )


def _latest_as_of_snapshot(
    base_df: DataFrame,
    source_df: DataFrame,
    join_condition: Column,
    row_number_prefix: str,
) -> DataFrame:
    """Seleciona a linha mais recente disponivel para cada chave do snapshot."""
    latest_window = Window.partitionBy('snapshot_date', 'purchase_id', 'purchase_partition').orderBy(
        F.col(f'{row_number_prefix}transaction_datetime').desc_nulls_last(),
        F.col(f'{row_number_prefix}transaction_date').desc_nulls_last(),
        F.col(f'{row_number_prefix}ingestion_ts').desc_nulls_last(),
        F.col(f'{row_number_prefix}record_hash').desc_nulls_last(),
    )
    return (
        base_df.join(source_df, on=join_condition, how='left')
        .withColumn('_latest_row_number', F.row_number().over(latest_window))
        .where(F.col('_latest_row_number') == 1)
        .drop('_latest_row_number')
    )


def _quality_flags_array(flag_definitions: list[tuple[str, Column]]) -> Column:
    """Materializa um array ordenado de flags geradas no snapshot gold."""
    raw_flags = F.array(
        *[F.when(condition, F.lit(flag_name)) for flag_name, condition in flag_definitions]
    )
    return F.sort_array(F.array_distinct(F.filter(raw_flags, lambda flag: flag.isNotNull())))


def _prefixed_quality_flags(column_name: str, prefix: str) -> Column:
    """Prefixa flags da silver para preservar a origem da anomalia na gold."""
    return F.transform(
        F.coalesce(F.col(column_name), _empty_string_array()),
        lambda flag: F.concat(F.lit(f'{prefix}:'), flag),
    )


def _merge_flag_arrays(*arrays: Column) -> Column:
    """Concatena multiplos arrays de flags preservando unicidade e ordem."""
    return F.sort_array(F.array_distinct(F.flatten(F.array(*arrays))))


def _add_gold_quality_columns(df: DataFrame) -> DataFrame:
    """Resume integridade e elegibilidade do estado montado da compra."""
    has_purchase = F.col('purchase_purchase_id').isNotNull()
    has_product_item = F.col('product_item_prod_item_id').isNotNull()
    has_extra_info = F.col('purchase_extra_info_purchase_id').isNotNull()
    has_order_transaction_cost_hist = F.col('order_transaction_cost_hist_purchase_id').isNotNull()

    derived_flags = _quality_flags_array(
        [
            ('missing_purchase_snapshot', ~has_purchase),
            ('missing_product_item_snapshot', ~has_product_item),
            ('missing_purchase_extra_info_snapshot', ~has_extra_info),
            ('missing_subsidiary_snapshot', has_extra_info & F.col('purchase_extra_info_subsidiary').isNull()),
            ('missing_purchase_status_snapshot', F.col('purchase_purchase_status').isNull()),
            (
                'metric_ineligible_purchase_status',
                F.col('purchase_purchase_status').isNotNull()
                & (F.col('purchase_purchase_status') != 'APROVADA'),
            ),
            ('missing_release_date_snapshot', F.col('purchase_release_date').isNull()),
            ('missing_purchase_total_value_snapshot', F.col('purchase_purchase_total_value').isNull()),
            (
                'negative_purchase_total_value_snapshot',
                F.col('purchase_purchase_total_value').isNotNull()
                & (F.col('purchase_purchase_total_value') < 0),
            ),
        ]
    )
    combined_flags = _merge_flag_arrays(
        _prefixed_quality_flags('purchase_quality_flags', 'purchase'),
        _prefixed_quality_flags('product_item_quality_flags', 'product_item'),
        _prefixed_quality_flags('purchase_extra_info_quality_flags', 'purchase_extra_info'),
        _prefixed_quality_flags(
            'order_transaction_cost_hist_quality_flags',
            'order_transaction_cost_hist',
        ),
        derived_flags,
    )

    blocking_issue = (
        (F.col('purchase_quality_status') == QUALITY_STATUS_ERROR)
        | (F.col('product_item_quality_status') == QUALITY_STATUS_ERROR)
        | (F.col('purchase_extra_info_quality_status') == QUALITY_STATUS_ERROR)
        | (F.col('order_transaction_cost_hist_quality_status') == QUALITY_STATUS_ERROR)
        | (~has_purchase)
    )
    is_metric_eligible = (
        has_purchase
        & has_product_item
        & has_extra_info
        & F.col('purchase_extra_info_subsidiary').isNotNull()
        & (F.col('purchase_purchase_status') == 'APROVADA')
        & F.col('purchase_release_date').isNotNull()
        & F.col('purchase_purchase_total_value').isNotNull()
        & (F.col('purchase_purchase_total_value') >= 0)
        & (F.col('purchase_quality_status') != QUALITY_STATUS_ERROR)
        & (
            F.col('product_item_quality_status').isNull()
            | (F.col('product_item_quality_status') != QUALITY_STATUS_ERROR)
        )
        & (
            F.col('purchase_extra_info_quality_status').isNull()
            | (F.col('purchase_extra_info_quality_status') != QUALITY_STATUS_ERROR)
        )
    )

    return (
        df.withColumn('has_purchase', has_purchase)
        .withColumn('has_product_item', has_product_item)
        .withColumn('has_extra_info', has_extra_info)
        .withColumn('has_order_transaction_cost_hist', has_order_transaction_cost_hist)
        .withColumn('is_metric_eligible', is_metric_eligible)
        .withColumn('quality_flags', combined_flags)
        .withColumn(
            'quality_status',
            F.when(blocking_issue, F.lit(QUALITY_STATUS_ERROR))
            .when(F.size(combined_flags) > 0, F.lit(QUALITY_STATUS_WARNING))
            .otherwise(F.lit(QUALITY_STATUS_VALID)),
        )
    )


def build_purchase_state_snapshot_dataframe(
    silver_dataframes: dict[str, DataFrame],
    snapshot_created_at: str | None = None,
) -> DataFrame:
    """Monta o snapshot historico diario por compra a partir da silver CDC."""
    purchase_input_df = _valid_snapshot_input(silver_dataframes['purchase'])
    snapshot_calendar_df = _build_snapshot_calendar(silver_dataframes)
    purchase_snapshot_base_df = _build_purchase_snapshot_base(
        purchase_df=purchase_input_df,
        snapshot_calendar_df=snapshot_calendar_df,
    )

    purchase_snapshot_df = _latest_as_of_snapshot(
        base_df=purchase_snapshot_base_df,
        source_df=_prefix_columns(purchase_input_df, 'purchase_'),
        join_condition=(
            (F.col('purchase_id') == F.col('purchase_purchase_id'))
            & (F.col('purchase_partition') == F.col('purchase_purchase_partition'))
            & (F.col('purchase_transaction_date') <= F.col('snapshot_date'))
        ),
        row_number_prefix='purchase_',
    )
    purchase_extra_info_snapshot_df = _latest_as_of_snapshot(
        base_df=purchase_snapshot_df,
        source_df=_prefix_columns(
            _valid_snapshot_input(silver_dataframes['purchase_extra_info']),
            'purchase_extra_info_',
        ),
        join_condition=(
            (F.col('purchase_id') == F.col('purchase_extra_info_purchase_id'))
            & (
                F.col('purchase_partition')
                == F.col('purchase_extra_info_purchase_partition')
            )
            & (F.col('purchase_extra_info_transaction_date') <= F.col('snapshot_date'))
        ),
        row_number_prefix='purchase_extra_info_',
    )
    order_transaction_cost_hist_snapshot_df = _latest_as_of_snapshot(
        base_df=purchase_extra_info_snapshot_df,
        source_df=_prefix_columns(
            _valid_snapshot_input(silver_dataframes['order_transaction_cost_hist']),
            'order_transaction_cost_hist_',
        ),
        join_condition=(
            (F.col('purchase_id') == F.col('order_transaction_cost_hist_purchase_id'))
            & (
                F.col('purchase_partition')
                == F.col('order_transaction_cost_hist_purchase_partition')
            )
            & (
                F.col('order_transaction_cost_hist_transaction_date')
                <= F.col('snapshot_date')
            )
        ),
        row_number_prefix='order_transaction_cost_hist_',
    )
    product_item_snapshot_df = _latest_as_of_snapshot(
        base_df=order_transaction_cost_hist_snapshot_df,
        source_df=_prefix_columns(
            _valid_snapshot_input(silver_dataframes['product_item']),
            'product_item_',
        ),
        join_condition=(
            (F.col('purchase_prod_item_id') == F.col('product_item_prod_item_id'))
            & (
                F.col('purchase_prod_item_partition')
                == F.col('product_item_prod_item_partition')
            )
            & (F.col('product_item_transaction_date') <= F.col('snapshot_date'))
        ),
        row_number_prefix='product_item_',
    )

    snapshot_created_at_expr = (
        F.current_timestamp()
        if snapshot_created_at is None
        else F.lit(snapshot_created_at).cast('timestamp')
    )
    enriched_df = _add_gold_quality_columns(product_item_snapshot_df).withColumn(
        'snapshot_created_at',
        snapshot_created_at_expr,
    )

    final_df = enriched_df.select(
        F.col('snapshot_date'),
        F.col('snapshot_created_at'),
        F.col('quality_status'),
        F.col('quality_flags'),
        F.col('purchase_id'),
        F.col('purchase_partition'),
        F.col('purchase_buyer_id').alias('buyer_id'),
        F.col('purchase_producer_id').alias('producer_id'),
        F.col('purchase_purchase_status').alias('purchase_status'),
        F.col('purchase_order_date').alias('order_date'),
        F.col('purchase_release_date').alias('release_date'),
        F.col('purchase_release_date').alias('gmv_date'),
        F.col('purchase_purchase_total_value').alias('purchase_total_value'),
        F.col('purchase_prod_item_id').alias('prod_item_id'),
        F.col('purchase_prod_item_partition').alias('prod_item_partition'),
        F.col('product_item_product_id').alias('product_id'),
        F.col('product_item_item_quantity').alias('item_quantity'),
        F.col('product_item_purchase_value').alias('purchase_value'),
        F.col('purchase_extra_info_subsidiary').alias('subsidiary'),
        F.col('order_transaction_cost_hist_order_transaction_cost_vat_value').alias(
            'order_transaction_cost_vat_value'
        ),
        F.col(
            'order_transaction_cost_hist_order_transaction_cost_installment_value'
        ).alias('order_transaction_cost_installment_value'),
        F.col('order_transaction_cost_hist_order_transaction_cost_date').alias(
            'order_transaction_cost_date'
        ),
        F.col('has_purchase'),
        F.col('has_product_item'),
        F.col('has_extra_info'),
        F.col('has_order_transaction_cost_hist'),
        F.col('is_metric_eligible'),
        F.col('purchase_quality_status'),
        F.col('product_item_quality_status'),
        F.col('purchase_extra_info_quality_status'),
        F.col('order_transaction_cost_hist_quality_status'),
        F.col('purchase_transaction_datetime').alias('purchase_source_transaction_datetime'),
        F.col('purchase_transaction_date').alias('purchase_source_transaction_date'),
        F.col('purchase_record_hash').alias('purchase_source_record_hash'),
        F.col('product_item_transaction_datetime').alias(
            'product_item_source_transaction_datetime'
        ),
        F.col('product_item_transaction_date').alias('product_item_source_transaction_date'),
        F.col('product_item_record_hash').alias('product_item_source_record_hash'),
        F.col('purchase_extra_info_transaction_datetime').alias(
            'purchase_extra_info_source_transaction_datetime'
        ),
        F.col('purchase_extra_info_transaction_date').alias(
            'purchase_extra_info_source_transaction_date'
        ),
        F.col('purchase_extra_info_record_hash').alias(
            'purchase_extra_info_source_record_hash'
        ),
        F.col('order_transaction_cost_hist_transaction_datetime').alias(
            'order_transaction_cost_hist_source_transaction_datetime'
        ),
        F.col('order_transaction_cost_hist_transaction_date').alias(
            'order_transaction_cost_hist_source_transaction_date'
        ),
        F.col('order_transaction_cost_hist_record_hash').alias(
            'order_transaction_cost_hist_source_record_hash'
        ),
    )
    return final_df.select([F.col(column_name) for column_name in GOLD_PURCHASE_STATE_SNAPSHOT_COLUMNS])
