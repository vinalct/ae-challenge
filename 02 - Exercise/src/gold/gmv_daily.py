"""Agregacao final de GMV diario por subsidiaria na camada gold."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .contracts import GOLD_GMV_DAILY_BY_SUBSIDIARY_COLUMNS

QUALITY_STATUS_VALID = 'valid'
QUALITY_STATUS_WARNING = 'warning'
QUALITY_STATUS_ERROR = 'error'


def build_gmv_daily_by_subsidiary_dataframe(purchase_snapshot_df: DataFrame) -> DataFrame:
    """Agrega o snapshot de compras na visao historica diaria de GMV por subsidiaria."""
    eligible_df = purchase_snapshot_df.where(F.col('is_metric_eligible'))

    aggregated_df = eligible_df.groupBy('snapshot_date', 'gmv_date', 'subsidiary').agg(
        F.sum('purchase_total_value').cast('double').alias('gmv_daily_amount'),
        F.count_distinct('purchase_id', 'purchase_partition').cast('long').alias(
            'gmv_daily_purchase_count'
        ),
        F.sum(F.coalesce(F.col('item_quantity').cast('long'), F.lit(0))).cast('long').alias(
            'gmv_daily_item_quantity'
        ),
        F.max('snapshot_created_at').alias('snapshot_created_at'),
        F.max(
            F.when(F.col('quality_status') == QUALITY_STATUS_ERROR, F.lit(1)).otherwise(F.lit(0))
        ).alias('_has_error'),
        F.max(
            F.when(F.col('quality_status') == QUALITY_STATUS_WARNING, F.lit(1)).otherwise(F.lit(0))
        ).alias('_has_warning'),
    )

    month_window = Window.partitionBy(
        'snapshot_date',
        'subsidiary',
        '_gmv_month_start',
    ).orderBy('gmv_date').rowsBetween(Window.unboundedPreceding, Window.currentRow)

    final_df = (
        aggregated_df.withColumn('_gmv_month_start', F.trunc('gmv_date', 'month'))
        .withColumn(
            'quality_status',
            F.when(F.col('_has_error') > 0, F.lit(QUALITY_STATUS_ERROR))
            .when(F.col('_has_warning') > 0, F.lit(QUALITY_STATUS_WARNING))
            .otherwise(F.lit(QUALITY_STATUS_VALID)),
        )
        .withColumn('gmv_mtd_amount', F.sum('gmv_daily_amount').over(month_window).cast('double'))
        .drop('_gmv_month_start', '_has_error', '_has_warning')
    )

    return final_df.select([F.col(column_name) for column_name in GOLD_GMV_DAILY_BY_SUBSIDIARY_COLUMNS])
