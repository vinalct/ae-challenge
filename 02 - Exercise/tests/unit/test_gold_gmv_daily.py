"""Testes unitarios do agregado gold de GMV diario por subsidiaria."""

from __future__ import annotations

from datetime import date, datetime

from chispa.dataframe_comparer import assert_df_equality

from gold.gmv_daily import build_gmv_daily_by_subsidiary_dataframe


def test_gmv_daily_aggregate_uses_only_metric_eligible_rows(spark):
    """Agrega GMV, quantidade, contagem e MTD apenas sobre linhas elegiveis."""
    purchase_snapshot_df = spark.createDataFrame(
        [
            {
                'snapshot_date': date(2023, 1, 2),
                'gmv_date': date(2023, 1, 1),
                'subsidiary': 'nacional',
                'purchase_total_value': 100.0,
                'purchase_id': 1,
                'purchase_partition': 1,
                'item_quantity': 2,
                'snapshot_created_at': datetime(2024, 1, 10, 0, 0, 0),
                'quality_status': 'valid',
                'is_metric_eligible': True,
            },
            {
                'snapshot_date': date(2023, 1, 2),
                'gmv_date': date(2023, 1, 1),
                'subsidiary': 'nacional',
                'purchase_total_value': 50.0,
                'purchase_id': 2,
                'purchase_partition': 1,
                'item_quantity': 1,
                'snapshot_created_at': datetime(2024, 1, 10, 0, 0, 0),
                'quality_status': 'warning',
                'is_metric_eligible': True,
            },
            {
                'snapshot_date': date(2023, 1, 2),
                'gmv_date': date(2023, 1, 2),
                'subsidiary': 'nacional',
                'purchase_total_value': 200.0,
                'purchase_id': 3,
                'purchase_partition': 1,
                'item_quantity': 1,
                'snapshot_created_at': datetime(2024, 1, 10, 0, 0, 0),
                'quality_status': 'valid',
                'is_metric_eligible': True,
            },
            {
                'snapshot_date': date(2023, 1, 2),
                'gmv_date': date(2023, 1, 2),
                'subsidiary': 'nacional',
                'purchase_total_value': 999.0,
                'purchase_id': 4,
                'purchase_partition': 1,
                'item_quantity': 9,
                'snapshot_created_at': datetime(2024, 1, 10, 0, 0, 0),
                'quality_status': 'error',
                'is_metric_eligible': False,
            },
        ]
    )

    aggregated_df = build_gmv_daily_by_subsidiary_dataframe(purchase_snapshot_df).orderBy('gmv_date')
    expected_df = spark.createDataFrame(
        [
            {
                'snapshot_date': date(2023, 1, 2),
                'gmv_date': date(2023, 1, 1),
                'subsidiary': 'nacional',
                'gmv_daily_amount': 150.0,
                'gmv_daily_purchase_count': 2,
                'gmv_daily_item_quantity': 3,
                'gmv_mtd_amount': 150.0,
                'quality_status': 'warning',
                'snapshot_created_at': datetime(2024, 1, 10, 0, 0, 0),
            },
            {
                'snapshot_date': date(2023, 1, 2),
                'gmv_date': date(2023, 1, 2),
                'subsidiary': 'nacional',
                'gmv_daily_amount': 200.0,
                'gmv_daily_purchase_count': 1,
                'gmv_daily_item_quantity': 1,
                'gmv_mtd_amount': 350.0,
                'quality_status': 'valid',
                'snapshot_created_at': datetime(2024, 1, 10, 0, 0, 0),
            },
        ],
        schema=aggregated_df.schema,
    ).orderBy('gmv_date')

    assert_df_equality(aggregated_df, expected_df, ignore_nullable=True, ignore_row_order=False)
