"""Testes unitarios do snapshot gold de compras."""

from __future__ import annotations

from datetime import date, datetime

from chispa.dataframe_comparer import assert_df_equality

from gold.purchase_snapshot import build_purchase_state_snapshot_dataframe
from silver.standardization import build_silver_dataframes


def test_purchase_snapshot_carries_forward_and_applies_metric_eligibility(bronze_df_builder):
    """A compra so se torna elegivel quando a informacao extra chega ao snapshot."""
    bronze_dataframes = {
        'purchase': bronze_df_builder(
            'purchase',
            [
                {
                    'purchase_id': 1,
                    'buyer_id': 10,
                    'prod_item_id': 100,
                    'order_date': date(2023, 1, 1),
                    'release_date': date(2023, 1, 1),
                    'producer_id': 20,
                    'purchase_partition': 1,
                    'prod_item_partition': 1,
                    'purchase_total_value': 50.0,
                    'purchase_status': 'APROVADA',
                    'transaction_datetime': datetime(2023, 1, 1, 10, 0, 0),
                    'transaction_date': date(2023, 1, 1),
                }
            ],
        ),
        'product_item': bronze_df_builder(
            'product_item',
            [
                {
                    'prod_item_id': 100,
                    'prod_item_partition': 1,
                    'product_id': 900,
                    'item_quantity': 1,
                    'purchase_value': 50.0,
                    'transaction_datetime': datetime(2023, 1, 1, 10, 1, 0),
                    'transaction_date': date(2023, 1, 1),
                }
            ],
        ),
        'purchase_extra_info': bronze_df_builder(
            'purchase_extra_info',
            [
                {
                    'purchase_id': 1,
                    'purchase_partition': 1,
                    'subsidiary': 'nacional',
                    'transaction_datetime': datetime(2023, 1, 2, 8, 0, 0),
                    'transaction_date': date(2023, 1, 2),
                }
            ],
        ),
        'order_transaction_cost_hist': bronze_df_builder('order_transaction_cost_hist', []),
    }

    silver_dataframes = build_silver_dataframes(bronze_dataframes)
    snapshot_df = (
        build_purchase_state_snapshot_dataframe(
            silver_dataframes=silver_dataframes,
            snapshot_created_at='2024-01-10 00:00:00',
        )
        .select('snapshot_date', 'purchase_id', 'has_extra_info', 'is_metric_eligible')
        .orderBy('snapshot_date')
    )

    expected_df = snapshot_df.sparkSession.createDataFrame(
        [
            (date(2023, 1, 1), 1, False, False),
            (date(2023, 1, 2), 1, True, True),
        ],
        schema=snapshot_df.schema,
    )

    assert_df_equality(snapshot_df, expected_df, ignore_nullable=True, ignore_row_order=False)
