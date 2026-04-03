"""Testes unitarios da camada silver CDC."""

from __future__ import annotations

from datetime import date, datetime

from chispa.dataframe_comparer import assert_df_equality

from silver.standardization import build_silver_dataframes


def test_build_silver_dataframes_deduplicates_exact_resends_and_keeps_versions(bronze_df_builder):
    """Mantem uma linha por resend identico e preserva correcoes como novas versoes."""
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
                    'purchase_status': 'aprovada',
                    'transaction_datetime': datetime(2023, 1, 1, 10, 0, 0),
                    'transaction_date': date(2023, 1, 1),
                    'record_hash': 'dup-hash',
                },
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
                    'purchase_status': 'aprovada',
                    'transaction_datetime': datetime(2023, 1, 1, 10, 0, 0),
                    'transaction_date': date(2023, 1, 1),
                    'record_hash': 'dup-hash',
                },
                {
                    'purchase_id': 1,
                    'buyer_id': 11,
                    'prod_item_id': 100,
                    'order_date': date(2023, 1, 1),
                    'release_date': date(2023, 1, 1),
                    'producer_id': 20,
                    'purchase_partition': 1,
                    'prod_item_partition': 1,
                    'purchase_total_value': 50.0,
                    'purchase_status': 'APROVADA',
                    'transaction_datetime': datetime(2023, 1, 2, 9, 0, 0),
                    'transaction_date': date(2023, 1, 2),
                    'record_hash': 'corr-hash',
                },
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
                    'subsidiary': 'Nacional',
                    'transaction_datetime': datetime(2023, 1, 1, 10, 2, 0),
                    'transaction_date': date(2023, 1, 1),
                }
            ],
        ),
        'order_transaction_cost_hist': bronze_df_builder('order_transaction_cost_hist', []),
    }

    silver_purchase_df = (
        build_silver_dataframes(bronze_dataframes)['purchase']
        .select(
            'purchase_id',
            'purchase_partition',
            'buyer_id',
            'purchase_status',
            'record_hash',
            'resend_duplicate_count',
            'event_version_number',
            'event_latest_rank',
        )
        .orderBy('event_version_number')
    )

    expected_df = silver_purchase_df.sparkSession.createDataFrame(
        [
            (1, 1, 10, 'APROVADA', 'dup-hash', 2, 1, 2),
            (1, 1, 11, 'APROVADA', 'corr-hash', 1, 2, 1),
        ],
        schema=silver_purchase_df.schema,
    )

    assert_df_equality(silver_purchase_df, expected_df, ignore_nullable=True, ignore_row_order=False)
