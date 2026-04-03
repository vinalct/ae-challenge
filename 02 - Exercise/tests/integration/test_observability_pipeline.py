"""Teste de integracao do fluxo de observabilidade sobre a pipeline local."""

from __future__ import annotations

from datetime import date, datetime

from gold.contracts import (
    GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
    GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
)
from gold.gmv_daily import build_gmv_daily_by_subsidiary_dataframe
from gold.purchase_snapshot import build_purchase_state_snapshot_dataframe
from ops.quality import build_data_quality_outputs
from bronze.contracts import BRONZE_TABLE_SPECS
from silver.contracts import SILVER_TABLE_SPECS
from silver.standardization import build_silver_dataframes


def test_data_quality_outputs_succeed_for_a_clean_end_to_end_flow(bronze_df_builder):
    """Executa bronze, silver, gold e observabilidade com uma massa minima consistente."""
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
                    'transaction_datetime': datetime(2023, 1, 1, 10, 2, 0),
                    'transaction_date': date(2023, 1, 1),
                }
            ],
        ),
        'order_transaction_cost_hist': bronze_df_builder(
            'order_transaction_cost_hist',
            [
                {
                    'purchase_id': 1,
                    'purchase_partition': 1,
                    'order_transaction_cost_vat_value': 2.0,
                    'order_transaction_cost_installment_value': 1.0,
                    'order_transaction_cost_date': date(2023, 1, 1),
                    'transaction_datetime': datetime(2023, 1, 1, 10, 3, 0),
                    'transaction_date': date(2023, 1, 1),
                }
            ],
        ),
    }

    silver_dataframes = build_silver_dataframes(bronze_dataframes)
    snapshot_df = build_purchase_state_snapshot_dataframe(
        silver_dataframes=silver_dataframes,
        snapshot_created_at='2024-01-10 00:00:00',
    )
    gmv_df = build_gmv_daily_by_subsidiary_dataframe(snapshot_df)
    table_dataframes = {
        **{
            spec.target_table: bronze_dataframes[source_name]
            for source_name, spec in BRONZE_TABLE_SPECS.items()
        },
        **{
            spec.target_table: silver_dataframes[source_name]
            for source_name, spec in SILVER_TABLE_SPECS.items()
        },
        GOLD_PURCHASE_STATE_SNAPSHOT_TABLE: snapshot_df,
        GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE: gmv_df,
    }

    pipeline_run_log_df, quality_results_df, quarantine_df = build_data_quality_outputs(
        table_dataframes,
        run_id='integration-run',
        pipeline_name='integration-tests',
        evaluated_at='2024-01-10 00:00:00',
    )

    overall_row = pipeline_run_log_df.where(pipeline_run_log_df.table_name == 'all_tables').first()
    assert overall_row['run_status'] == 'succeeded'
    assert quality_results_df.where(quality_results_df.rule_status == 'failed').count() == 0
    assert quarantine_df.count() == 0
