"""Testes focados nas saidas operacionais de qualidade."""

from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import functions as F

from bronze.contracts import BRONZE_TABLE_SPECS
from gold.contracts import (
    GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE,
    GOLD_PURCHASE_STATE_SNAPSHOT_TABLE,
)
from gold.gmv_daily import build_gmv_daily_by_subsidiary_dataframe
from gold.purchase_snapshot import build_purchase_state_snapshot_dataframe
from ops.quality import build_data_quality_outputs
from silver.contracts import SILVER_TABLE_SPECS
from silver.standardization import build_silver_dataframes


def test_quality_outputs_capture_failed_snapshot_contract_and_quarantine_samples(bronze_df_builder):
    """Uma inconsistenca no snapshot precisa aparecer em results e quarantine."""
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
                    'transaction_datetime': datetime(2023, 1, 2, 10, 2, 0),
                    'transaction_date': date(2023, 1, 2),
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
    buggy_snapshot_df = snapshot_df.withColumn(
        'is_metric_eligible',
        F.when(F.col('snapshot_date') == F.lit(date(2023, 1, 1)), F.lit(True)).otherwise(
            F.col('is_metric_eligible')
        ),
    )
    gmv_df = build_gmv_daily_by_subsidiary_dataframe(buggy_snapshot_df)
    table_dataframes = {
        **{
            spec.target_table: bronze_dataframes[source_name]
            for source_name, spec in BRONZE_TABLE_SPECS.items()
        },
        **{
            spec.target_table: silver_dataframes[source_name]
            for source_name, spec in SILVER_TABLE_SPECS.items()
        },
        GOLD_PURCHASE_STATE_SNAPSHOT_TABLE: buggy_snapshot_df,
        GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE: gmv_df,
    }

    _, quality_results_df, quarantine_df = build_data_quality_outputs(
        table_dataframes,
        run_id='dq-run',
        pipeline_name='dq-tests',
        evaluated_at='2024-01-10 00:00:00',
    )

    failed_rule = quality_results_df.where(
        (quality_results_df.table_name == GOLD_PURCHASE_STATE_SNAPSHOT_TABLE)
        & (quality_results_df.rule_name == 'gold_snapshot_metric_eligibility_contract')
    ).first()
    assert failed_rule['rule_status'] == 'failed'
    assert failed_rule['impacted_record_count'] == 1
    assert quarantine_df.where(quarantine_df.rule_name == 'gold_snapshot_metric_eligibility_contract').count() == 1
