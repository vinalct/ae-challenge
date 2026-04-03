"""Contratos e nomes das tabelas gold do snapshot de compra."""

from __future__ import annotations

GOLD_PURCHASE_STATE_SNAPSHOT_TABLE = 'gold_purchase_state_snapshot'
GOLD_GMV_DAILY_BY_SUBSIDIARY_SNAPSHOT_TABLE = 'gold_gmv_daily_by_subsidiary_snapshot'
GOLD_GMV_DAILY_BY_SUBSIDIARY_CURRENT_VIEW = 'vw_gmv_daily_by_subsidiary_current'

SILVER_INPUT_TABLES = {
    'purchase': 'silver_purchase_cdc',
    'product_item': 'silver_product_item_cdc',
    'purchase_extra_info': 'silver_purchase_extra_info_cdc',
    'order_transaction_cost_hist': 'silver_order_transaction_cost_hist_cdc',
}

GOLD_PURCHASE_STATE_SNAPSHOT_COLUMNS = (
    'snapshot_date',
    'snapshot_created_at',
    'quality_status',
    'quality_flags',
    'purchase_id',
    'purchase_partition',
    'buyer_id',
    'producer_id',
    'purchase_status',
    'order_date',
    'release_date',
    'gmv_date',
    'purchase_total_value',
    'prod_item_id',
    'prod_item_partition',
    'product_id',
    'item_quantity',
    'purchase_value',
    'subsidiary',
    'order_transaction_cost_vat_value',
    'order_transaction_cost_installment_value',
    'order_transaction_cost_date',
    'has_purchase',
    'has_product_item',
    'has_extra_info',
    'has_order_transaction_cost_hist',
    'is_metric_eligible',
    'purchase_quality_status',
    'product_item_quality_status',
    'purchase_extra_info_quality_status',
    'order_transaction_cost_hist_quality_status',
    'purchase_source_transaction_datetime',
    'purchase_source_transaction_date',
    'purchase_source_record_hash',
    'product_item_source_transaction_datetime',
    'product_item_source_transaction_date',
    'product_item_source_record_hash',
    'purchase_extra_info_source_transaction_datetime',
    'purchase_extra_info_source_transaction_date',
    'purchase_extra_info_source_record_hash',
    'order_transaction_cost_hist_source_transaction_datetime',
    'order_transaction_cost_hist_source_transaction_date',
    'order_transaction_cost_hist_source_record_hash',
)

GOLD_GMV_DAILY_BY_SUBSIDIARY_COLUMNS = (
    'snapshot_date',
    'gmv_date',
    'subsidiary',
    'gmv_daily_amount',
    'gmv_daily_purchase_count',
    'gmv_daily_item_quantity',
    'gmv_mtd_amount',
    'quality_status',
    'snapshot_created_at',
)
