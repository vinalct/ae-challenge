"""Contratos das tabelas silver CDC e seus metadados tecnicos."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import StructType

from common.source_schemas import SOURCE_SCHEMAS

BRONZE_METADATA_COLUMNS = (
    'ingestion_ts',
    'batch_id',
    'source_file',
    'record_hash',
)
SILVER_TECHNICAL_COLUMNS = (
    'resend_duplicate_count',
    'event_count_for_key',
    'event_version_number',
    'event_latest_rank',
    'is_business_key_complete',
    'is_ordering_valid',
)


@dataclass(frozen=True)
class SilverTableSpec:
    """Define o contrato minimo de uma tabela silver."""

    source_name: str
    bronze_table: str
    target_table: str
    schema: StructType
    business_keys: tuple[str, ...]
    reference_columns: tuple[str, ...] = ()

    @property
    def output_columns(self) -> tuple[str, ...]:
        """Retorna a ordem final das colunas publicadas na silver."""
        return (
            tuple(self.schema.fieldNames())
            + BRONZE_METADATA_COLUMNS
            + SILVER_TECHNICAL_COLUMNS
            + self.reference_columns
            + ('quality_status', 'quality_flags')
        )


SILVER_TABLE_SPECS = {
    'purchase': SilverTableSpec(
        source_name='purchase',
        bronze_table='bronze_purchase_events',
        target_table='silver_purchase_cdc',
        schema=SOURCE_SCHEMAS['purchase'],
        business_keys=('purchase_id', 'purchase_partition'),
        reference_columns=('has_product_item_match', 'has_extra_info_match'),
    ),
    'product_item': SilverTableSpec(
        source_name='product_item',
        bronze_table='bronze_product_item_events',
        target_table='silver_product_item_cdc',
        schema=SOURCE_SCHEMAS['product_item'],
        business_keys=('prod_item_id', 'prod_item_partition'),
        reference_columns=('has_purchase_match',),
    ),
    'purchase_extra_info': SilverTableSpec(
        source_name='purchase_extra_info',
        bronze_table='bronze_purchase_extra_info_events',
        target_table='silver_purchase_extra_info_cdc',
        schema=SOURCE_SCHEMAS['purchase_extra_info'],
        business_keys=('purchase_id', 'purchase_partition'),
        reference_columns=('has_purchase_match',),
    ),
    'order_transaction_cost_hist': SilverTableSpec(
        source_name='order_transaction_cost_hist',
        bronze_table='bronze_order_transaction_cost_hist_events',
        target_table='silver_order_transaction_cost_hist_cdc',
        schema=SOURCE_SCHEMAS['order_transaction_cost_hist'],
        business_keys=('purchase_id', 'purchase_partition'),
        reference_columns=('has_purchase_match',),
    ),
}


def get_silver_table_spec(source_name: str) -> SilverTableSpec:
    """Retorna o contrato silver para a origem informada."""
    try:
        return SILVER_TABLE_SPECS[source_name]
    except KeyError as exc:
        supported = ', '.join(sorted(SILVER_TABLE_SPECS))
        raise ValueError(
            f"Unsupported silver source '{source_name}'. Expected one of: {supported}."
        ) from exc
