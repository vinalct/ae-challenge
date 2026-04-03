"""Contratos das tabelas bronze e seus metadados minimos."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.types import StructType

from common.source_schemas import SOURCE_SCHEMAS


@dataclass(frozen=True)
class BronzeTableSpec:
    """Define o contrato minimo de uma tabela bronze."""

    source_name: str
    target_table: str
    schema: StructType
    partition_field: str = 'transaction_date'
    metadata_columns: tuple[str, ...] = (
        'ingestion_ts',
        'batch_id',
        'source_file',
        'record_hash',
    )


BRONZE_TABLE_SPECS = {
    'purchase': BronzeTableSpec(
        source_name='purchase',
        target_table='bronze_purchase_events',
        schema=SOURCE_SCHEMAS['purchase'],
    ),
    'product_item': BronzeTableSpec(
        source_name='product_item',
        target_table='bronze_product_item_events',
        schema=SOURCE_SCHEMAS['product_item'],
    ),
    'purchase_extra_info': BronzeTableSpec(
        source_name='purchase_extra_info',
        target_table='bronze_purchase_extra_info_events',
        schema=SOURCE_SCHEMAS['purchase_extra_info'],
    ),
    'order_transaction_cost_hist': BronzeTableSpec(
        source_name='order_transaction_cost_hist',
        target_table='bronze_order_transaction_cost_hist_events',
        schema=SOURCE_SCHEMAS['order_transaction_cost_hist'],
    ),
}


def get_bronze_table_spec(source_name: str) -> BronzeTableSpec:
    """Retorna o contrato bronze para a origem informada."""
    try:
        return BRONZE_TABLE_SPECS[source_name]
    except KeyError as exc:
        supported = ', '.join(sorted(BRONZE_TABLE_SPECS))
        raise ValueError(
            f"Unsupported bronze source '{source_name}'. Expected one of: {supported}."
        ) from exc
