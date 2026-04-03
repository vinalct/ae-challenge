"""Schemas canonicos das tabelas de origem usados nas ingestoes."""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PURCHASE_SCHEMA = StructType(
    [
        StructField('purchase_id', LongType(), False),
        StructField('buyer_id', LongType(), True),
        StructField('prod_item_id', LongType(), True),
        StructField('order_date', DateType(), True),
        StructField('release_date', DateType(), True),
        StructField('producer_id', LongType(), True),
        StructField('purchase_partition', LongType(), False),
        StructField('prod_item_partition', LongType(), True),
        StructField('purchase_total_value', DoubleType(), True),
        StructField('purchase_status', StringType(), True),
        StructField('transaction_datetime', TimestampType(), False),
        StructField('transaction_date', DateType(), False),
    ]
)

ORDER_TRANSACTION_COST_HIST_SCHEMA = StructType(
    [
        StructField('purchase_id', LongType(), False),
        StructField('purchase_partition', LongType(), False),
        StructField('order_transaction_cost_vat_value', DoubleType(), True),
        StructField('order_transaction_cost_installment_value', DoubleType(), True),
        StructField('order_transaction_cost_date', DateType(), True),
        StructField('transaction_datetime', TimestampType(), False),
        StructField('transaction_date', DateType(), False),
    ]
)

PRODUCT_ITEM_SCHEMA = StructType(
    [
        StructField('prod_item_id', LongType(), False),
        StructField('prod_item_partition', LongType(), False),
        StructField('product_id', LongType(), True),
        StructField('item_quantity', IntegerType(), True),
        StructField('purchase_value', DoubleType(), True),
        StructField('transaction_datetime', TimestampType(), False),
        StructField('transaction_date', DateType(), False),
    ]
)

PURCHASE_EXTRA_INFO_SCHEMA = StructType(
    [
        StructField('purchase_id', LongType(), False),
        StructField('purchase_partition', LongType(), False),
        StructField('subsidiary', StringType(), True),
        StructField('transaction_datetime', TimestampType(), False),
        StructField('transaction_date', DateType(), False),
    ]
)

SOURCE_SCHEMAS = {
    'purchase': PURCHASE_SCHEMA,
    'order_transaction_cost_hist': ORDER_TRANSACTION_COST_HIST_SCHEMA,
    'product_item': PRODUCT_ITEM_SCHEMA,
    'purchase_extra_info': PURCHASE_EXTRA_INFO_SCHEMA,
}


def get_source_schema(source_name: str) -> StructType:
    """Retorna o schema esperado para uma origem suportada."""
    try:
        return SOURCE_SCHEMAS[source_name]
    except KeyError as exc:
        supported = ', '.join(sorted(SOURCE_SCHEMAS))
        raise ValueError(
            f"Unsupported source '{source_name}'. Expected one of: {supported}."
        ) from exc
