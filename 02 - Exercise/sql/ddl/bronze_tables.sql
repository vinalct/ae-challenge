
CREATE NAMESPACE IF NOT EXISTS ae_challenge;

CREATE TABLE IF NOT EXISTS ae_challenge.bronze_purchase_events (
    purchase_id BIGINT,
    buyer_id BIGINT,
    prod_item_id BIGINT,
    order_date DATE,
    release_date DATE,
    producer_id BIGINT,
    purchase_partition BIGINT,
    prod_item_partition BIGINT,
    purchase_total_value DOUBLE,
    purchase_status STRING,
    transaction_datetime TIMESTAMP,
    transaction_date DATE,
    ingestion_ts TIMESTAMP,
    batch_id STRING,
    source_file STRING,
    record_hash STRING
)
USING iceberg
PARTITIONED BY (days(transaction_date));

CREATE TABLE IF NOT EXISTS ae_challenge.bronze_product_item_events (
    prod_item_id BIGINT,
    prod_item_partition BIGINT,
    product_id BIGINT,
    item_quantity INT,
    purchase_value DOUBLE,
    transaction_datetime TIMESTAMP,
    transaction_date DATE,
    ingestion_ts TIMESTAMP,
    batch_id STRING,
    source_file STRING,
    record_hash STRING
)
USING iceberg
PARTITIONED BY (days(transaction_date));

CREATE TABLE IF NOT EXISTS ae_challenge.bronze_purchase_extra_info_events (
    purchase_id BIGINT,
    purchase_partition BIGINT,
    subsidiary STRING,
    transaction_datetime TIMESTAMP,
    transaction_date DATE,
    ingestion_ts TIMESTAMP,
    batch_id STRING,
    source_file STRING,
    record_hash STRING
)
USING iceberg
PARTITIONED BY (days(transaction_date));

CREATE TABLE IF NOT EXISTS ae_challenge.bronze_order_transaction_cost_hist_events (
    purchase_id BIGINT,
    purchase_partition BIGINT,
    order_transaction_cost_vat_value DOUBLE,
    order_transaction_cost_installment_value DOUBLE,
    order_transaction_cost_date DATE,
    transaction_datetime TIMESTAMP,
    transaction_date DATE,
    ingestion_ts TIMESTAMP,
    batch_id STRING,
    source_file STRING,
    record_hash STRING
)
USING iceberg
PARTITIONED BY (days(transaction_date));
