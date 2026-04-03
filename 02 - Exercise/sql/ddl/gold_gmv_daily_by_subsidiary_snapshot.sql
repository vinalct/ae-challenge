
CREATE NAMESPACE IF NOT EXISTS ae_challenge;

CREATE TABLE IF NOT EXISTS ae_challenge.gold_gmv_daily_by_subsidiary_snapshot (
    snapshot_date DATE,
    gmv_date DATE,
    subsidiary STRING,
    gmv_daily_amount DOUBLE,
    gmv_daily_purchase_count BIGINT,
    gmv_daily_item_quantity BIGINT,
    gmv_mtd_amount DOUBLE,
    quality_status STRING,
    snapshot_created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(snapshot_date));
