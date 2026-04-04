WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM ae_challenge.gold_gmv_daily_by_subsidiary_snapshot
)
SELECT
    snapshot_date,
    gmv_date,
    subsidiary,
    gmv_daily_amount,
    gmv_daily_purchase_count,
    gmv_daily_item_quantity,
    gmv_mtd_amount,
    quality_status
FROM ae_challenge.gold_gmv_daily_by_subsidiary_snapshot
WHERE snapshot_date = (SELECT snapshot_date FROM latest_snapshot)
ORDER BY gmv_date, subsidiary;
