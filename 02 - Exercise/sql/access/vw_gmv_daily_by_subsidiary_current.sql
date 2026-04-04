CREATE VIEW ae_challenge.vw_gmv_daily_by_subsidiary_current AS
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS snapshot_date
    FROM ae_challenge.gold_gmv_daily_by_subsidiary_snapshot
)
SELECT aggregate_snapshot.*
FROM ae_challenge.gold_gmv_daily_by_subsidiary_snapshot AS aggregate_snapshot
CROSS JOIN latest_snapshot
WHERE aggregate_snapshot.snapshot_date = latest_snapshot.snapshot_date;
