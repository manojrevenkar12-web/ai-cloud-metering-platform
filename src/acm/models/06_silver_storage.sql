CREATE OR REPLACE TABLE silver_storage AS
SELECT event_id, bucket_id, customer_id, region, snapshot_time_ts AS snapshot_ts, bytes_stored, _visible_at AS visible_at
FROM (
  SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY _visible_at, _file) AS rn FROM bronze_storage_snapshot
) WHERE rn = 1;
