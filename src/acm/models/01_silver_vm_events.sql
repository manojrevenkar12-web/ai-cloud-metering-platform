-- Lifecycle events, one row per event_id (retries removed: the first delivery wins).
CREATE OR REPLACE TABLE silver_vm_events AS
SELECT event_id, vm_id, customer_id, project_id, event_type, event_time_ts AS event_ts, region, gpu_type,
       gpu_count, host_id, schema_version, _file AS source_file, _visible_at AS visible_at
FROM (
  SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY _visible_at, _file) AS rn
  FROM bronze_vm_lifecycle
) WHERE rn = 1;
