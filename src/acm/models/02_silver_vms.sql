-- One row per VM with what the control plane said about it.
CREATE OR REPLACE TABLE silver_vms AS
SELECT vm_id,
       any_value(customer_id) AS customer_id,
       any_value(project_id) AS project_id,
       any_value(region) AS region,
       any_value(gpu_type) AS gpu_type,
       any_value(gpu_count) AS gpu_count,
       any_value(host_id) AS host_id,
       min(event_ts) FILTER (WHERE event_type = 'created') AS created_at,
       min(event_ts) FILTER (WHERE event_type = 'started') AS started_at,
       min(event_ts) FILTER (WHERE event_type = 'stopped') AS stopped_at,
       min(event_ts) FILTER (WHERE event_type = 'deleted') AS deleted_at,
       count(DISTINCT gpu_type) AS gpu_type_variants
FROM silver_vm_events
GROUP BY vm_id;
