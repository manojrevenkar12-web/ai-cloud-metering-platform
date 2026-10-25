-- Heartbeats, one per (vm_id, seq): an agent that restarts re-sends its spool under new event_ids.
-- Host clocks drift, so time comes from the schedule: the agent beats every 300 s from the VM's
-- start (control plane, true time). The host's own clock is kept only to measure its skew.
CREATE OR REPLACE TABLE silver_heartbeats AS
SELECT h.vm_id, h.seq, v.customer_id, v.region, v.gpu_type, v.gpu_count, h.host_id,
       v.started_at + h.seq * 300 AS beat_ts,
       h.host_time_ts - (v.started_at + h.seq * 300) AS host_skew_s,
       h.gpu_util_pct, h.gpu_mem_used_gb, h.event_id, h._visible_at AS visible_at, h.copies
FROM (
  SELECT *, count(*) OVER (PARTITION BY vm_id, seq) AS copies,
         row_number() OVER (PARTITION BY vm_id, seq ORDER BY _visible_at, event_id) AS rn
  FROM bronze_gpu_heartbeat
) h
JOIN silver_vms v USING (vm_id)
WHERE h.rn = 1 AND v.started_at IS NOT NULL;
