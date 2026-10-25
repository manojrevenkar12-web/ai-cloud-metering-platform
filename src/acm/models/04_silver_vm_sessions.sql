-- Billable session per VM (docs/BILLING_RULES.md): from `started` to `stopped`, at least 60 s.
-- No stop event: the earlier of `deleted` and one heartbeat period after the last heartbeat
-- (end_source records which). Still running: up to the watermark, not yet final.
CREATE OR REPLACE TABLE silver_vm_sessions AS
WITH hb AS (
  SELECT vm_id, max(beat_ts) AS last_beat, count(*) AS beats, sum(copies) - count(*) AS duplicate_beats,
         median(host_skew_s) AS host_skew_s
  FROM silver_heartbeats GROUP BY vm_id
), s AS (
  SELECT v.*, hb.last_beat, coalesce(hb.beats, 0) AS beats, coalesce(hb.duplicate_beats, 0) AS duplicate_beats,
         hb.host_skew_s,
         CASE
           WHEN v.stopped_at IS NOT NULL THEN 'stop_event'
           WHEN v.deleted_at IS NOT NULL OR hb.last_beat + 300 + $grace < $now THEN 'inferred'
           ELSE 'running'
         END AS end_source
  FROM silver_vms v LEFT JOIN hb USING (vm_id)
  WHERE v.started_at IS NOT NULL
)
SELECT *,
       CASE end_source
         WHEN 'stop_event' THEN greatest(stopped_at, started_at + 60)
         WHEN 'inferred' THEN greatest(least(coalesce(deleted_at, last_beat + 300), coalesce(last_beat, started_at) + 300),
                                       started_at + 60)
         ELSE $now
       END AS billed_until
FROM s;
