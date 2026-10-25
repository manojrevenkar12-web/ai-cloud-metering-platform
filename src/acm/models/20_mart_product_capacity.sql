-- Allocation, utilisation and commitments per day, region and GPU type, for capacity planning.
CREATE OR REPLACE TABLE mart_product_capacity AS
WITH alloc AS (
  SELECT CAST(to_timestamp(minute * 60) AS DATE) AS day, region, gpu_type,
         sum(gpu_seconds) / 3600.0 AS allocated_gpu_hours
  FROM gold_gpu_minutes GROUP BY ALL
), busy AS (
  SELECT CAST(to_timestamp(beat_ts) AS DATE) AS day, region, gpu_type,
         sum(gpu_util_pct / 100.0 * gpu_count * 300) / 3600.0 AS busy_gpu_hours,
         sum(gpu_count * 300) / 3600.0 AS beat_gpu_hours
  FROM silver_heartbeats GROUP BY ALL
), days AS (
  SELECT DISTINCT day FROM alloc
), committed AS (
  SELECT d.day, t.region, t.gpu_type,
         sum(t.gpus * (least(t.ends, epoch(d.day + INTERVAL 1 DAY)) - greatest(t.starts, epoch(d.day))) / 3600.0)
           AS committed_gpu_hours
  FROM days d JOIN silver_commitment_terms t
    ON t.starts < epoch(d.day + INTERVAL 1 DAY) AND t.ends > epoch(d.day)
  GROUP BY ALL
)
SELECT a.day, a.region, a.gpu_type, a.allocated_gpu_hours,
       coalesce(b.busy_gpu_hours, 0) AS busy_gpu_hours,
       coalesce(c.committed_gpu_hours, 0) AS committed_gpu_hours,
       least(1.0, coalesce(b.beat_gpu_hours, 0) / nullif(a.allocated_gpu_hours, 0)) AS heartbeat_coverage,
       coalesce(coalesce(b.beat_gpu_hours, 0) / nullif(a.allocated_gpu_hours, 0) < 0.8, true) AS low_coverage
FROM alloc a LEFT JOIN busy b USING (day, region, gpu_type) LEFT JOIN committed c USING (day, region, gpu_type);
