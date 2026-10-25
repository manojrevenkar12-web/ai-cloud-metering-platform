-- GPU-seconds per customer, GPU type, region and minute of one month, set against committed GPUs.
DELETE FROM gold_gpu_minutes WHERE month = $month;
INSERT INTO gold_gpu_minutes
WITH sessions AS (
  SELECT customer_id, gpu_type, region, gpu_count,
         greatest(started_at, $month_start) AS a, least(billed_until, $month_end) AS b
  FROM silver_vm_sessions
  WHERE started_at < $month_end AND billed_until > $month_start
), minutes AS (
  SELECT customer_id, gpu_type, region, gpu_count, a, b, unnest(generate_series(a // 60, (b - 1) // 60)) AS minute
  FROM sessions WHERE b > a
), usage AS (
  SELECT customer_id, gpu_type, region, minute,
         sum((least(b, (minute + 1) * 60) - greatest(a, minute * 60)) * gpu_count) AS gpu_seconds
  FROM minutes GROUP BY ALL
)
SELECT $month AS month, u.customer_id, u.gpu_type, u.region, u.minute, u.gpu_seconds,
       coalesce(sum(c.gpus), 0) * 60 AS committed_seconds,
       greatest(0, u.gpu_seconds - coalesce(sum(c.gpus), 0) * 60) AS overage_seconds,
       any_value(p.usd) AS usd_per_gpu_hour
FROM usage u
LEFT JOIN silver_commitment_terms c
  ON c.customer_id = u.customer_id AND c.gpu_type = u.gpu_type AND c.region = u.region
 AND u.minute * 60 >= c.starts AND u.minute * 60 < c.ends
LEFT JOIN ref_prices p
  ON p.sku = 'gpu:' || u.gpu_type || ':' || u.region AND u.minute * 60 >= p.valid_from AND u.minute * 60 < p.valid_to
GROUP BY u.customer_id, u.gpu_type, u.region, u.minute, u.gpu_seconds;
