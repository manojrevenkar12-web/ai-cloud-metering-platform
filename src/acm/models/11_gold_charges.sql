-- Charges per customer and month, before credits (credits are applied across months in Python,
-- oldest month first; see periods.py).
DELETE FROM gold_charges WHERE month = $month;
INSERT INTO gold_charges
WITH gpu AS (
  SELECT customer_id, sum(overage_seconds / 3600.0 * usd_per_gpu_hour) AS gpu_on_demand,
         sum(gpu_seconds) AS gpu_seconds,
         sum(CASE WHEN usd_per_gpu_hour IS NULL AND overage_seconds > 0 THEN overage_seconds ELSE 0 END) AS unpriced_seconds
  FROM gold_gpu_minutes WHERE month = $month GROUP BY customer_id
), commit_ AS (
  SELECT customer_id,
         sum(gpus * (least(ends, $month_end) - greatest(starts, $month_start)) / 3600.0 * usd_per_gpu_hour) AS commitment
  FROM silver_commitment_terms WHERE starts < $month_end AND ends > $month_start GROUP BY customer_id
), inf AS (
  SELECT i.customer_id,
         sum(i.input_tokens / 1e6 * pi.usd + i.output_tokens / 1e6 * po.usd) AS inference
  FROM silver_inference i
  JOIN ref_prices pi ON pi.sku = 'inference:' || i.model || ':input' AND i.window_ts >= pi.valid_from AND i.window_ts < pi.valid_to
  JOIN ref_prices po ON po.sku = 'inference:' || i.model || ':output' AND i.window_ts >= po.valid_from AND i.window_ts < po.valid_to
  WHERE i.window_ts >= $month_start AND i.window_ts < $month_end GROUP BY i.customer_id
), sto AS (
  SELECT s.customer_id, sum(s.bytes_stored / 1e9 * p.usd / 730) AS storage
  FROM silver_storage s
  JOIN ref_prices p ON p.sku = 'storage:object' AND s.snapshot_ts >= p.valid_from AND s.snapshot_ts < p.valid_to
  WHERE s.snapshot_ts >= $month_start AND s.snapshot_ts < $month_end GROUP BY s.customer_id
), customers AS (
  SELECT customer_id FROM gpu UNION SELECT customer_id FROM commit_ UNION SELECT customer_id FROM inf
  UNION SELECT customer_id FROM sto
)
SELECT $month AS month, c.customer_id,
       coalesce(gpu.gpu_on_demand, 0) AS gpu_on_demand, coalesce(commit_.commitment, 0) AS commitment,
       coalesce(inf.inference, 0) AS inference, coalesce(sto.storage, 0) AS storage,
       coalesce(gpu.gpu_seconds, 0) AS gpu_seconds, coalesce(gpu.unpriced_seconds, 0) AS unpriced_seconds
FROM customers c
LEFT JOIN gpu USING (customer_id) LEFT JOIN commit_ USING (customer_id)
LEFT JOIN inf USING (customer_id) LEFT JOIN sto USING (customer_id);
