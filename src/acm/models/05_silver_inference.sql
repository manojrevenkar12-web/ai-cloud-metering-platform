-- Inference usage per window. The gateway corrects a window by re-sending its event_id with a
-- higher revision: the highest revision wins, and among equal revisions the first delivery.
CREATE OR REPLACE TABLE silver_inference AS
SELECT event_id, revision, customer_id, model, region, window_start_ts AS window_ts, requests, input_tokens,
       output_tokens, api_key_owner, _visible_at AS visible_at, copies
FROM (
  SELECT *, count(*) OVER (PARTITION BY event_id) AS copies,
         row_number() OVER (PARTITION BY event_id ORDER BY revision DESC, _visible_at, _file) AS rn
  FROM bronze_inference_usage
) WHERE rn = 1;
