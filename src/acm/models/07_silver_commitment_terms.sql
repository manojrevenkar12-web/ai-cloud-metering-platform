-- Commitment terms in force: a renewal ends the commitment it replaces at the renewal's start.
CREATE OR REPLACE TABLE silver_commitment_terms AS
SELECT k.commitment_id, k.customer_id, k.gpu_type, k.region, k.gpus, k.usd_per_gpu_hour, k.starts,
       least(k.ends, coalesce(r.renewal_starts, k.ends)) AS ends, r.renewal_starts IS NOT NULL AS replaced
FROM ref_commitments k
LEFT JOIN (SELECT replaces, min(starts) AS renewal_starts FROM ref_commitments WHERE replaces IS NOT NULL GROUP BY replaces) r
  ON r.replaces = k.commitment_id;
