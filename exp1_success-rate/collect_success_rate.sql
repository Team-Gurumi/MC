DO $$
BEGIN
  IF length(trim(:'job_prefix')) = 0 THEN
    RAISE EXCEPTION 'job_prefix must be non-empty';
  END IF;
END $$;

WITH scope AS (
  SELECT
    j.id,
    j.status,
    j.retry_count,
    CASE
      WHEN j.metrics ? 'attempt_no'
       AND (j.metrics->>'attempt_no') ~ '^-?[0-9]+$'
      THEN (j.metrics->>'attempt_no')::int
      ELSE NULL
    END AS attempt_no,
    CASE
      WHEN j.metrics ? 'duration_ms'
       AND (j.metrics->>'duration_ms') ~ '^-?[0-9]+(\.[0-9]+)?$'
      THEN (j.metrics->>'duration_ms')::numeric
      ELSE NULL
    END AS duration_ms,
    CASE
      WHEN j.metrics ? 'submit_ts'
       AND j.metrics ? 'exec_end_ts'
      THEN EXTRACT(EPOCH FROM (
        (j.metrics->>'exec_end_ts')::timestamptz -
        (j.metrics->>'submit_ts')::timestamptz
      )) * 1000.0
      ELSE NULL
    END AS e2e_ms,
    CASE
      WHEN j.metrics ? 'exit_code'
       AND (j.metrics->>'exit_code') ~ '^-?[0-9]+$'
      THEN (j.metrics->>'exit_code')::int
      ELSE NULL
    END AS exit_code
  FROM demand_jobs j
  WHERE j.id LIKE :'job_prefix' || '%'
),
durations AS (
  SELECT duration_ms
  FROM scope
  WHERE duration_ms IS NOT NULL
),
e2e_durations AS (
  SELECT e2e_ms
  FROM scope
  WHERE e2e_ms IS NOT NULL
),
agg AS (
  SELECT
    COUNT(*)::bigint AS total_jobs,
    COUNT(*) FILTER (
      WHERE status = 'succeeded'
        AND (exit_code IS NULL OR exit_code = 0)
    )::bigint AS succeeded_jobs,
    COALESCE(AVG(attempt_no::numeric), 0)::numeric AS avg_attempts,
    COALESCE(MAX(attempt_no), 0)::int AS max_attempts,
    COALESCE(AVG(retry_count::numeric), 0)::numeric AS avg_retry_count,
    COALESCE(MAX(retry_count), 0)::int AS max_retry_count,
    (SELECT percentile_disc(0.50) WITHIN GROUP (ORDER BY duration_ms) FROM durations) AS p50_ms,
    (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_ms) FROM durations) AS p95_ms,
    (SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY duration_ms) FROM durations) AS p99_ms,
    (SELECT percentile_disc(0.50) WITHIN GROUP (ORDER BY e2e_ms) FROM e2e_durations) AS e2e_p50_ms,
    (SELECT percentile_disc(0.95) WITHIN GROUP (ORDER BY e2e_ms) FROM e2e_durations) AS e2e_p95_ms,
    (SELECT percentile_disc(0.99) WITHIN GROUP (ORDER BY e2e_ms) FROM e2e_durations) AS e2e_p99_ms,
    (SELECT AVG(e2e_ms) FROM e2e_durations) AS e2e_mean_ms
  FROM scope
)
SELECT
  total_jobs,
  succeeded_jobs,
  CASE
    WHEN total_jobs = 0 THEN 0::numeric
    ELSE ROUND((succeeded_jobs::numeric / total_jobs::numeric), 6)
  END AS success_rate,
  p50_ms,
  p95_ms,
  p99_ms,
  e2e_p50_ms,
  e2e_p95_ms,
  e2e_p99_ms,
  ROUND(COALESCE(e2e_mean_ms, 0), 6) AS e2e_mean_ms,
  ROUND(avg_attempts, 6) AS avg_attempts,
  max_attempts,
  ROUND(avg_retry_count, 6) AS avg_retry_count,
  max_retry_count
FROM agg;
