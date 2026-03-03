-- Detect duplicate execution by lease interval overlap using metrics JSONB only.
-- Usage:
--   psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -v job_prefix='exp1-20260220T051628Z-cpu-N500-A10-R1' -f exp1_success-rate/pipeline/detect_duplicate_overlap.sql

WITH params AS (
  SELECT :'job_prefix'::text AS job_prefix
),
base AS (
  SELECT
    dj.id AS job_id,
    dj.metrics
  FROM demand_jobs dj
  CROSS JOIN params p
  WHERE p.job_prefix <> ''
    AND dj.id LIKE p.job_prefix || '%'
),
attempt_events AS (
  -- Prefer explicit metrics.lease_events array when present.
  SELECT
    b.job_id,
    e.value AS ev
  FROM base b,
       LATERAL jsonb_array_elements(
         CASE
           WHEN jsonb_typeof(b.metrics->'lease_events') = 'array' THEN b.metrics->'lease_events'
           WHEN b.metrics ? 'lease_acquired_ts' THEN jsonb_build_array(
             jsonb_build_object(
               'lease_acquired_ts', b.metrics->>'lease_acquired_ts',
               'agent_id',         b.metrics->>'agent_id',
               'attempt_no',       b.metrics->>'attempt_no',
               'ttl_sec',          b.metrics->>'ttl_sec'
             )
           )
           ELSE '[]'::jsonb
         END
       ) AS e
),
intervals AS (
  SELECT
    job_id,
    ev->>'agent_id' AS agent_id,
    NULLIF(ev->>'attempt_no', '')::int AS attempt_no,
    (ev->>'lease_acquired_ts')::timestamptz AS lease_acquired_ts,
    ((ev->>'lease_acquired_ts')::timestamptz
      + make_interval(secs => COALESCE(NULLIF(ev->>'ttl_sec', '')::int, 0))) AS lease_expires_at
  FROM attempt_events
  WHERE ev ? 'agent_id'
    AND ev ? 'lease_acquired_ts'
),
duplicate_pairs AS (
  SELECT
    a.job_id,
    a.agent_id AS agent_a,
    b.agent_id AS agent_b,
    a.attempt_no AS attempt_a,
    b.attempt_no AS attempt_b,
    a.lease_acquired_ts AS a_lease_acquired_ts,
    a.lease_expires_at  AS a_lease_expires_at,
    b.lease_acquired_ts AS b_lease_acquired_ts,
    b.lease_expires_at  AS b_lease_expires_at
  FROM intervals a
  JOIN intervals b
    ON a.job_id = b.job_id
   AND a.agent_id <> b.agent_id
   AND (a.agent_id, COALESCE(a.attempt_no, -1), a.lease_acquired_ts)
     < (b.agent_id, COALESCE(b.attempt_no, -1), b.lease_acquired_ts)
  WHERE
    -- overlap rule requested:
    a.lease_acquired_ts < b.lease_expires_at
    AND
    b.lease_acquired_ts < a.lease_expires_at
)
SELECT *
FROM duplicate_pairs
ORDER BY job_id, a_lease_acquired_ts, b_lease_acquired_ts;
