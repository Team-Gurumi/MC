#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

: "${CONTROL_URL:?CONTROL_URL is required}"
: "${MC_DB_DSN:?MC_DB_DSN is required}"
: "${CONTROL_TOKEN:?CONTROL_TOKEN is required}"
: "${BOOTSTRAP:?BOOTSTRAP is required}"

if [[ "${TTL_SEC}" != "10" || "${HEARTBEAT_SEC}" != "3" || "${RUN_TIMEOUT_SEC:-600}" != "600" || "${QUEUED_TIMEOUT_SEC:-180}" != "180" ]]; then
  echo "invalid environment: require TTL_SEC=10 HEARTBEAT_SEC=3 RUN_TIMEOUT_SEC=600 QUEUED_TIMEOUT_SEC=180" >&2
  echo "current: TTL_SEC=${TTL_SEC} HEARTBEAT_SEC=${HEARTBEAT_SEC} RUN_TIMEOUT_SEC=${RUN_TIMEOUT_SEC:-unset} QUEUED_TIMEOUT_SEC=${QUEUED_TIMEOUT_SEC:-unset}" >&2
  exit 1
fi

echo "env_ok TTL_SEC=${TTL_SEC} HEARTBEAT_SEC=${HEARTBEAT_SEC} RUN_TIMEOUT_SEC=${RUN_TIMEOUT_SEC:-600} QUEUED_TIMEOUT_SEC=${QUEUED_TIMEOUT_SEC:-180}"

cleanup() {
  "${SCRIPT_DIR}/run_agents.sh" stop >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

check_backlog_zero() {
  local counts
  counts="$(psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -F, -c "
    SELECT
      COALESCE(SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END),0) AS queued,
      COALESCE(SUM(CASE WHEN status='assigned' THEN 1 ELSE 0 END),0) AS assigned,
      COALESCE(SUM(CASE WHEN status='running' THEN 1 ELSE 0 END),0) AS running
    FROM demand_jobs;
  ")"
  local queued assigned running
  IFS=',' read -r queued assigned running <<< "$counts"
  if [[ "$queued" != "0" || "$assigned" != "0" || "$running" != "0" ]]; then
    echo "backlog_not_zero queued=${queued} assigned=${assigned} running=${running}" >&2
    exit 1
  fi
}

run_one() {
  local agents="$1"
  local jobs="$2"
  local workload="$3"
  local failure="$4"
  local rep="$5"

  check_backlog_zero
  echo "start agents=${agents} jobs=${jobs} workload=${workload} failure=${failure} rep=${rep}"
  AGENTS="$agents" REP="$rep" FAILURE_RATE="$failure" "${SCRIPT_DIR}/run_agents.sh" start >/dev/null
  local row run_id missing
  row="$(AGENTS="$agents" REP="$rep" FAILURE_RATE="$failure" "${SCRIPT_DIR}/run_matrix.sh" "$jobs" "$workload")"
  "${SCRIPT_DIR}/run_agents.sh" stop >/dev/null 2>&1 || true
  run_id="${row%%,*}"

  missing="$(psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "
    SELECT COUNT(*)
    FROM demand_jobs
    WHERE id LIKE '${run_id}%'
      AND NOT (metrics ? 'success');
  ")"
  if [[ "$missing" != "0" ]]; then
    echo "missing_success_key_detected run_id=${run_id} missing=${missing}" >&2
    exit 1
  fi
  echo "done run_id=${run_id}"
}

# A=50 baseline: CPU N=50,250 / IO N=50,250 / REP=1..5
for rep in 1 2 3 4 5; do
  run_one 50 50 cpu 0 "$rep"
  run_one 50 250 cpu 0 "$rep"
  run_one 50 50 io 0 "$rep"
  run_one 50 250 io 0 "$rep"
done

# A=50 crash (CPU): N=250, f=10/20/40, REP=1..5
for rep in 1 2 3 4 5; do
  run_one 50 250 cpu 10 "$rep"
  run_one 50 250 cpu 20 "$rep"
  run_one 50 250 cpu 40 "$rep"
done

# A=10 baseline CPU: N=10, REP=1..5
for rep in 1 2 3 4 5; do
  run_one 10 10 cpu 0 "$rep"
done

# A=25 baseline CPU: N=125, REP=2,3 only
for rep in 2 3; do
  run_one 25 125 cpu 0 "$rep"
done

echo "rerun_complete"
echo "post_check_global_missing_success:"
psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -F, -c "
  SELECT COUNT(*) FILTER (WHERE NOT (metrics ? 'success')) AS missing_success_key,
         COUNT(*) AS total
  FROM demand_jobs
  WHERE id LIKE 'exp1-%';
"
