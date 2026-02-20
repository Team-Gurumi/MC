#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <N> <workload_type>" >&2
  exit 1
fi

N="$1"
WORKLOAD_TYPE="$2"

if ! [[ "$N" =~ ^[0-9]+$ ]] || [[ "$N" -le 0 ]]; then
  echo "N must be a positive integer" >&2
  exit 1
fi
if [[ "$WORKLOAD_TYPE" != "cpu" && "$WORKLOAD_TYPE" != "io" ]]; then
  echo "workload_type must be cpu or io" >&2
  exit 1
fi
if ! [[ "$AGENTS" =~ ^[0-9]+$ ]]; then
  echo "AGENTS must be integer" >&2
  exit 1
fi
if ! [[ "$FAILURE_RATE" =~ ^[0-9]+$ ]]; then
  echo "FAILURE_RATE must be integer" >&2
  exit 1
fi
if (( FAILURE_RATE < 0 || FAILURE_RATE > 100 )); then
  echo "FAILURE_RATE must be in [0,100]" >&2
  exit 1
fi
if ! [[ "$REP" =~ ^[0-9]+$ ]]; then
  echo "REP must be integer" >&2
  exit 1
fi
if ! [[ "$FAILURE_INJECT_DELAY_SEC" =~ ^[0-9]+$ ]]; then
  echo "FAILURE_INJECT_DELAY_SEC must be integer" >&2
  exit 1
fi

: "${CONTROL_URL:?CONTROL_URL is required}"
: "${MC_DB_DSN:?MC_DB_DSN is required}"

json_escape() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/ }"
  printf '%s' "$s"
}

wait_until_drained() {
  local prefix="$1"
  local deadline now pending
  deadline=$(( $(date +%s) + WAIT_TIMEOUT_SEC ))

  while :; do
    now=$(date +%s)
    if (( now > deadline )); then
      echo "timeout waiting for jobs to leave queued/running" >&2
      return 1
    fi

    pending="$(psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "
      SELECT COUNT(*)
      FROM demand_jobs
      WHERE id LIKE '${prefix}%'
        AND status IN ('queued','running');
    ")"

    pending="${pending//[[:space:]]/}"
    if [[ "$pending" == "0" ]]; then
      return 0
    fi
    sleep "$POLL_SEC"
  done
}

ensure_clean_prefix() {
  local prefix="$1"
  local active_count
  active_count="$(psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "
    SELECT COUNT(*)
    FROM demand_jobs
    WHERE id LIKE '${prefix}%'
      AND status IN ('queued','assigned','running');
  ")"
  active_count="${active_count//[[:space:]]/}"
  if [[ "$active_count" != "0" ]]; then
    echo "non-terminal jobs already exist for prefix=${prefix}" >&2
    psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -F, -c "
      SELECT status, COUNT(*)
      FROM demand_jobs
      WHERE id LIKE '${prefix}%'
      GROUP BY status
      ORDER BY status;
    " >&2
    return 1
  fi
}

select_random_lines() {
  local count="$1"
  local src="$2"
  if command -v shuf >/dev/null 2>&1; then
    shuf -n "$count" "$src"
  else
    awk 'BEGIN {srand()} {printf "%.12f\t%s\n", rand(), $0}' "$src" | sort -n | head -n "$count" | cut -f2-
  fi
}

inject_failure_by_kill() {
  local run_dir="$1"
  local log_file="${run_dir}/failure_injection.log"
  local selected_file="${run_dir}/killed_agents.txt"

  if (( FAILURE_RATE <= 0 )); then
    return 0
  fi
  if [[ -z "${AGENT_PIDS_FILE:-}" ]]; then
    echo "FAILURE_RATE=${FAILURE_RATE} requires AGENT_PIDS_FILE (one pid per line)" >&2
    return 1
  fi
  if [[ ! -f "$AGENT_PIDS_FILE" ]]; then
    echo "AGENT_PIDS_FILE not found: $AGENT_PIDS_FILE" >&2
    return 1
  fi

  local live_pids_file
  live_pids_file="$(mktemp)"
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    if kill -0 "$pid" 2>/dev/null; then
      printf '%s\n' "$pid" >> "$live_pids_file"
    fi
  done < "$AGENT_PIDS_FILE"

  local total_live kill_count
  total_live="$(wc -l < "$live_pids_file" | tr -d ' ')"
  if [[ "$total_live" == "0" ]]; then
    rm -f "$live_pids_file"
    echo "no live agents available for failure injection" >&2
    return 1
  fi
  kill_count=$(( (total_live * FAILURE_RATE + 99) / 100 ))
  if (( kill_count <= 0 )); then
    rm -f "$live_pids_file"
    return 0
  fi
  if (( kill_count > total_live )); then
    kill_count="$total_live"
  fi

  {
    printf '[%s] injecting failure_rate=%s%% delay_sec=%s total_live=%s kill_count=%s\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$FAILURE_RATE" "$FAILURE_INJECT_DELAY_SEC" "$total_live" "$kill_count"
  } > "$log_file"

  sleep "$FAILURE_INJECT_DELAY_SEC"
  select_random_lines "$kill_count" "$live_pids_file" > "$selected_file"

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
      printf '[%s] killed pid=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$pid" >> "$log_file"
    fi
  done < "$selected_file"
  rm -f "$live_pids_file"
}

mkdir -p "$RUNS_ROOT"

run_id="exp1-$(date -u +%Y%m%dT%H%M%SZ)-${WORKLOAD_TYPE}-N${N}-A${AGENTS}-R${REP}"
job_prefix="$run_id"
run_dir="${RUNS_ROOT}/${run_id}"
mkdir -p "$run_dir"
ensure_clean_prefix "$job_prefix"

config_json_file="${run_dir}/config.json"
submission_ids_file="${run_dir}/submission_ids.txt"
submission_log_file="${run_dir}/submission.log"
control_snapshot_file="${run_dir}/control_snapshot.log"
job_metrics_file="${run_dir}/job_metrics.csv"
summary_json_file="${run_dir}/summary.json"
summary_csv_file="${run_dir}/summary.csv"

start_ts="$(date +%s)"
start_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cat > "$config_json_file" <<JSON
{
  "run_id": "$(json_escape "$run_id")",
  "job_prefix": "$(json_escape "$job_prefix")",
  "control_url": "$(json_escape "$CONTROL_URL")",
  "agents": ${AGENTS},
  "failure_rate": ${FAILURE_RATE},
  "rep": ${REP},
  "jobs": ${N},
  "workload": "$(json_escape "$WORKLOAD_TYPE")",
  "ttl_sec": ${TTL_SEC},
  "heartbeat_sec": ${HEARTBEAT_SEC},
  "agent_pids_file": "$(json_escape "$AGENT_PIDS_FILE")",
  "failure_inject_delay_sec": ${FAILURE_INJECT_DELAY_SEC},
  "wait_timeout_sec": ${WAIT_TIMEOUT_SEC},
  "poll_sec": ${POLL_SEC},
  "image": "$(json_escape "$IMAGE")",
  "manifest_root_cid": "$(json_escape "$MANIFEST_ROOT_CID")",
  "start_utc": "$(json_escape "$start_utc")",
  "start_ts": ${start_ts}
}
JSON

{
  echo "[$start_utc] GET /api/health"
  curl -sS -i "${CONTROL_URL%/}/api/health"
  echo
  echo "[$start_utc] GET /api/stats/tasks"
  if [[ -n "$CONTROL_TOKEN" ]]; then
    curl -sS -i -H "Authorization: Bearer ${CONTROL_TOKEN}" "${CONTROL_URL%/}/api/stats/tasks" || true
  else
    curl -sS -i "${CONTROL_URL%/}/api/stats/tasks" || true
  fi
} > "$control_snapshot_file"

SUBMIT_FAILURE_LOG="${run_dir}/submit_failures.log" \
JOB_PREFIX="$job_prefix" WORKLOAD_TYPE="$WORKLOAD_TYPE" \
  "${SCRIPT_DIR}/submit_jobs.sh" "$N" \
  > "$submission_ids_file" 2> "$submission_log_file"

submitted_jobs="$(grep -c '.' "$submission_ids_file" || true)"
if [[ "$submitted_jobs" -eq 0 ]]; then
  echo "no jobs submitted successfully" >&2
  exit 1
fi

inject_failure_by_kill "$run_dir"

wait_until_drained "$job_prefix"
end_ts="$(date +%s)"
end_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -v job_prefix="$job_prefix" -c "
COPY (
  SELECT
    id,
    created_at,
    status,
    retry_count,
    CASE
      WHEN metrics ? 'attempt_no'
       AND (metrics->>'attempt_no') ~ '^-?[0-9]+$'
      THEN (metrics->>'attempt_no')::int
      ELSE NULL
    END AS attempt_no,
    CASE
      WHEN metrics ? 'duration_ms'
       AND (metrics->>'duration_ms') ~ '^-?[0-9]+(\\.[0-9]+)?$'
      THEN (metrics->>'duration_ms')::numeric
      ELSE NULL
    END AS duration_ms,
    CASE
      WHEN metrics ? 'submit_ts'
       AND metrics ? 'exec_end_ts'
      THEN EXTRACT(EPOCH FROM (
        (metrics->>'exec_end_ts')::timestamptz -
        (metrics->>'submit_ts')::timestamptz
      )) * 1000.0
      ELSE NULL
    END AS e2e_ms
  FROM demand_jobs
  WHERE id LIKE :'job_prefix' || '%'
) TO STDOUT WITH CSV HEADER;
" > "$job_metrics_file"

summary_metrics_line="$(psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -v job_prefix="$job_prefix" -At -F, -f "${SCRIPT_DIR}/collect_success_rate.sql" | tail -n 1)"
IFS=',' read -r \
  total_jobs succeeded_jobs success_rate \
  p50_ms p95_ms p99_ms \
  e2e_p50_ms e2e_p95_ms e2e_p99_ms e2e_mean_ms \
  avg_attempts max_attempts avg_retry_count max_retry_count \
  <<< "$summary_metrics_line"

makespan_sec=$((end_ts - start_ts))
if (( makespan_sec > 0 )); then
  throughput="$(awk -v t="$total_jobs" -v m="$makespan_sec" 'BEGIN { printf "%.6f", t/m }')"
else
  throughput="0"
fi

if [[ ! -f "$RESULTS_CSV" ]]; then
  echo "run_id,timestamp_utc,start_ts,end_ts,agents,jobs,workload,failure_rate,rep,makespan_sec,throughput,total_jobs,succeeded_jobs,success_rate,p50_ms,p95_ms,p99_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_mean_ms,avg_attempts,max_attempts,avg_retry_count,max_retry_count" > "$RESULTS_CSV"
fi

csv_row="${run_id},${end_utc},${start_ts},${end_ts},${AGENTS},${N},${WORKLOAD_TYPE},${FAILURE_RATE},${REP},${makespan_sec},${throughput},${total_jobs},${succeeded_jobs},${success_rate},${p50_ms:-},${p95_ms:-},${p99_ms:-},${e2e_p50_ms:-},${e2e_p95_ms:-},${e2e_p99_ms:-},${e2e_mean_ms:-},${avg_attempts},${max_attempts},${avg_retry_count},${max_retry_count}"

echo "run_id,timestamp_utc,start_ts,end_ts,agents,jobs,workload,failure_rate,rep,makespan_sec,throughput,total_jobs,succeeded_jobs,success_rate,p50_ms,p95_ms,p99_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_mean_ms,avg_attempts,max_attempts,avg_retry_count,max_retry_count" > "$summary_csv_file"
printf '%s\n' "$csv_row" >> "$summary_csv_file"
printf '%s\n' "$csv_row" >> "$RESULTS_CSV"

cat > "$summary_json_file" <<JSON
{
  "run_id": "$(json_escape "$run_id")",
  "agents": ${AGENTS},
  "jobs": ${N},
  "workload": "$(json_escape "$WORKLOAD_TYPE")",
  "failure_rate": ${FAILURE_RATE},
  "makespan_sec": ${makespan_sec},
  "throughput": ${throughput},
  "success_rate": ${success_rate},
  "p50_ms": ${p50_ms:-null},
  "p95_ms": ${p95_ms:-null},
  "p99_ms": ${p99_ms:-null},
  "e2e_p50_ms": ${e2e_p50_ms:-null},
  "e2e_p95_ms": ${e2e_p95_ms:-null},
  "e2e_p99_ms": ${e2e_p99_ms:-null},
  "e2e_mean_ms": ${e2e_mean_ms:-null},
  "avg_attempts": ${avg_attempts},
  "max_attempts": ${max_attempts},
  "avg_retry_count": ${avg_retry_count},
  "max_retry_count": ${max_retry_count},
  "total_jobs": ${total_jobs},
  "succeeded_jobs": ${succeeded_jobs},
  "start_ts": ${start_ts},
  "end_ts": ${end_ts}
}
JSON

printf '%s\n' "$csv_row"
echo "run_complete agents=${AGENTS} failure_rate=${FAILURE_RATE} ttl_sec=${TTL_SEC} heartbeat_sec=${HEARTBEAT_SEC} rep=${REP}"
