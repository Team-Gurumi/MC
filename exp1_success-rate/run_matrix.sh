#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAW_HAS_TTL_SEC="${TTL_SEC+x}"
RAW_HAS_HEARTBEAT_SEC="${HEARTBEAT_SEC+x}"
RAW_HAS_RUN_TIMEOUT_SEC="${RUN_TIMEOUT_SEC+x}"
RAW_HAS_QUEUED_TIMEOUT_SEC="${QUEUED_TIMEOUT_SEC+x}"
RAW_HAS_REPETITIONS="${REPETITIONS+x}"
RAW_HAS_MATRIX_PHASE="${MATRIX_PHASE+x}"
RAW_HAS_A="${A+x}"
RAW_HAS_N="${N+x}"
RAW_HAS_WORKLOAD_TYPE="${WORKLOAD_TYPE+x}"
RAW_HAS_FAILURE_RATE="${FAILURE_RATE+x}"
RAW_HAS_FAILURE_RATE_PERCENT="${FAILURE_RATE_PERCENT+x}"
RAW_ENV_A="${A-}"
RAW_ENV_N="${N-}"
RAW_ENV_WORKLOAD_TYPE="${WORKLOAD_TYPE-}"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"
STRICT_FINAL="${STRICT_FINAL:-1}"

if [[ "${MATRIX_RUNNER_ACTIVE:-0}" != "1" ]]; then
  echo "run_matrix.sh direct execution is disabled. use matrix_runner.sh as the single orchestrator." >&2
  exit 1
fi

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

# Resolve failure rate source strictly:
# - both set: error
# - only percent set: map to FAILURE_RATE
# - neither set: default 0
if [[ "${RAW_HAS_FAILURE_RATE}" == "x" && "${RAW_HAS_FAILURE_RATE_PERCENT}" == "x" ]]; then
  echo "STRICT_FINAL requires only one of FAILURE_RATE or FAILURE_RATE_PERCENT (not both)" >&2
  exit 1
elif [[ "${RAW_HAS_FAILURE_RATE}" != "x" && "${RAW_HAS_FAILURE_RATE_PERCENT}" == "x" ]]; then
  FAILURE_RATE="$FAILURE_RATE_PERCENT"
elif [[ "${RAW_HAS_FAILURE_RATE}" != "x" && "${RAW_HAS_FAILURE_RATE_PERCENT}" != "x" ]]; then
  FAILURE_RATE=0
fi

require_raw_env() {
  local name="$1"
  local flag="$2"
  if [[ "$flag" != "x" ]]; then
    echo "STRICT_FINAL requires env var: ${name}" >&2
    return 1
  fi
}

assert_strict_final_env() {
  require_raw_env "TTL_SEC" "$RAW_HAS_TTL_SEC"
  require_raw_env "HEARTBEAT_SEC" "$RAW_HAS_HEARTBEAT_SEC"
  require_raw_env "RUN_TIMEOUT_SEC" "$RAW_HAS_RUN_TIMEOUT_SEC"
  require_raw_env "QUEUED_TIMEOUT_SEC" "$RAW_HAS_QUEUED_TIMEOUT_SEC"
  require_raw_env "REPETITIONS" "$RAW_HAS_REPETITIONS"
  require_raw_env "MATRIX_PHASE" "$RAW_HAS_MATRIX_PHASE"
  require_raw_env "A" "$RAW_HAS_A"
  require_raw_env "N" "$RAW_HAS_N"
  require_raw_env "WORKLOAD_TYPE" "$RAW_HAS_WORKLOAD_TYPE"
  if [[ "${RAW_ENV_A}" != "${AGENTS}" ]]; then
    echo "STRICT_FINAL mismatch: A=${RAW_ENV_A} AGENTS=${AGENTS}" >&2
    return 1
  fi
  if [[ "${RAW_ENV_N}" != "$1" ]]; then
    echo "STRICT_FINAL mismatch: N(env)=${RAW_ENV_N} N(arg)=$1" >&2
    return 1
  fi
  if [[ "${RAW_ENV_WORKLOAD_TYPE}" != "$2" ]]; then
    echo "STRICT_FINAL mismatch: WORKLOAD_TYPE(env)=${RAW_ENV_WORKLOAD_TYPE} WORKLOAD_TYPE(arg)=$2" >&2
    return 1
  fi
}

git_sha_short() {
  if command -v git >/dev/null 2>&1; then
    (cd "${SCRIPT_DIR}/.." && git rev-parse --short HEAD 2>/dev/null) || true
  fi
}

assert_db_isolation() {
  local log_file="$1"
  local iso iso_lc
  iso="$(psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "SHOW default_transaction_isolation;" | tr -d '\r' | xargs)"
  iso_lc="$(printf '%s' "$iso" | tr '[:upper:]' '[:lower:]')"
  printf '[%s] db_isolation=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$iso" >> "$log_file"
  if [[ "$iso_lc" != "read committed" ]]; then
    echo "invalid DB isolation: expected 'read committed', got '${iso}'" >&2
    return 1
  fi
}

check_control_effective_config() {
  local log_file="$1"
  local url="${CONTROL_URL%/}/debug/config"
  local expected_queued_timeout
  case "${MATRIX_PHASE:-}" in
    baseline) expected_queued_timeout=600 ;;
    crash) expected_queued_timeout=180 ;;
    *) expected_queued_timeout="$QUEUED_TIMEOUT_SEC" ;;
  esac
  local resp run_to queued_to
  local attempts=8
  local attempt=1
  local sleep_sec=0.2

  # harness stability patch: tolerate short control blips with bounded retry/backoff.
  while (( attempt <= attempts )); do
    if [[ -n "$CONTROL_TOKEN" ]]; then
      resp="$(curl -sS --connect-timeout 1 --max-time 2 --retry 5 --retry-all-errors --retry-delay 1 \
        -H "Authorization: Bearer ${CONTROL_TOKEN}" "$url" || true)"
    else
      resp="$(curl -sS --connect-timeout 1 --max-time 2 --retry 5 --retry-all-errors --retry-delay 1 \
        "$url" || true)"
    fi
    run_to="$(printf '%s' "$resp" | sed -nE 's/.*"run_timeout_sec"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n1)"
    queued_to="$(printf '%s' "$resp" | sed -nE 's/.*"queued_timeout_sec"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n1)"
    printf '[%s] control_debug_config=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$resp" >> "$log_file"

    if [[ -n "$run_to" && -n "$queued_to" ]]; then
      if [[ "$run_to" != "$RUN_TIMEOUT_SEC" || "$queued_to" != "$expected_queued_timeout" ]]; then
        echo "control timeout mismatch: control(run=${run_to},queued=${queued_to}) expected(run=${RUN_TIMEOUT_SEC},queued=${expected_queued_timeout}) phase=${MATRIX_PHASE:-unknown}" >&2
        return 1
      fi
      PHASE_POLICY_EXPECTED_QUEUED_TIMEOUT="$expected_queued_timeout"
      PHASE_POLICY_CONTROL_QUEUED_TIMEOUT="$queued_to"
      return 0
    fi

    if (( attempt == attempts )); then
      break
    fi
    sleep "$sleep_sec"
    sleep_sec="$(awk -v s="$sleep_sec" 'BEGIN { s*=2; if (s>2) s=2; printf "%.3f", s }')"
    attempt=$((attempt + 1))
  done

  echo "failed to verify control effective config from ${url}" >&2
  return 1
}

write_schema_file() {
  local schema_file="$1"
  local git_sha="$2"
  cat > "$schema_file" <<JSON
{
  "schema_version": "v1",
  "git_sha": "${git_sha}",
  "columns": ["job_id","created_at","status","retry_count","attempt_no","duration_ms","e2e_ms","agent_id","completion_ok","delivery_ok","success","submit_ts","exec_start_ts","exec_end_ts","finish_reported_ts"]
}
JSON
}

validate_job_metrics_columns() {
  local csv_file="$1"
  local required_cols="job_id agent_id duration_ms e2e_ms retry_count completion_ok delivery_ok success submit_ts exec_start_ts exec_end_ts finish_reported_ts"
  local header
  header="$(head -n1 "$csv_file")"
  for c in $required_cols; do
    if ! printf '%s' "$header" | tr ',' '\n' | grep -qx "$c"; then
      echo "job_metrics.csv missing required column: $c" >&2
      return 1
    fi
  done
}

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
  local sleep_sec=0.5
  deadline=$(( $(date +%s) + WAIT_TIMEOUT_SEC ))

  while :; do
    now=$(date +%s)
    if (( now > deadline )); then
      echo "timeout waiting for jobs to leave queued/assigned/running" >&2
      return 1
    fi

    pending="$(psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "
      SELECT COUNT(*)
      FROM demand_jobs
      WHERE id LIKE '${prefix}%'
        AND status IN ('queued','assigned','running');
    ")"

    pending="${pending//[[:space:]]/}"
    if [[ "$pending" == "0" ]]; then
      return 0
    fi
    # harness stability patch: reduce DB polling churn with bounded backoff.
    sleep "$sleep_sec"
    sleep_sec="$(awk -v s="$sleep_sec" 'BEGIN { s*=1.5; if (s>5) s=5; printf "%.3f", s }')"
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
sql_job_prefix_quoted="'$(printf '%s' "$job_prefix" | sed "s/'/''/g")'"
run_dir="${RUNS_ROOT}/${run_id}"
mkdir -p "$run_dir"
ensure_clean_prefix "$job_prefix"
if [[ "$STRICT_FINAL" == "1" ]]; then
  assert_strict_final_env "$N" "$WORKLOAD_TYPE"
fi

config_json_file="${run_dir}/config.json"
submission_ids_file="${run_dir}/submission_ids.txt"
submission_log_file="${run_dir}/submission.log"
control_snapshot_file="${run_dir}/control_snapshot.log"
validation_log_file="${run_dir}/validation.log"
effective_config_file="${run_dir}/effective_config.jsonl"
job_metrics_file="${run_dir}/job_metrics.csv"
job_metrics_schema_file="${run_dir}/job_metrics.schema.json"
summary_json_file="${run_dir}/summary.json"
summary_csv_file="${run_dir}/summary.csv"

start_ts="$(date +%s)"
start_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
git_sha="$(git_sha_short)"
if [[ -z "$git_sha" ]]; then
  git_sha="unknown"
fi
matrix_profile="${MATRIX_PROFILE:-final105}"
workload_seed="${WORKLOAD_SEED:-$(printf '%s' "$run_id" | cksum | awk '{print $1}')}"

if [[ "$STRICT_FINAL" == "1" ]]; then
  assert_db_isolation "$validation_log_file"
  check_control_effective_config "$validation_log_file"
  {
    echo "[phase-policy]"
    echo "MATRIX_PHASE=${MATRIX_PHASE:-unknown}"
    echo "EXPECTED_QUEUED_TIMEOUT=${PHASE_POLICY_EXPECTED_QUEUED_TIMEOUT:-unknown}"
    echo "CONTROL_QUEUED_TIMEOUT=${PHASE_POLICY_CONTROL_QUEUED_TIMEOUT:-unknown}"
  } >&2
fi

printf '{"timestamp":"%s","run_id":"%s","matrix_profile":"%s","ttl_sec":%s,"heartbeat_sec":%s,"run_timeout_sec":%s,"queued_timeout_sec":%s,"A":%s,"N":%s,"workload_type":"%s","failure_rate":%s,"repetitions":%s,"workload_seed":"%s","git_sha":"%s"}\n' \
  "$start_utc" "$run_id" "$matrix_profile" "$TTL_SEC" "$HEARTBEAT_SEC" "$RUN_TIMEOUT_SEC" "$QUEUED_TIMEOUT_SEC" \
  "$AGENTS" "$N" "$WORKLOAD_TYPE" "$FAILURE_RATE" "${REPETITIONS:-}" "$workload_seed" "$git_sha" \
  >> "$effective_config_file"

cat > "$config_json_file" <<JSON
{
  "run_id": "$(json_escape "$run_id")",
  "job_prefix": "$(json_escape "$job_prefix")",
  "control_url": "$(json_escape "$CONTROL_URL")",
  "agents": ${AGENTS},
  "failure_rate": ${FAILURE_RATE},
  "rep": ${REP},
  "repetitions": ${REPETITIONS:-0},
  "jobs": ${N},
  "workload": "$(json_escape "$WORKLOAD_TYPE")",
  "workload_seed": "$(json_escape "$workload_seed")",
  "matrix_phase": "$(json_escape "${MATRIX_PHASE:-}")",
  "ttl_sec": ${TTL_SEC},
  "heartbeat_sec": ${HEARTBEAT_SEC},
  "run_timeout_sec": ${RUN_TIMEOUT_SEC},
  "queued_timeout_sec": ${QUEUED_TIMEOUT_SEC},
  "agent_pids_file": "$(json_escape "$AGENT_PIDS_FILE")",
  "failure_inject_delay_sec": ${FAILURE_INJECT_DELAY_SEC},
  "wait_timeout_sec": ${WAIT_TIMEOUT_SEC},
  "poll_sec": ${POLL_SEC},
  "image": "$(json_escape "$IMAGE")",
  "manifest_root_cid": "$(json_escape "$MANIFEST_ROOT_CID")",
  "effective_git_sha": "$(json_escape "$git_sha")",
  "effective_matrix_profile": "$(json_escape "$matrix_profile")",
  "effective_ttl_sec": ${TTL_SEC},
  "effective_heartbeat_sec": ${HEARTBEAT_SEC},
  "effective_run_timeout_sec": ${RUN_TIMEOUT_SEC},
  "effective_queued_timeout_sec": ${QUEUED_TIMEOUT_SEC},
  "effective_A": ${AGENTS},
  "effective_N": ${N},
  "effective_workload_type": "$(json_escape "$WORKLOAD_TYPE")",
  "effective_failure_rate": ${FAILURE_RATE},
  "effective_repetitions": ${REPETITIONS:-0},
  "effective_workload_seed": "$(json_escape "$workload_seed")",
  "effective_timestamp": "$(json_escape "$start_utc")",
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
JOB_PREFIX="$job_prefix" WORKLOAD_TYPE="$WORKLOAD_TYPE" WORKLOAD_SEED="$workload_seed" \
  "${SCRIPT_DIR}/submit_jobs.sh" "$N" \
  > "$submission_ids_file" 2> "$submission_log_file"

submitted_jobs=0
if [[ -f "$submission_ids_file" ]]; then
  submitted_jobs="$(grep -c '.' "$submission_ids_file" 2>/dev/null || true)"
fi
submitted_jobs="${submitted_jobs:-0}"
if (( submitted_jobs == 0 )); then
  echo "no jobs submitted successfully" >&2
  exit 1
fi

inject_failure_by_kill "$run_dir"

wait_until_drained "$job_prefix"
end_ts="$(date +%s)"
end_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -c "
COPY (
  SELECT
    id AS job_id,
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
    END AS e2e_ms,
    metrics->>'agent_id' AS agent_id,
    CASE
      WHEN metrics ? 'completion_ok'
      THEN COALESCE((metrics->>'completion_ok')::boolean, false)
      ELSE false
    END AS completion_ok,
    CASE
      WHEN metrics ? 'delivery_ok'
      THEN COALESCE((metrics->>'delivery_ok')::boolean, false)
      ELSE false
    END AS delivery_ok,
    CASE
      WHEN metrics ? 'success'
      THEN COALESCE((metrics->>'success')::boolean, false)
      ELSE false
    END AS success,
    metrics->>'submit_ts' AS submit_ts,
    metrics->>'exec_start_ts' AS exec_start_ts,
    metrics->>'exec_end_ts' AS exec_end_ts,
    metrics->>'finish_reported_ts' AS finish_reported_ts
  FROM demand_jobs
  WHERE id LIKE ${sql_job_prefix_quoted} || '%'
) TO STDOUT WITH CSV HEADER;
" > "$job_metrics_file"
write_schema_file "$job_metrics_schema_file" "$git_sha"
validate_job_metrics_columns "$job_metrics_file"

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

run_pass="$(
  awk -v sr="${success_rate:-0}" -v ms="$makespan_sec" -v rt="$RUN_TIMEOUT_SEC" 'BEGIN {
    if ((sr + 0.0) == 1.0 && ms <= rt) print 1; else print 0;
  }'
)"

if [[ ! -f "$RESULTS_CSV" ]]; then
  echo "run_id,timestamp_utc,start_ts,end_ts,agents,jobs,workload,failure_rate,rep,makespan_sec,throughput,total_jobs,succeeded_jobs,success_rate,p50_ms,p95_ms,p99_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_mean_ms,avg_attempts,max_attempts,avg_retry_count,max_retry_count,run_pass" > "$RESULTS_CSV"
fi

csv_row="${run_id},${end_utc},${start_ts},${end_ts},${AGENTS},${N},${WORKLOAD_TYPE},${FAILURE_RATE},${REP},${makespan_sec},${throughput},${total_jobs},${succeeded_jobs},${success_rate},${p50_ms:-},${p95_ms:-},${p99_ms:-},${e2e_p50_ms:-},${e2e_p95_ms:-},${e2e_p99_ms:-},${e2e_mean_ms:-},${avg_attempts},${max_attempts},${avg_retry_count},${max_retry_count},${run_pass}"

echo "run_id,timestamp_utc,start_ts,end_ts,agents,jobs,workload,failure_rate,rep,makespan_sec,throughput,total_jobs,succeeded_jobs,success_rate,p50_ms,p95_ms,p99_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_mean_ms,avg_attempts,max_attempts,avg_retry_count,max_retry_count,run_pass" > "$summary_csv_file"
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
  "run_pass": ${run_pass},
  "total_jobs": ${total_jobs},
  "succeeded_jobs": ${succeeded_jobs},
  "start_ts": ${start_ts},
  "end_ts": ${end_ts}
}
JSON

printf '%s\n' "$csv_row"
echo "run_complete agents=${AGENTS} failure_rate=${FAILURE_RATE} ttl_sec=${TTL_SEC} heartbeat_sec=${HEARTBEAT_SEC} rep=${REP}" >&2
