#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

MATRIX_PROFILE="${MATRIX_PROFILE:-final105}"
REPETITIONS="${REPETITIONS:-5}"
AGENT_VALUES=(10 25 50)
CRASH_RATES=(10 20 40)
CRASH_INCLUDE_IO="${CRASH_INCLUDE_IO:-0}"
SUMMARY_AGG_CSV="${SUMMARY_AGG_CSV:-${SCRIPT_DIR}/summary_aggregated.csv}"
ALLOW_MUTABLE_MATRIX="${ALLOW_MUTABLE_MATRIX:-0}"
MAX_RUN_RETRIES="${MAX_RUN_RETRIES:-2}"
RETRY_BACKOFF_SEC="${RETRY_BACKOFF_SEC:-3}"
RUN_FAILURE_COOLDOWN_SEC="${RUN_FAILURE_COOLDOWN_SEC:-5}"
NON_FATAL_RUN_FAILURES="${NON_FATAL_RUN_FAILURES:-1}"
START_CONTROL_RETRIES="${START_CONTROL_RETRIES:-3}"
START_CONTROL_BACKOFF_SEC="${START_CONTROL_BACKOFF_SEC:-2}"
FAILURE_ANALYSIS_LOG="${FAILURE_ANALYSIS_LOG:-${RUNS_ROOT}/failure_analysis.log}"
CONTROL_HTTP_PORT="${CONTROL_HTTP_PORT:-8080}"
CONTROL_PID_FILE="${RUNS_ROOT}/control.pid"
CURRENT_CONTROL_PHASE=""
CONTROL_BIN_PATH="${CONTROL_BIN_PATH:-${SCRIPT_DIR}/../control}"

mkdir -p "$RUNS_ROOT"

if [[ -z "${AGENT_PIDS_FILE:-}" ]]; then
  AGENT_PIDS_FILE="${RUNS_ROOT}/agents.pids"
fi
export AGENT_PIDS_FILE

LOCK_DIR="${RUNS_ROOT}/.matrix_runner.lock"
LOCK_PID_FILE="${LOCK_DIR}/pid"
RUNNER_PID_FILE="${RUNS_ROOT}/matrix_runner.pid"
LOCK_FILE="${RUNS_ROOT}/.matrix_runner.flock"
USE_FLOCK=0

# harness stability patch: dual guard (pid file + lock dir) to prevent duplicate runners
# even if lock dir is manually removed.
if [[ -f "$RUNNER_PID_FILE" ]]; then
  existing_pid="$(cat "$RUNNER_PID_FILE" 2>/dev/null || true)"
  if [[ -n "${existing_pid:-}" ]] && kill -0 "$existing_pid" 2>/dev/null; then
    echo "matrix_runner already running (pid=${existing_pid}). aborting duplicate start." >&2
    exit 1
  fi
  rm -f "$RUNNER_PID_FILE"
fi

if command -v flock >/dev/null 2>&1; then
  USE_FLOCK=1
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "matrix_runner already running (flock busy). aborting duplicate start." >&2
    exit 1
  fi
else
  echo "[warn] flock not available; using pid+lockdir guard only" >&2
fi

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  if [[ -f "$LOCK_PID_FILE" ]]; then
    lock_pid="$(cat "$LOCK_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
      echo "matrix_runner already running (pid=${lock_pid}). aborting duplicate start." >&2
      exit 1
    fi
  fi
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR"
fi
echo "$$" > "$LOCK_PID_FILE"
echo "$$" > "$RUNNER_PID_FILE"

cleanup_on_exit() {
  "${SCRIPT_DIR}/run_agents.sh" stop >/dev/null 2>&1 || true
  stop_control >/dev/null 2>&1 || true
  rm -f "$RUNNER_PID_FILE"
  rm -rf "$LOCK_DIR"
  if [[ "$USE_FLOCK" == "1" ]]; then
    rm -f "$LOCK_FILE"
  fi
}
trap cleanup_on_exit EXIT INT TERM

# harness stability patch: supervise control lifecycle by phase.
wait_control_ready() {
  local attempt=1
  local max_attempts=20
  local sleep_sec=1
  local http_code
  while (( attempt <= max_attempts )); do
    http_code="$(curl -sS --connect-timeout 1 --max-time 2 -o /dev/null -w '%{http_code}' "${CONTROL_URL%/}/api/health" || true)"
    if [[ "$http_code" == "200" ]]; then
      return 0
    fi
    if [[ -n "${CONTROL_TOKEN:-}" ]]; then
      http_code="$(curl -sS --connect-timeout 1 --max-time 2 -H "Authorization: Bearer ${CONTROL_TOKEN}" -o /dev/null -w '%{http_code}' "${CONTROL_URL%/}/debug/config" || true)"
    else
      http_code="$(curl -sS --connect-timeout 1 --max-time 2 -o /dev/null -w '%{http_code}' "${CONTROL_URL%/}/debug/config" || true)"
    fi
    if [[ "$http_code" == "200" ]]; then
      return 0
    fi
    if (( attempt == max_attempts )); then
      break
    fi
    sleep "$sleep_sec"
    if (( sleep_sec < 5 )); then
      sleep_sec=$((sleep_sec + 1))
    fi
    attempt=$((attempt + 1))
  done
  return 1
}

# harness stability patch: phase-start control config verification with bounded retry.
# - connection/parse failures: retry
# - value mismatch: immediate abort
verify_control_phase_config() {
  local expected_queued_timeout="$1"
  local url="${CONTROL_URL%/}/debug/config"
  local attempts=12
  local attempt=1
  local sleep_sec=1

  while (( attempt <= attempts )); do
    local resp run_to queued_to
    if [[ -n "${CONTROL_TOKEN:-}" ]]; then
      resp="$(curl -sS --connect-timeout 1 --max-time 2 --retry 3 --retry-all-errors --retry-delay 1 -H "Authorization: Bearer ${CONTROL_TOKEN}" "$url" || true)"
    else
      resp="$(curl -sS --connect-timeout 1 --max-time 2 --retry 3 --retry-all-errors --retry-delay 1 "$url" || true)"
    fi
    run_to="$(printf '%s' "$resp" | sed -nE 's/.*"run_timeout_sec"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n1)"
    queued_to="$(printf '%s' "$resp" | sed -nE 's/.*"queued_timeout_sec"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n1)"
    if [[ -n "$run_to" && -n "$queued_to" ]]; then
      if [[ "$run_to" != "$RUN_TIMEOUT_SEC" || "$queued_to" != "$expected_queued_timeout" ]]; then
        echo "control timeout mismatch at phase boundary: control(run=${run_to},queued=${queued_to}) expected(run=${RUN_TIMEOUT_SEC},queued=${expected_queued_timeout})" >&2
        return 1
      fi
      return 0
    fi
    if printf '%s' "$resp" | grep -qi "unauthorized"; then
      echo "control /debug/config unauthorized: check CONTROL_TOKEN consistency between harness and control" >&2
      return 1
    fi
    if (( attempt == attempts )); then
      break
    fi
    sleep "$sleep_sec"
    if (( sleep_sec < 8 )); then
      sleep_sec=$((sleep_sec * 2))
    fi
    attempt=$((attempt + 1))
  done
  echo "failed to verify control config from ${url}" >&2
  return 1
}

# harness stability patch: stop previous control process before phase switch.
stop_control() {
  if [[ -f "$CONTROL_PID_FILE" ]]; then
    local pid
    pid="$(cat "$CONTROL_PID_FILE" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      sleep 1
      if kill -0 "$pid" 2>/dev/null; then
        kill -9 "$pid" 2>/dev/null || true
      fi
    fi
    rm -f "$CONTROL_PID_FILE"
  fi
  pkill -f "cmd/control.*-http-port ${CONTROL_HTTP_PORT}" >/dev/null 2>&1 || true
  pkill -f "/control -http-port ${CONTROL_HTTP_PORT}" >/dev/null 2>&1 || true
}

# harness stability patch: start control with phase timeout policy and readiness gate.
start_control() {
  local phase="$1"
  local queued_timeout="$2"
  local control_log="${RUNS_ROOT}/control_${phase}.log"
  local attempt=1

  : > "$control_log"

  while (( attempt <= START_CONTROL_RETRIES )); do
    stop_control

    if [[ ! -x "$CONTROL_BIN_PATH" ]]; then
      (
        cd "${SCRIPT_DIR}/.."
        go build -o control ./cmd/control
      ) >>"$control_log" 2>&1
    fi

    (
      cd "${SCRIPT_DIR}/.."
      MC_DB_DSN="$MC_DB_DSN" \
      CONTROL_TOKEN="$CONTROL_TOKEN" \
      RUN_TIMEOUT_SEC="$RUN_TIMEOUT_SEC" \
      QUEUED_TIMEOUT_SEC="$queued_timeout" \
      "$CONTROL_BIN_PATH" -http-port "$CONTROL_HTTP_PORT"
    ) >>"$control_log" 2>&1 &

    local pid="$!"
    echo "$pid" > "$CONTROL_PID_FILE"

    if wait_control_ready && verify_control_phase_config "$queued_timeout"; then
      CURRENT_CONTROL_PHASE="$phase"
      echo "[control] phase=${phase} pid=${pid} queued_timeout=${queued_timeout} ready=1"
      return 0
    fi

    echo "[warn] control start retry phase=${phase} attempt=${attempt}/${START_CONTROL_RETRIES}" >&2
    stop_control
    if (( attempt < START_CONTROL_RETRIES )); then
      sleep "$START_CONTROL_BACKOFF_SEC"
    fi
    attempt=$((attempt + 1))
  done

  echo "[error] control not ready for phase=${phase} queued_timeout=${queued_timeout}" >&2
  tail -n 30 "$control_log" >&2 || true
  return 1
}

ensure_phase_control() {
  local phase="$1"
  local queued_timeout="$2"
  if [[ "$CURRENT_CONTROL_PHASE" == "$phase" ]] && [[ -f "$CONTROL_PID_FILE" ]]; then
    local pid
    pid="$(cat "$CONTROL_PID_FILE" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
  fi
  start_control "$phase" "$queued_timeout"
}

check_postgres_ready() {
  local retries=5
  local i=1
  while (( i <= retries )); do
    if psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
    i=$((i + 1))
  done
  echo "postgres is not reachable after ${retries} attempts" >&2
  return 1
}

clean_incomplete_run_dirs() {
  local d run_id
  for d in "${RUNS_ROOT}"/exp1-*; do
    [[ -d "$d" ]] || continue
    if [[ ! -f "$d/summary.csv" ]]; then
      run_id="$(basename "$d")"
      rm -rf "$d"
      echo "[preflight] removed incomplete run dir: ${run_id}"
    fi
  done
}

assert_db_backlog_empty() {
  local active
  active="$(psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "
    SELECT COUNT(*)
    FROM demand_jobs
    WHERE id LIKE 'exp1-%'
      AND status IN ('queued','assigned','running');
  " | tr -d '[:space:]')"
  if [[ "$active" != "0" ]]; then
    echo "preflight failed: DB backlog is not empty (count=${active})" >&2
    psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -F, -c "
      SELECT status, COUNT(*)
      FROM demand_jobs
      WHERE id LIKE 'exp1-%'
        AND status IN ('queued','assigned','running')
      GROUP BY status
      ORDER BY status;
    " >&2 || true
    return 1
  fi
}

assert_control_port_free() {
  if lsof -nP -iTCP:"${CONTROL_HTTP_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "preflight failed: control port ${CONTROL_HTTP_PORT} already in use; start matrix_runner with a clean environment only." >&2
    return 1
  fi
}

preflight_gate() {
  check_postgres_ready
  clean_incomplete_run_dirs
  assert_db_backlog_empty
  assert_control_port_free
}

if [[ ! -f "$SUMMARY_CSV" ]]; then
  echo "run_id,timestamp_utc,start_ts,end_ts,agents,jobs,workload,failure_rate,rep,makespan_sec,throughput,total_jobs,succeeded_jobs,success_rate,p50_ms,p95_ms,p99_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_mean_ms,avg_attempts,max_attempts,avg_retry_count,max_retry_count,run_pass" > "$SUMMARY_CSV"
fi

if [[ ! -f "$SUMMARY_AGG_CSV" ]]; then
  echo "timestamp_utc,phase,workload,load_model,agents,jobs,failure_rate,repetitions,mean_success_rate,std_success_rate,mean_makespan_sec,std_makespan_sec,duration_p50_ms,duration_p95_ms,duration_p99_ms,duration_samples,mean_e2e_mean_ms,std_e2e_mean_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_samples,accepted_runs,failed_runs" > "$SUMMARY_AGG_CSV"
fi

calc_mean_std() {
  # Args: numeric values...
  printf '%s\n' "$@" | awk '
    NF {
      n += 1
      x += $1
      xx += ($1 * $1)
    }
    END {
      if (n == 0) {
        printf "0.000000,0.000000"
        exit
      }
      m = x / n
      v = (xx / n) - (m * m)
      if (v < 0) v = 0
      printf "%.6f,%.6f", m, sqrt(v)
    }
  '
}

calc_percentiles_from_file() {
  # Args: file_with_numeric_lines
  local f="$1"
  if [[ ! -s "$f" ]]; then
    printf ",,,0"
    return
  fi

  local sorted tmp n i50 i95 i99 p50 p95 p99
  sorted="$(mktemp)"
  sort -n "$f" > "$sorted"
  n="$(wc -l < "$sorted" | tr -d ' ')"

  i50=$(( (50 * n + 99) / 100 ))
  i95=$(( (95 * n + 99) / 100 ))
  i99=$(( (99 * n + 99) / 100 ))

  p50="$(sed -n "${i50}p" "$sorted")"
  p95="$(sed -n "${i95}p" "$sorted")"
  p99="$(sed -n "${i99}p" "$sorted")"
  rm -f "$sorted"

  printf "%s,%s,%s,%s" "${p50}" "${p95}" "${p99}" "${n}"
}

classify_run_failure() {
  local stderr_file="$1"
  local reason="unknown"
  if grep -qi "too many clients already" "$stderr_file"; then
    reason="db_too_many_clients"
  elif grep -qi "Can't assign requested address" "$stderr_file"; then
    reason="db_local_addr_exhausted"
  elif grep -qi "control timeout mismatch" "$stderr_file"; then
    reason="control_timeout_mismatch"
  elif grep -qi "no live agents available for failure injection" "$stderr_file"; then
    reason="no_live_agents_for_injection"
  elif grep -qi "agents not stable after" "$stderr_file"; then
    reason="agents_not_stable"
  elif grep -qi "failed to launch all agents" "$stderr_file"; then
    reason="agents_launch_partial"
  elif grep -qi "agents\\.pids: No such file or directory" "$stderr_file"; then
    reason="agents_pidfile_missing"
  elif grep -qi "failed to verify control effective config" "$stderr_file"; then
    reason="control_debug_unreachable"
  elif grep -qi "timeout waiting for jobs to leave queued/running" "$stderr_file" \
    || grep -qi "timeout waiting for jobs to leave queued/assigned/running" "$stderr_file"; then
    reason="wait_until_drained_timeout"
  fi
  printf '%s' "$reason"
}

is_transient_failure() {
  local reason="$1"
  case "$reason" in
    db_too_many_clients|db_local_addr_exhausted|no_live_agents_for_injection|agents_not_stable|agents_launch_partial|agents_pidfile_missing|control_debug_unreachable)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

has_completed_rep() {
  local workload="$1"
  local agents="$2"
  local jobs="$3"
  local failure_rate="$4"
  local rep="$5"
  [[ -f "$SUMMARY_CSV" ]] || return 1
  awk -F',' -v a="$agents" -v j="$jobs" -v w="$workload" -v f="$failure_rate" -v r="$rep" '
    $1 ~ /^exp1-/ && $5==a && $6==j && $7==w && $8==f && $9==r {
      if (f == 0) {
        found=1
      } else if ($NF == 1) {
        found=1
      }
    }
    END { exit(found ? 0 : 1) }
  ' "$SUMMARY_CSV"
}

count_recorded_runs() {
  [[ -f "$SUMMARY_CSV" ]] || { echo 0; return; }
  awk -F',' '
    $1 ~ /^exp1-/ {
      key = $5 "," $6 "," $7 "," $8 "," $9
      if ($8 == 0) {
        done[key] = 1
      } else if ($NF == 1) {
        done[key] = 1
      }
    }
    END {
      n = 0
      for (k in done) n++
      print n+0
    }
  ' "$SUMMARY_CSV"
}

validate_effective_config() {
  echo "effective_config matrix_profile=${MATRIX_PROFILE} repetitions=${REPETITIONS} crash_include_io=${CRASH_INCLUDE_IO} ttl_sec=${TTL_SEC} heartbeat_sec=${HEARTBEAT_SEC} run_timeout_sec=${RUN_TIMEOUT_SEC} queued_timeout_sec=${QUEUED_TIMEOUT_SEC}"
  [[ -n "${CONTROL_URL:-}" ]] || { echo "CONTROL_URL must be set" >&2; exit 1; }
  [[ -n "${MC_DB_DSN:-}" ]] || { echo "MC_DB_DSN must be set" >&2; exit 1; }
  [[ -n "${CONTROL_TOKEN:-}" ]] || { echo "CONTROL_TOKEN must be set (required for /debug/config verification)" >&2; exit 1; }

  if [[ "$ALLOW_MUTABLE_MATRIX" != "1" ]]; then
    [[ "$MATRIX_PROFILE" == "final105" ]] || { echo "MATRIX_PROFILE must be final105 (set ALLOW_MUTABLE_MATRIX=1 to bypass)" >&2; exit 1; }
    [[ "$CRASH_INCLUDE_IO" == "0" ]] || { echo "CRASH_INCLUDE_IO must be 0 (set ALLOW_MUTABLE_MATRIX=1 to bypass)" >&2; exit 1; }
  fi

  [[ "$REPETITIONS" == "5" ]] || { echo "REPETITIONS must be 5 for final runs" >&2; exit 1; }
  [[ "$TTL_SEC" == "10" ]] || { echo "TTL_SEC must be 10 for final runs" >&2; exit 1; }
  [[ "$HEARTBEAT_SEC" == "3" ]] || { echo "HEARTBEAT_SEC must be 3 for final runs" >&2; exit 1; }
  [[ "$RUN_TIMEOUT_SEC" == "600" ]] || { echo "RUN_TIMEOUT_SEC must be 600 for final runs" >&2; exit 1; }
  [[ "$QUEUED_TIMEOUT_SEC" == "180" ]] || { echo "QUEUED_TIMEOUT_SEC must be 180 for final runs" >&2; exit 1; }
}

collect_run_ids_for_cell() {
  local workload="$1"
  local agents="$2"
  local jobs="$3"
  local failure_rate="$4"
  [[ -f "$SUMMARY_CSV" ]] || return 0
  awk -F',' -v a="$agents" -v j="$jobs" -v w="$workload" -v f="$failure_rate" '
    $1 ~ /^exp1-/ && $5==a && $6==j && $7==w && $8==f {
      by_rep[$9]=$1
    }
    END {
      for (rep in by_rep) print by_rep[rep]
    }
  ' "$SUMMARY_CSV"
}

total_expected_runs() {
  if [[ "$MATRIX_PROFILE" == "legacy" ]]; then
    echo $((4 * 2 * REPETITIONS))
    return
  fi
  if [[ "$CRASH_INCLUDE_IO" == "1" ]]; then
    echo 150
    return
  fi
  echo 105
}

aggregate_cell() {
  local phase="$1"
  local workload="$2"
  local load_model="$3"
  local agents="$4"
  local jobs="$5"
  local failure_rate="$6"
  shift 6
  local run_ids=("$@")

  local success_rates=()
  local makespans=()
  local e2e_means=()
  local accepted_runs=0
  local failed_runs=0
  local rid row
  for rid in "${run_ids[@]}"; do
    row="$(grep "^${rid}," "$SUMMARY_CSV" | tail -n 1)"
    [[ -z "$row" ]] && continue
    IFS=',' read -r \
      _run_id _ts _start _end _agents _jobs _wl _fr _rep \
      makespan_sec _throughput _total _succ success_rate \
      _p50 _p95 _p99 \
      _e2e_p50 _e2e_p95 _e2e_p99 e2e_mean_ms \
      _avg_attempts _max_attempts _avg_retry_count _max_retry_count run_pass \
      <<< "$row"
    if [[ "${run_pass}" != "1" ]]; then
      failed_runs=$((failed_runs + 1))
      continue
    fi
    accepted_runs=$((accepted_runs + 1))
    success_rates+=("$success_rate")
    makespans+=("$makespan_sec")
    e2e_means+=("$e2e_mean_ms")
  done

  if (( failed_runs > 0 )); then
    echo "[warn] aggregate excludes non-passing runs phase=${phase} workload=${workload} load=${load_model} agents=${agents} jobs=${jobs} failure=${failure_rate} failed_runs=${failed_runs} accepted_runs=${accepted_runs}" >&2
  fi

  local success_stats makespan_stats e2e_mean_stats
  if (( accepted_runs > 0 )); then
    success_stats="$(calc_mean_std "${success_rates[@]}")"
    makespan_stats="$(calc_mean_std "${makespans[@]}")"
    e2e_mean_stats="$(calc_mean_std "${e2e_means[@]}")"
  else
    success_stats="0.000000,0.000000"
    makespan_stats="0.000000,0.000000"
    e2e_mean_stats="0.000000,0.000000"
  fi
  local mean_success_rate std_success_rate mean_makespan_sec std_makespan_sec mean_e2e_mean_ms std_e2e_mean_ms
  mean_success_rate="${success_stats%,*}"
  std_success_rate="${success_stats#*,}"
  mean_makespan_sec="${makespan_stats%,*}"
  std_makespan_sec="${makespan_stats#*,}"
  mean_e2e_mean_ms="${e2e_mean_stats%,*}"
  std_e2e_mean_ms="${e2e_mean_stats#*,}"

  local duration_samples_file e2e_samples_file
  duration_samples_file="$(mktemp)"
  e2e_samples_file="$(mktemp)"
  for rid in "${run_ids[@]}"; do
    local metrics_csv
    metrics_csv="${RUNS_ROOT}/${rid}/job_metrics.csv"
    if [[ -f "$metrics_csv" ]]; then
      awk -F',' 'NR > 1 && $6 != "" {print $6}' "$metrics_csv" >> "$duration_samples_file"
      awk -F',' 'NR > 1 && $7 != "" {print $7}' "$metrics_csv" >> "$e2e_samples_file"
    fi
  done

  local duration_stats e2e_stats
  duration_stats="$(calc_percentiles_from_file "$duration_samples_file")"
  e2e_stats="$(calc_percentiles_from_file "$e2e_samples_file")"
  rm -f "$duration_samples_file" "$e2e_samples_file"

  local duration_p50_ms duration_p95_ms duration_p99_ms duration_samples
  local e2e_p50_ms e2e_p95_ms e2e_p99_ms e2e_samples
  duration_p50_ms="$(printf '%s' "$duration_stats" | cut -d',' -f1)"
  duration_p95_ms="$(printf '%s' "$duration_stats" | cut -d',' -f2)"
  duration_p99_ms="$(printf '%s' "$duration_stats" | cut -d',' -f3)"
  duration_samples="$(printf '%s' "$duration_stats" | cut -d',' -f4)"
  e2e_p50_ms="$(printf '%s' "$e2e_stats" | cut -d',' -f1)"
  e2e_p95_ms="$(printf '%s' "$e2e_stats" | cut -d',' -f2)"
  e2e_p99_ms="$(printf '%s' "$e2e_stats" | cut -d',' -f3)"
  e2e_samples="$(printf '%s' "$e2e_stats" | cut -d',' -f4)"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$phase" \
    "$workload" \
    "$load_model" \
    "$agents" \
    "$jobs" \
    "$failure_rate" \
    "$REPETITIONS" \
    "$mean_success_rate" \
    "$std_success_rate" \
    "$mean_makespan_sec" \
    "$std_makespan_sec" \
    "$duration_p50_ms" \
    "$duration_p95_ms" \
    "$duration_p99_ms" \
    "$duration_samples" \
    "$mean_e2e_mean_ms" \
    "$std_e2e_mean_ms" \
    "$e2e_p50_ms" \
    "$e2e_p95_ms" \
    "$e2e_p99_ms" \
    "$e2e_samples" \
    "$accepted_runs" \
    "$failed_runs" >> "$SUMMARY_AGG_CSV"
}

run_cell() {
  local phase="$1"
  local workload="$2"
  local load_model="$3"
  local agents="$4"
  local jobs="$5"
  local failure_rate="$6"

  local run_ids=()
  local rep row run_id
  local new_runs_in_cell=0
  local cell_failed_runs=0
  local queued_timeout_for_phase="$QUEUED_TIMEOUT_SEC"
  if [[ "$phase" == "baseline" ]]; then
    queued_timeout_for_phase=600
  fi
  for rep in $(seq 1 "$REPETITIONS"); do
    if has_completed_rep "$workload" "$agents" "$jobs" "$failure_rate" "$rep"; then
      echo "[skip] phase=${phase} workload=${workload} load=${load_model} agents=${agents} jobs=${jobs} failure=${failure_rate} rep=${rep} already recorded"
      continue
    fi

    current_run_no=$((run_count + 1))
    echo "[${current_run_no}/${expected_runs}] phase=${phase} workload=${workload} load=${load_model} agents=${agents} jobs=${jobs} failure=${failure_rate} rep=${rep} start"
    local attempt=1
    local run_rc=1
    local reason=""
    while (( attempt <= MAX_RUN_RETRIES + 1 )); do
      # harness stability patch: re-validate per-run control/db readiness.
      ensure_phase_control "$phase" "$queued_timeout_for_phase"
      check_postgres_ready
      # harness stability patch: force-clean stray agents before every launch attempt.
      pkill -f "/agent -ns" >/dev/null 2>&1 || true
      rm -f "$AGENT_PIDS_FILE" >/dev/null 2>&1 || true
      "${SCRIPT_DIR}/run_agents.sh" stop >/dev/null 2>&1 || true
      local start_stderr
      start_stderr="$(mktemp)"
      set +e
      AGENTS="$agents" REP="$rep" FAILURE_RATE="$failure_rate" \
        QUEUED_TIMEOUT_SEC="$queued_timeout_for_phase" \
        REPETITIONS="$REPETITIONS" MATRIX_PROFILE="$MATRIX_PROFILE" \
        MATRIX_PHASE="$phase" \
        A="$agents" N="$jobs" WORKLOAD_TYPE="$workload" \
        MATRIX_RUNNER_ACTIVE=1 \
        STRICT_FINAL="${STRICT_FINAL:-1}" \
        "${SCRIPT_DIR}/run_agents.sh" start > "${RUNS_ROOT}/agents_launcher.log" 2> "$start_stderr"
      run_rc=$?
      set -e
      if (( run_rc != 0 )); then
        reason="$(classify_run_failure "$start_stderr")"
        {
          printf '[%s] run_failed phase=%s workload=%s load=%s agents=%s jobs=%s failure=%s rep=%s attempt=%s/%s reason=%s\n' \
            "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$phase" "$workload" "$load_model" "$agents" "$jobs" "$failure_rate" "$rep" "$attempt" "$((MAX_RUN_RETRIES + 1))" "$reason"
          sed -n '1,80p' "$start_stderr"
        } >> "$FAILURE_ANALYSIS_LOG"
        rm -f "$start_stderr"
        if is_transient_failure "$reason" && (( attempt <= MAX_RUN_RETRIES )); then
          echo "[warn] transient failure (${reason}) retrying in ${RETRY_BACKOFF_SEC}s phase=${phase} workload=${workload} agents=${agents} jobs=${jobs} failure=${failure_rate} rep=${rep} attempt=${attempt}" >&2
          sleep "$RETRY_BACKOFF_SEC"
          attempt=$((attempt + 1))
          continue
        fi
        break
      fi
      rm -f "$start_stderr"
      local stderr_file
      stderr_file="$(mktemp)"
      set +e
      row="$(AGENTS="$agents" REP="$rep" FAILURE_RATE="$failure_rate" \
        QUEUED_TIMEOUT_SEC="$queued_timeout_for_phase" \
        REPETITIONS="$REPETITIONS" MATRIX_PROFILE="$MATRIX_PROFILE" \
        MATRIX_PHASE="$phase" \
        A="$agents" N="$jobs" WORKLOAD_TYPE="$workload" \
        MATRIX_RUNNER_ACTIVE=1 \
        STRICT_FINAL="${STRICT_FINAL:-1}" \
        "${SCRIPT_DIR}/run_matrix.sh" "$jobs" "$workload" 2> "$stderr_file")"
      run_rc=$?
      set -e
      "${SCRIPT_DIR}/run_agents.sh" stop >/dev/null 2>&1 || true
      if (( run_rc == 0 )); then
        rm -f "$stderr_file"
        break
      fi

      reason="$(classify_run_failure "$stderr_file")"
      {
        printf '[%s] run_failed phase=%s workload=%s load=%s agents=%s jobs=%s failure=%s rep=%s attempt=%s/%s reason=%s\n' \
          "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$phase" "$workload" "$load_model" "$agents" "$jobs" "$failure_rate" "$rep" "$attempt" "$((MAX_RUN_RETRIES + 1))" "$reason"
        sed -n '1,80p' "$stderr_file"
      } >> "$FAILURE_ANALYSIS_LOG"
      rm -f "$stderr_file"

      if is_transient_failure "$reason" && (( attempt <= MAX_RUN_RETRIES )); then
        echo "[warn] transient failure (${reason}) retrying in ${RETRY_BACKOFF_SEC}s phase=${phase} workload=${workload} agents=${agents} jobs=${jobs} failure=${failure_rate} rep=${rep} attempt=${attempt}" >&2
        sleep "$RETRY_BACKOFF_SEC"
        attempt=$((attempt + 1))
        continue
      fi
      break
    done
    if (( run_rc != 0 )); then
      echo "[error] run_matrix failed phase=${phase} workload=${workload} load=${load_model} agents=${agents} jobs=${jobs} failure=${failure_rate} rep=${rep} reason=${reason}; agents cleaned up (see ${FAILURE_ANALYSIS_LOG})" >&2
      cell_failed_runs=$((cell_failed_runs + 1))
      if [[ "$NON_FATAL_RUN_FAILURES" != "1" ]]; then
        return "$run_rc"
      fi
      sleep "$RUN_FAILURE_COOLDOWN_SEC"
      continue
    fi
    printf '%s\n' "$row" >> "$SUMMARY_CSV"
    run_count=$((run_count + 1))
    new_runs_in_cell=$((new_runs_in_cell + 1))
    run_id="${row%%,*}"
    echo "[${run_count}/${expected_runs}] run_id=${run_id} completed"
  done

  if (( new_runs_in_cell == 0 )); then
    if (( cell_failed_runs > 0 )); then
      echo "[warn] no successful new runs in cell phase=${phase} workload=${workload} load=${load_model} agents=${agents} jobs=${jobs} failure=${failure_rate} failed_runs=${cell_failed_runs}" >&2
    fi
    echo "[skip-aggregate] phase=${phase} workload=${workload} load=${load_model} agents=${agents} jobs=${jobs} failure=${failure_rate} no new runs"
    return
  fi

  while IFS= read -r rid; do
    [[ -n "$rid" ]] && run_ids+=("$rid")
  done < <(collect_run_ids_for_cell "$workload" "$agents" "$jobs" "$failure_rate")

  aggregate_cell "$phase" "$workload" "$load_model" "$agents" "$jobs" "$failure_rate" "${run_ids[@]}"
  echo "[aggregate] phase=${phase} workload=${workload} load=${load_model} agents=${agents} jobs=${jobs} failure=${failure_rate} done"
}

run_count=0
validate_effective_config
preflight_gate
expected_runs="$(total_expected_runs)"
run_count="$(count_recorded_runs)"

case "$MATRIX_PROFILE" in
  final105)
    ensure_phase_control baseline 600
    for agents in "${AGENT_VALUES[@]}"; do
      for workload in cpu io; do
        run_cell baseline "$workload" balanced "$agents" "$agents" 0
        run_cell baseline "$workload" overload "$agents" "$((5 * agents))" 0
      done
    done

    ensure_phase_control crash 180
    for agents in "${AGENT_VALUES[@]}"; do
      for failure_rate in "${CRASH_RATES[@]}"; do
        run_cell crash cpu overload "$agents" "$((5 * agents))" "$failure_rate"
      done
    done

    if [[ "$CRASH_INCLUDE_IO" == "1" ]]; then
      for agents in "${AGENT_VALUES[@]}"; do
        for failure_rate in "${CRASH_RATES[@]}"; do
          run_cell crash io overload "$agents" "$((5 * agents))" "$failure_rate"
        done
      done
    fi
    ;;
  legacy)
    for n in 50 100 200 500; do
      for workload in cpu io; do
        run_cell legacy "$workload" legacy "$AGENTS" "$n" "$FAILURE_RATE"
      done
    done
    ;;
  *)
    echo "unknown MATRIX_PROFILE=${MATRIX_PROFILE} (use final105 or legacy)" >&2
    exit 1
    ;;
esac

if (( run_count != expected_runs )); then
  echo "matrix run count mismatch: actual=${run_count} expected=${expected_runs}" >&2
  exit 1
fi

echo "matrix complete: profile=${MATRIX_PROFILE} runs=${run_count}"
echo "summary: ${SUMMARY_CSV}"
echo "aggregated summary: ${SUMMARY_AGG_CSV}"
