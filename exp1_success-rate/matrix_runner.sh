#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

N_VALUES=(50 100 200 500)
WORKLOAD_VALUES=(cpu io)
REPETITIONS=5
SUMMARY_AGG_CSV="${SUMMARY_AGG_CSV:-${SCRIPT_DIR}/summary_aggregated.csv}"

mkdir -p "$RUNS_ROOT"

if [[ -z "${AGENT_PIDS_FILE:-}" ]]; then
  AGENT_PIDS_FILE="${RUNS_ROOT}/agents.pids"
fi
export AGENT_PIDS_FILE

cleanup_agents() {
  "${SCRIPT_DIR}/run_agents.sh" stop >/dev/null 2>&1 || true
}
trap cleanup_agents EXIT INT TERM

if [[ ! -f "$SUMMARY_CSV" ]]; then
  echo "run_id,timestamp_utc,start_ts,end_ts,agents,jobs,workload,failure_rate,rep,makespan_sec,throughput,total_jobs,succeeded_jobs,success_rate,p50_ms,p95_ms,p99_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_mean_ms,avg_attempts,max_attempts,avg_retry_count,max_retry_count" > "$SUMMARY_CSV"
fi

if [[ ! -f "$SUMMARY_AGG_CSV" ]]; then
  echo "timestamp_utc,agents,failure_rate,jobs,workload,repetitions,mean_success_rate,std_success_rate,mean_makespan_sec,std_makespan_sec,duration_p50_ms,duration_p95_ms,duration_p99_ms,duration_samples,mean_e2e_mean_ms,std_e2e_mean_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_samples" > "$SUMMARY_AGG_CSV"
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

for n in "${N_VALUES[@]}"; do
  for workload in "${WORKLOAD_VALUES[@]}"; do
    run_ids=()
    success_rates=()
    makespans=()
    e2e_means=()

    for rep in $(seq 1 "$REPETITIONS"); do
      "${SCRIPT_DIR}/run_agents.sh" start > "${RUNS_ROOT}/agents_launcher.log" 2>&1
      row="$(AGENTS="$AGENTS" REP="$rep" "${SCRIPT_DIR}/run_matrix.sh" "$n" "$workload")"
      "${SCRIPT_DIR}/run_agents.sh" stop >/dev/null 2>&1 || true
      printf '%s\n' "$row" >> "$SUMMARY_CSV"

      IFS=',' read -r \
        run_id _ts _start _end _agents _jobs _wl _fr _rep \
        makespan_sec _throughput _total _succ success_rate \
        _p50 _p95 _p99 \
        _e2e_p50 _e2e_p95 _e2e_p99 e2e_mean_ms \
        _avg_attempts _max_attempts _avg_retry_count _max_retry_count \
        <<< "$row"
      run_ids+=("$run_id")
      success_rates+=("$success_rate")
      makespans+=("$makespan_sec")
      e2e_means+=("$e2e_mean_ms")
    done

    success_stats="$(calc_mean_std "${success_rates[@]}")"
    makespan_stats="$(calc_mean_std "${makespans[@]}")"
    mean_success_rate="${success_stats%,*}"
    std_success_rate="${success_stats#*,}"
    mean_makespan_sec="${makespan_stats%,*}"
    std_makespan_sec="${makespan_stats#*,}"
    e2e_mean_stats="$(calc_mean_std "${e2e_means[@]}")"
    mean_e2e_mean_ms="${e2e_mean_stats%,*}"
    std_e2e_mean_ms="${e2e_mean_stats#*,}"

    duration_samples_file="$(mktemp)"
    e2e_samples_file="$(mktemp)"
    for rid in "${run_ids[@]}"; do
      metrics_csv="${RUNS_ROOT}/${rid}/job_metrics.csv"
      if [[ -f "$metrics_csv" ]]; then
        awk -F',' 'NR > 1 && $6 != "" {print $6}' "$metrics_csv" >> "$duration_samples_file"
        awk -F',' 'NR > 1 && $7 != "" {print $7}' "$metrics_csv" >> "$e2e_samples_file"
      fi
    done

    duration_stats="$(calc_percentiles_from_file "$duration_samples_file")"
    rm -f "$duration_samples_file"
    duration_p50_ms="$(printf '%s' "$duration_stats" | cut -d',' -f1)"
    duration_p95_ms="$(printf '%s' "$duration_stats" | cut -d',' -f2)"
    duration_p99_ms="$(printf '%s' "$duration_stats" | cut -d',' -f3)"
    duration_samples="$(printf '%s' "$duration_stats" | cut -d',' -f4)"

    e2e_stats="$(calc_percentiles_from_file "$e2e_samples_file")"
    rm -f "$e2e_samples_file"
    e2e_p50_ms="$(printf '%s' "$e2e_stats" | cut -d',' -f1)"
    e2e_p95_ms="$(printf '%s' "$e2e_stats" | cut -d',' -f2)"
    e2e_p99_ms="$(printf '%s' "$e2e_stats" | cut -d',' -f3)"
    e2e_samples="$(printf '%s' "$e2e_stats" | cut -d',' -f4)"

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "$AGENTS" \
      "$FAILURE_RATE" \
      "$n" \
      "$workload" \
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
      "$e2e_samples" >> "$SUMMARY_AGG_CSV"
  done
done

echo "matrix complete: ${SUMMARY_CSV}"
echo "aggregated summary: ${SUMMARY_AGG_CSV}"
