#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

SUMMARY_CSV="${SUMMARY_CSV:-${EXP1_ROOT}/raw/summary.csv}"
SUMMARY_AGG_CSV="${SUMMARY_AGG_CSV:-${EXP1_ROOT}/curated/summary_aggregated.csv}"
RESULTS_CSV="${RESULTS_CSV:-${EXP1_ROOT}/raw/results.csv}"
FILTERED_RESULTS_CSV="${EXP1_ROOT}/curated/filtered_results.csv"
ANOMALY_CSV="${EXP1_ROOT}/curated/anomaly_runs.csv"
DIAG_CSV="${EXP1_ROOT}/curated/anomaly_diagnosis.csv"

if [[ ! -f "$SUMMARY_CSV" ]]; then
  echo "missing summary file: $SUMMARY_CSV" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

means_file="${tmpdir}/means.tsv"
clean_ids="${tmpdir}/clean_ids.txt"
anomaly_ids="${tmpdir}/anomaly_ids.txt"
clean_summary="${tmpdir}/clean_summary.csv"
group_rows="${tmpdir}/group_rows.tsv"

is_nan_like() {
  local v="${1:-}"
  local lv
  [[ -z "$v" ]] && return 0
  lv="$(printf '%s' "$v" | tr '[:upper:]' '[:lower:]')"
  case "$lv" in
    nan|inf|-inf) return 0 ;;
    *) return 1 ;;
  esac
}

float_ge() {
  local a="$1"
  local b="$2"
  awk -v x="$a" -v y="$b" 'BEGIN { exit !(x >= y) }'
}

float_lt() {
  local a="$1"
  local b="$2"
  awk -v x="$a" -v y="$b" 'BEGIN { exit !(x < y) }'
}

float_mul() {
  local a="$1"
  local b="$2"
  awk -v x="$a" -v y="$b" 'BEGIN { printf "%.9f", x*y }'
}

calc_mean_std_list() {
  # input: newline-delimited numerics via stdin
  awk '
    NF {
      n += 1
      s += $1
      ss += ($1 * $1)
    }
    END {
      if (n == 0) {
        printf "0.000000,0.000000"
        exit
      }
      m = s / n
      v = (ss / n) - (m * m)
      if (v < 0) v = 0
      printf "%.6f,%.6f", m, sqrt(v)
    }
  '
}

calc_percentiles_file() {
  local f="$1"
  if [[ ! -s "$f" ]]; then
    printf ",,,0"
    return
  fi
  local sorted n i50 i95 i99 p50 p95 p99
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
  printf "%s,%s,%s,%s" "$p50" "$p95" "$p99" "$n"
}

phase_for_row() {
  local failure_rate="$1"
  if [[ "$failure_rate" -gt 0 ]]; then
    printf "crash"
  else
    printf "baseline"
  fi
}

load_model_for_row() {
  local jobs="$1"
  local agents="$2"
  if [[ "$jobs" -eq "$agents" ]]; then
    printf "balanced"
  else
    printf "overload"
  fi
}

# Mean makespan per 동일 조건(agents,jobs,workload,failure_rate)
awk -F',' '
  NR > 1 && $1 ~ /^exp1-/ {
    key = $5 "|" $6 "|" $7 "|" $8
    sum[key] += $10
    cnt[key] += 1
  }
  END {
    for (k in sum) printf "%s\t%.9f\n", k, sum[k] / cnt[k]
  }
' "$SUMMARY_CSV" > "$means_file"

echo "run_id,rules,success_rate,makespan_sec,mean_makespan_same_cond,jobs,failure_rate,p50_ms,p95_ms,p99_ms,e2e_p50_ms,e2e_mean_ms,avg_retry_count" > "$ANOMALY_CSV"

while IFS=, read -r \
  run_id timestamp_utc start_ts end_ts agents jobs workload failure_rate rep \
  makespan_sec throughput total_jobs succeeded_jobs success_rate \
  p50_ms p95_ms p99_ms \
  e2e_p50_ms e2e_p95_ms e2e_p99_ms e2e_mean_ms \
  avg_attempts max_attempts avg_retry_count max_retry_count; do

  [[ "$run_id" == "run_id" ]] && continue
  [[ "$run_id" =~ ^exp1- ]] || continue

  key="${agents}|${jobs}|${workload}|${failure_rate}"
  mean_makespan="$(awk -F'\t' -v k="$key" '$1==k{print $2; found=1; exit} END{if(!found) print "0"}' "$means_file")"
  rules=()

  # Rule 1
  if [[ "$failure_rate" == "0" ]] && float_lt "${success_rate:-0}" "0.95"; then
    rules+=("R1_base_sr_lt_0.95")
  fi

  # Rule 2
  thrice_mean="$(float_mul "$mean_makespan" "3")"
  if float_ge "${makespan_sec:-0}" "$thrice_mean"; then
    rules+=("R2_makespan_ge_3x_mean")
  fi

  # Rule 3
  if [[ "${jobs:-0}" -le 10 ]] && float_ge "${makespan_sec:-0}" "60"; then
    rules+=("R3_small_job_makespan_gt_60")
  fi

  # Rule 4
  if ! is_nan_like "$p50_ms" && float_ge "${p50_ms:-0}" "0.000001"; then
    two_p50="$(float_mul "$p50_ms" "2")"
    if { ! is_nan_like "$e2e_p50_ms" && float_ge "${e2e_p50_ms:-0}" "$two_p50"; } || \
       { ! is_nan_like "$e2e_mean_ms" && float_ge "${e2e_mean_ms:-0}" "$two_p50"; }; then
      rules+=("R4_e2e_ge_2x_duration_p50")
    fi
  fi

  # Rule 5
  run_dir="${RUNS_ROOT}/${run_id}"
  if [[ "${avg_retry_count:-0}" == "0.000000" || "${avg_retry_count:-0}" == "0" ]]; then
    if [[ -d "$run_dir" ]] && grep -Rqs "lease_expired" "$run_dir"; then
      rules+=("R5_retry0_but_lease_expired_log")
    fi
  fi

  # Rule 6
  if is_nan_like "$p95_ms" || is_nan_like "$p99_ms"; then
    rules+=("R6_p95_or_p99_nan")
  fi

  if [[ "${#rules[@]}" -gt 0 ]]; then
    rule_join="$(IFS='|'; echo "${rules[*]}")"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$run_id" "$rule_join" "${success_rate:-}" "${makespan_sec:-}" "$mean_makespan" \
      "${jobs:-}" "${failure_rate:-}" "${p50_ms:-}" "${p95_ms:-}" "${p99_ms:-}" \
      "${e2e_p50_ms:-}" "${e2e_mean_ms:-}" "${avg_retry_count:-}" >> "$ANOMALY_CSV"
    printf '%s\n' "$run_id" >> "$anomaly_ids"
  else
    printf '%s\n' "$run_id" >> "$clean_ids"
  fi
done < "$SUMMARY_CSV"

sort -u "$anomaly_ids" -o "$anomaly_ids" 2>/dev/null || true
sort -u "$clean_ids" -o "$clean_ids" 2>/dev/null || true

# Build filtered results without modifying original results.csv
if [[ -f "$RESULTS_CSV" ]]; then
  head -n 1 "$RESULTS_CSV" > "$FILTERED_RESULTS_CSV"
  awk -F',' 'NR==FNR{bad[$1]=1; next} NR>1 { if(!($1 in bad)) print }' "$anomaly_ids" "$RESULTS_CSV" >> "$FILTERED_RESULTS_CSV"
fi

echo "run_id,started_with_backlog,backlog_queued,backlog_assigned,active_claim_agents,timeout_reason_seen,e2e_submit_ts_logic,metrics_merge_failure,possible_causes" > "$DIAG_CSV"

lease_map_file="${tmpdir}/lease_map.csv"
if [[ -d "${RUNS_ROOT}/agents" ]]; then
  grep -Rhs "\"event\":\"lease_acquired\"" "${RUNS_ROOT}/agents"/agent_*.log 2>/dev/null \
    | sed -n -E 's/.*"job_id":"([^"]+)".*"agent_id":"([^"]+)".*/\1,\2/p' \
    | sed -E 's/-[0-9]+,/,/' \
    | sort -u > "$lease_map_file" || true
fi

db_available="no"
if [[ -n "${MC_DB_DSN:-}" ]]; then
  if psql "$MC_DB_DSN" -At -v ON_ERROR_STOP=1 -c "SELECT 1;" >/dev/null 2>&1; then
    db_available="yes"
  fi
fi

while IFS= read -r run_id; do
  [[ -n "$run_id" ]] || continue
  run_dir="${RUNS_ROOT}/${run_id}"
  control_snapshot="${run_dir}/control_snapshot.log"
  job_metrics="${run_dir}/job_metrics.csv"

  backlog_q="unknown"
  backlog_a="unknown"
  started_with_backlog="unknown"
  if [[ -f "$control_snapshot" ]]; then
    backlog_q="$(grep -Eo '"queued":[0-9]+' "$control_snapshot" | tail -n1 | cut -d: -f2 || true)"
    backlog_a="$(grep -Eo '"assigned":[0-9]+' "$control_snapshot" | tail -n1 | cut -d: -f2 || true)"
    if [[ -n "$backlog_q" && -n "$backlog_a" ]]; then
      if [[ "$backlog_q" -gt 0 || "$backlog_a" -gt 0 ]]; then
        started_with_backlog="yes"
      else
        started_with_backlog="no"
      fi
    fi
  fi

  active_claim_agents="unknown"
  if [[ -f "$lease_map_file" ]]; then
    active_claim_agents="$(awk -F',' -v r="$run_id" '$1==r {print $2}' "$lease_map_file" | sort -u | wc -l | tr -d ' ')"
  fi

  timeout_reason_seen="unknown"
  if [[ "$db_available" == "yes" ]]; then
    timeout_reason_seen="$(psql "$MC_DB_DSN" -At -v ON_ERROR_STOP=1 -c "
      SELECT COUNT(*)
      FROM demand_jobs
      WHERE id LIKE '${run_id}%'
        AND (
          metrics->>'reason' IN ('run_timeout','queued_timeout')
          OR status = 'failed'
        );
    " 2>/dev/null || echo unknown)"
    if [[ "$timeout_reason_seen" =~ ^[0-9]+$ ]]; then
      if [[ "$timeout_reason_seen" -gt 0 ]]; then
        timeout_reason_seen="yes"
      else
        timeout_reason_seen="no"
      fi
    fi
  fi

  e2e_submit_ts_logic="yes(run_matrix.sh uses exec_end_ts-submit_ts)"

  metrics_merge_failure="no"
  if [[ -f "$job_metrics" ]]; then
    non_empty_duration="$(awk -F',' 'NR>1 && $6 != "" {n++} END{print n+0}' "$job_metrics")"
    total_rows="$(awk -F',' 'NR>1 {n++} END{print n+0}' "$job_metrics")"
    if [[ "$total_rows" -gt 0 && "$non_empty_duration" -eq 0 ]]; then
      metrics_merge_failure="yes(duration_ms empty for all jobs)"
    fi
  else
    metrics_merge_failure="yes(job_metrics.csv missing)"
  fi

  causes=()
  [[ "$started_with_backlog" == "yes" ]] && causes+=("start_backlog")
  if [[ "$active_claim_agents" =~ ^[0-9]+$ ]] && [[ "$active_claim_agents" -lt 3 ]]; then
    causes+=("low_active_agents")
  fi
  [[ "$timeout_reason_seen" == "yes" ]] && causes+=("timeout_path")
  [[ "$metrics_merge_failure" == yes* ]] && causes+=("metrics_merge_issue")
  if [[ "${#causes[@]}" -eq 0 ]]; then
    causes+=("check_agent_bootstrap_and_failure_injection_interaction")
  fi
  causes_join="$(IFS='|'; echo "${causes[*]}")"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$run_id" "$started_with_backlog" "${backlog_q:-unknown}" "${backlog_a:-unknown}" \
    "$active_claim_agents" "$timeout_reason_seen" "$e2e_submit_ts_logic" \
    "$metrics_merge_failure" "$causes_join" >> "$DIAG_CSV"
done < "$anomaly_ids"

# Build clean summary (exclude anomalies)
head -n 1 "$SUMMARY_CSV" > "$clean_summary"
awk -F',' 'NR==FNR{bad[$1]=1; next} NR>1 { if(!($1 in bad)) print }' "$anomaly_ids" "$SUMMARY_CSV" >> "$clean_summary"

# Rebuild summary_aggregated.csv excluding anomalies
if [[ -f "$SUMMARY_AGG_CSV" ]]; then
  cp "$SUMMARY_AGG_CSV" "${SUMMARY_AGG_CSV}.with_anomaly.bak"
fi
echo "timestamp_utc,phase,workload,load_model,agents,jobs,failure_rate,repetitions,mean_success_rate,std_success_rate,mean_makespan_sec,std_makespan_sec,duration_p50_ms,duration_p95_ms,duration_p99_ms,duration_samples,mean_e2e_mean_ms,std_e2e_mean_ms,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,e2e_samples" > "$SUMMARY_AGG_CSV"

tail -n +2 "$clean_summary" | while IFS=, read -r \
  run_id timestamp_utc start_ts end_ts agents jobs workload failure_rate rep \
  makespan_sec throughput total_jobs succeeded_jobs success_rate \
  p50_ms p95_ms p99_ms \
  e2e_p50_ms e2e_p95_ms e2e_p99_ms e2e_mean_ms \
  avg_attempts max_attempts avg_retry_count max_retry_count; do
  [[ "$run_id" =~ ^exp1- ]] || continue
  [[ "$agents" =~ ^[0-9]+$ ]] || continue
  [[ "$jobs" =~ ^[0-9]+$ ]] || continue
  [[ "$failure_rate" =~ ^[0-9]+$ ]] || continue
  phase="$(phase_for_row "${failure_rate:-0}")"
  load_model="$(load_model_for_row "${jobs:-0}" "${agents:-0}")"
  key="${phase}|${workload}|${load_model}|${agents}|${jobs}|${failure_rate}"
  printf '%s\t%s\n' "$key" "$run_id" >> "$group_rows"
done

if [[ -f "$group_rows" ]]; then
  cut -f1 "$group_rows" | sort -u | while IFS= read -r key; do
    [[ -n "$key" ]] || continue
    IFS='|' read -r phase workload load_model agents jobs failure_rate <<< "$key"

    runs_for_group="${tmpdir}/runs_${phase}_${workload}_${load_model}_${agents}_${jobs}_${failure_rate}.txt"
    awk -F'\t' -v k="$key" '$1==k {print $2}' "$group_rows" > "$runs_for_group"
    repetitions="$(wc -l < "$runs_for_group" | tr -d ' ')"

    sr_file="${tmpdir}/sr_${agents}_${jobs}_${failure_rate}_${workload}.txt"
    mk_file="${tmpdir}/mk_${agents}_${jobs}_${failure_rate}_${workload}.txt"
    e2e_mean_file="${tmpdir}/e2e_mean_${agents}_${jobs}_${failure_rate}_${workload}.txt"
    dur_samples="${tmpdir}/dur_s_${agents}_${jobs}_${failure_rate}_${workload}.txt"
    e2e_samples="${tmpdir}/e2e_s_${agents}_${jobs}_${failure_rate}_${workload}.txt"
    : > "$sr_file"
    : > "$mk_file"
    : > "$e2e_mean_file"
    : > "$dur_samples"
    : > "$e2e_samples"

    while IFS= read -r rid; do
      [[ -n "$rid" ]] || continue
      awk -F',' -v r="$rid" 'NR>1 && $1==r {print $14}' "$clean_summary" >> "$sr_file"
      awk -F',' -v r="$rid" 'NR>1 && $1==r {print $10}' "$clean_summary" >> "$mk_file"
      awk -F',' -v r="$rid" 'NR>1 && $1==r {print $21}' "$clean_summary" >> "$e2e_mean_file"
      jm="${RUNS_ROOT}/${rid}/job_metrics.csv"
      if [[ -f "$jm" ]]; then
        awk -F',' 'NR>1 && $6 != "" {print $6}' "$jm" >> "$dur_samples"
        awk -F',' 'NR>1 && $7 != "" {print $7}' "$jm" >> "$e2e_samples"
      fi
    done < "$runs_for_group"

    sr_stats="$(calc_mean_std_list < "$sr_file")"
    mk_stats="$(calc_mean_std_list < "$mk_file")"
    e2e_mean_stats="$(calc_mean_std_list < "$e2e_mean_file")"
    mean_sr="${sr_stats%,*}"
    std_sr="${sr_stats#*,}"
    mean_mk="${mk_stats%,*}"
    std_mk="${mk_stats#*,}"
    mean_e2e_m="${e2e_mean_stats%,*}"
    std_e2e_m="${e2e_mean_stats#*,}"

    dur_p="$(calc_percentiles_file "$dur_samples")"
    e2e_p="$(calc_percentiles_file "$e2e_samples")"
    dur_p50="$(echo "$dur_p" | cut -d',' -f1)"
    dur_p95="$(echo "$dur_p" | cut -d',' -f2)"
    dur_p99="$(echo "$dur_p" | cut -d',' -f3)"
    dur_n="$(echo "$dur_p" | cut -d',' -f4)"
    e2e_p50="$(echo "$e2e_p" | cut -d',' -f1)"
    e2e_p95="$(echo "$e2e_p" | cut -d',' -f2)"
    e2e_p99="$(echo "$e2e_p" | cut -d',' -f3)"
    e2e_n="$(echo "$e2e_p" | cut -d',' -f4)"

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "$phase" "$workload" "$load_model" "$agents" "$jobs" "$failure_rate" "$repetitions" \
      "$mean_sr" "$std_sr" "$mean_mk" "$std_mk" \
      "$dur_p50" "$dur_p95" "$dur_p99" "$dur_n" \
      "$mean_e2e_m" "$std_e2e_m" "$e2e_p50" "$e2e_p95" "$e2e_p99" "$e2e_n" >> "$SUMMARY_AGG_CSV"
  done
fi

# Readiness check
failed=0
echo "--- readiness_check ---"

if [[ -z "${MC_DB_DSN:-}" ]]; then
  echo "FAIL DB_DSN_MISSING: MC_DB_DSN is empty"
  failed=1
else
  active_counts="$(psql "$MC_DB_DSN" -At -v ON_ERROR_STOP=1 -c "
    SELECT
      COALESCE(SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END),0),
      COALESCE(SUM(CASE WHEN status='assigned' THEN 1 ELSE 0 END),0),
      COALESCE(SUM(CASE WHEN status='running' THEN 1 ELSE 0 END),0)
    FROM demand_jobs;
  " 2>/dev/null || true)"
  if [[ -z "$active_counts" ]]; then
    echo "FAIL DB_CONNECT: unable to query demand_jobs"
    failed=1
  else
    q="$(echo "$active_counts" | cut -d'|' -f1)"
    a="$(echo "$active_counts" | cut -d'|' -f2)"
    r="$(echo "$active_counts" | cut -d'|' -f3)"
    if [[ "$q" -eq 0 && "$a" -eq 0 && "$r" -eq 0 ]]; then
      echo "PASS DB_IDLE: queued=0 assigned=0 running=0"
    else
      echo "FAIL DB_IDLE: queued=${q} assigned=${a} running=${r}"
      failed=1
    fi
  fi
fi

control_count="unknown"
agent_count="unknown"
if command -v pgrep >/dev/null 2>&1; then
  control_count="$(pgrep -f 'go run ./cmd/control|/cmd/control' | wc -l | tr -d ' ' || true)"
  agent_count="$(pgrep -f '/agent -ns|go run ./cmd/agent' | wc -l | tr -d ' ' || true)"
elif ps -ax >/dev/null 2>&1; then
  control_count="$(ps -ax | grep -E '[g]o run ./cmd/control|[/]cmd/control' | wc -l | tr -d ' ')"
  agent_count="$(ps -ax | grep -E '[g]o run ./cmd/agent|[/]agent -ns' | wc -l | tr -d ' ')"
fi
echo "PROCESS_COUNTS control=${control_count} agent=${agent_count}"
if [[ "$control_count" =~ ^[0-9]+$ ]]; then
  # go run may show wrapper + child process, so accept >=1.
  if [[ "$control_count" -lt 1 ]]; then
    echo "FAIL CONTROL_COUNT: expected >=1, got ${control_count}"
    failed=1
  fi
fi
if [[ "$agent_count" =~ ^[0-9]+$ ]]; then
  if [[ "$agent_count" -ne 0 ]]; then
    echo "FAIL AGENT_COUNT: expected 0 before rerun, got ${agent_count}"
    failed=1
  fi
fi

incomplete_runs="$(for d in "${RUNS_ROOT}"/exp1-*; do [[ -d "$d" ]] || continue; [[ -f "$d/summary.csv" ]] || basename "$d"; done | wc -l | tr -d ' ')"
if [[ "$incomplete_runs" -eq 0 ]]; then
  echo "PASS NO_INCOMPLETE_RUN_DIRS"
else
  echo "FAIL INCOMPLETE_RUN_DIRS count=${incomplete_runs}"
  failed=1
fi

echo "ENV RUN_TIMEOUT_SEC=${RUN_TIMEOUT_SEC:-unset} QUEUED_TIMEOUT_SEC=${QUEUED_TIMEOUT_SEC:-unset}"
echo "ENV TTL_SEC=${TTL_SEC:-unset} HEARTBEAT_SEC=${HEARTBEAT_SEC:-unset}"

anomaly_count="$(awk 'NR>1{n++} END{print n+0}' "$ANOMALY_CSV")"
echo "ANOMALY_COUNT=${anomaly_count}"
echo "ANOMALY_FILE=${ANOMALY_CSV}"
echo "FILTERED_RESULTS_FILE=${FILTERED_RESULTS_CSV}"
echo "DIAGNOSIS_FILE=${DIAG_CSV}"
echo "SUMMARY_AGG_REBUILT=${SUMMARY_AGG_CSV}"

if [[ "$failed" -eq 0 ]]; then
  echo "READY_FOR_RERUN"
else
  echo "NOT_READY_FOR_RERUN"
fi
