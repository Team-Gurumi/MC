#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAX_CYCLES="${MAX_CYCLES:-30}"
SLEEP_BETWEEN_CYCLES_SEC="${SLEEP_BETWEEN_CYCLES_SEC:-5}"

need_env=(CONTROL_URL CONTROL_TOKEN MC_DB_DSN TTL_SEC HEARTBEAT_SEC RUN_TIMEOUT_SEC QUEUED_TIMEOUT_SEC MATRIX_PROFILE STRICT_FINAL REPETITIONS)
for k in "${need_env[@]}"; do
  if [[ -z "${!k:-}" ]]; then
    echo "missing env: ${k}" >&2
    exit 1
  fi
done

cd "${SCRIPT_DIR}/.."

for ((cycle=1; cycle<=MAX_CYCLES; cycle++)); do
  echo "[auto-resume] cycle=${cycle}/${MAX_CYCLES}"
  out="$(bash exp1_success-rate/reconcile_results.sh)"
  echo "$out" | tail -n 4
  pending="$(echo "$out" | sed -nE 's/.*pending_runs=([0-9]+).*/\1/p' | tail -n1)"
  pending="${pending:-0}"
  if [[ "$pending" == "0" ]]; then
    echo "[auto-resume] DONE pending_runs=0"
    exit 0
  fi

  bash exp1_success-rate/resume_remaining.sh || true

  if [[ -f exp1_success-rate/runs/matrix_runner.pid ]]; then
    pid="$(cat exp1_success-rate/runs/matrix_runner.pid 2>/dev/null || true)"
    if [[ -n "$pid" ]]; then
      while kill -0 "$pid" 2>/dev/null; do
        sleep 10
      done
    fi
  else
    # fallback: short wait if pid file is not created
    sleep 20
  fi

  echo "[auto-resume] runner exited; checking pending again"
  tail -n 20 exp1_success-rate/runs/failure_analysis.log 2>/dev/null || true
  sleep "$SLEEP_BETWEEN_CYCLES_SEC"
done

echo "[auto-resume] reached MAX_CYCLES with pending runs remaining" >&2
exit 1
