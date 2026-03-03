#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

CONTROL_HTTP_PORT="${CONTROL_HTTP_PORT:-8080}"
FAILURE_ANALYSIS_LOG="${FAILURE_ANALYSIS_LOG:-${RUNS_ROOT}/failure_analysis.log}"

mkdir -p "${RUNS_ROOT}"

# harness stability patch: single resume entry guard (prevents concurrent pkill storms).
RESUME_LOCK_DIR="${RUNS_ROOT}/.resume.lock"
if ! mkdir "$RESUME_LOCK_DIR" 2>/dev/null; then
  echo "[resume] another resume invocation is already running; aborting duplicate call" >&2
  exit 2
fi
cleanup_resume_lock() {
  rm -rf "$RESUME_LOCK_DIR"
}
trap cleanup_resume_lock EXIT INT TERM

echo "[resume] stopping stale orchestrator/agent/control processes"
bash "${SCRIPT_DIR}/run_agents.sh" stop >/dev/null 2>&1 || true
pkill -f "matrix_runner.sh|run_matrix.sh|run_agents.sh|/agent -ns|go run ./cmd/agent|cmd/control|/control -http-port" >/dev/null 2>&1 || true
rm -rf "${RUNS_ROOT}/.matrix_runner.lock"
rm -f "${RUNS_ROOT}/matrix_runner.pid" "${RUNS_ROOT}/control.pid"

echo "[resume] waiting for postgres"
for i in 1 2 3 4 5; do
  if psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "SELECT 1;" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -At -c "SELECT 1;" >/dev/null 2>&1; then
  echo "[resume] postgres not reachable" >&2
  exit 1
fi

echo "[resume] clearing non-terminal exp1 jobs"
psql "$MC_DB_DSN" -v ON_ERROR_STOP=1 -c "
DELETE FROM demand_jobs
WHERE id LIKE 'exp1-%'
  AND status IN ('queued','assigned','running');
" >/dev/null

# Start control with phase policy used by matrix bootstrap (baseline first).
if lsof -nP -iTCP:"${CONTROL_HTTP_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "[resume] port ${CONTROL_HTTP_PORT} already in use; aborting to avoid split-brain control" >&2
  exit 1
fi

echo "[resume] launching matrix runner"
: > "${RUNS_ROOT}/matrix_live.log"
nohup bash "${SCRIPT_DIR}/matrix_runner.sh" > "${RUNS_ROOT}/matrix_live.log" 2>&1 &
pid="$!"
echo "[resume] matrix_runner pid=${pid}"

echo "[resume] tail: ${RUNS_ROOT}/matrix_live.log"
tail -n 20 "${RUNS_ROOT}/matrix_live.log" || true

echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] resume_invoked pid=${pid}" >> "$FAILURE_ANALYSIS_LOG"
