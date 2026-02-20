#!/usr/bin/env bash
# Shared configuration for exp1_success-rate harness.
# Any variable can be overridden via environment before script invocation.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Control-plane and DB connectivity.
CONTROL_URL="${CONTROL_URL:-http://127.0.0.1:8080}"
MC_DB_DSN="${MC_DB_DSN:-postgres://mc:mcpass@127.0.0.1:5432/mcdb?sslmode=disable}"
CONTROL_TOKEN="${CONTROL_TOKEN:-}"

# Experiment timing and polling behavior.
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-1800}"
POLL_SEC="${POLL_SEC:-5}"

# Submission payload defaults.
IMAGE="${IMAGE:-alpine:3.20}"
MANIFEST_ROOT_CID="${MANIFEST_ROOT_CID:-noop}"

# Experiment dimensions / metadata.
AGENTS="${AGENTS:-1}"
FAILURE_RATE="${FAILURE_RATE:-0}"
REP="${REP:-1}"
TTL_SEC="${TTL_SEC:-20}"
HEARTBEAT_SEC="${HEARTBEAT_SEC:-2}"
AGENT_PIDS_FILE="${AGENT_PIDS_FILE:-}"
FAILURE_INJECT_DELAY_SEC="${FAILURE_INJECT_DELAY_SEC:-5}"

# Enforce lease safety margin.
if [ "${TTL_SEC}" -lt "$((5 * HEARTBEAT_SEC))" ]; then
  TTL_SEC="$((5 * HEARTBEAT_SEC))"
fi

# Artifact paths.
RUNS_ROOT="${RUNS_ROOT:-${SCRIPT_DIR}/runs}"
RESULTS_CSV="${RESULTS_CSV:-${SCRIPT_DIR}/results.csv}"
SUMMARY_CSV="${SUMMARY_CSV:-${SCRIPT_DIR}/summary.csv}"
SUBMIT_FAILURE_LOG="${SUBMIT_FAILURE_LOG:-${RUNS_ROOT}/submit_failures.log}"
