#!/usr/bin/env bash
set -euo pipefail

# Experiment: Duplicate Execution Rate
# This script runs the fault tolerance benchmark and then analyzes the logs
# for duplicate executions based on lease overlap.
#
# Run from the project root:
#   cd /path/to/MC && bash experiments/run_duplicate_expt.sh

# --- Experiment Config ---
export AGENTS="${AGENTS:-100}"
export TASKS="${TASKS:-200}"
export HB_SEC="${HB_SEC:-3}"
export TTL_SEC="${TTL_SEC:-10}"
export KILL_PERCENT="${KILL_PERCENT:-10}"
export RUNTIME_BEFORE_KILL="${RUNTIME_BEFORE_KILL:-10}"
export POST_KILL_OBSERVE="${POST_KILL_OBSERVE:-60}"
export MC_DISABLE_AUTH="1"

# Resolve the directory where this script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "####### Starting Duplicate Execution Rate Experiment #######"
echo "AGENTS=${AGENTS}, TASKS=${TASKS}"

# Run the benchmark from the experiments directory (where binaries are expected)
cd "${SCRIPT_DIR}/.."
bash "${SCRIPT_DIR}/run_fault_tolerance_benchmark.sh"

# Find the latest artifact directory
LATEST_ART="$(ls -td bench_artifacts/*/ 2>/dev/null | head -1)"
if [ -z "${LATEST_ART}" ]; then
    echo "[ERROR] Could not find artifact directory in bench_artifacts/"
    exit 1
fi

echo "===> Analyzing logs in ${LATEST_ART}..."

# Run the duplicate overlap analysis
python3 "${SCRIPT_DIR}/analyze_duplicate_overlap.py" "${LATEST_ART}"

echo ""
echo "===> Done. Report:"
cat "${LATEST_ART}/results/duplicate_report.json" 2>/dev/null || cat "${LATEST_ART}results/duplicate_report.json" 2>/dev/null || echo "[WARN] report file not found"
