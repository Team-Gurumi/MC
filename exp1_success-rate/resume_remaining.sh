#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "${SCRIPT_DIR}/reconcile_results.sh"

pending_count="$(awk 'NR>1{n++} END{print n+0}' "${SCRIPT_DIR}/pending_final105.csv")"
echo "pending_runs=${pending_count}"
if [[ "$pending_count" == "0" ]]; then
  echo "nothing to run"
  exit 0
fi

echo "pending list:"
cat "${SCRIPT_DIR}/pending_final105.csv"

echo "--- resume matrix runner ---"
bash "${SCRIPT_DIR}/resume.sh"
