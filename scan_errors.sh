#!/usr/bin/env bash
set -euo pipefail
ART="${1:-}"
if [[ -z "$ART" ]]; then echo "usage: $0 bench_artifacts/<timestamp>"; exit 2; fi
LOG="$ART/logs"
grep -nE 'error|failed|panic|permission denied|No such file|cannot|refused|unauthorized|forbidden' \
  "$LOG"/control.log "$LOG"/agent-*.log || echo "(no obvious errors)"
