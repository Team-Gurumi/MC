#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

ACTION="${1:-start}"
CONFIG_JSON_PATH="${CONFIG_JSON_PATH:-}"

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NS="${NS:-mc}"
BOOTSTRAP="${BOOTSTRAP:-}"
AGENT_FLAGS="${AGENT_FLAGS:-}"
AGENT_STABILIZE_SEC="${AGENT_STABILIZE_SEC:-6}"
AUTO_BOOTSTRAP_FROM_CONTROL="${AUTO_BOOTSTRAP_FROM_CONTROL:-1}"

if [[ -z "${AGENT_PIDS_FILE:-}" ]]; then
  AGENT_PIDS_FILE="${RUNS_ROOT}/agents.pids"
fi
LOG_DIR="${RUNS_ROOT}/agents"
BUILD_AGENT="${BUILD_AGENT:-0}"

json_get_value() {
  local key="$1"
  local file="$2"
  sed -n -E "s/^[[:space:]]*\"${key}\"[[:space:]]*:[[:space:]]*([^,]+).*/\\1/p" "$file" | head -n1 | tr -d ' "'
}

load_ttl_hb_from_json() {
  local file="$1"
  [[ -f "$file" ]] || return 0

  local ttl hb
  ttl="$(json_get_value "ttl_sec" "$file" || true)"
  hb="$(json_get_value "heartbeat_sec" "$file" || true)"
  if [[ "${ttl:-}" =~ ^[0-9]+$ ]]; then
    TTL_SEC="$ttl"
  fi
  if [[ "${hb:-}" =~ ^[0-9]+$ ]]; then
    HEARTBEAT_SEC="$hb"
  fi
}

kill_from_pid_file() {
  local pid_file="$1"
  [[ -f "$pid_file" ]] || return 0
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done < "$pid_file"
}

cleanup_on_exit() {
  kill_from_pid_file "$AGENT_PIDS_FILE"
}

ensure_agent_binary() {
  if [[ -x "${REPO_ROOT}/agent" ]]; then
    return 0
  fi
  if [[ "$BUILD_AGENT" != "1" ]]; then
    echo "agent binary not found or not executable: ${REPO_ROOT}/agent (set BUILD_AGENT=1 to auto-build)" >&2
    exit 1
  fi
  (
    cd "$REPO_ROOT"
    go build -o agent ./cmd/agent
  )
}

maybe_auto_bootstrap() {
  if [[ "$AUTO_BOOTSTRAP_FROM_CONTROL" != "1" ]]; then
    return 0
  fi
  local url resp selected
  url="${CONTROL_URL%/}/debug/bootstrap"
  if [[ -n "${CONTROL_TOKEN:-}" ]]; then
    resp="$(curl -sS -H "Authorization: Bearer ${CONTROL_TOKEN}" "$url" || true)"
  else
    resp="$(curl -sS "$url" || true)"
  fi
  selected="$(printf '%s' "$resp" | sed -nE 's/.*"bootstrap_selected"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' | head -n1)"
  if [[ -n "$selected" ]]; then
    BOOTSTRAP="$selected"
  fi
}

start_agents() {
  if [[ -n "${CONFIG_JSON_PATH}" ]]; then
    load_ttl_hb_from_json "$CONFIG_JSON_PATH"
  fi

  if ! [[ "$AGENTS" =~ ^[0-9]+$ ]] || (( AGENTS <= 0 )); then
    echo "AGENTS must be a positive integer" >&2
    exit 1
  fi
  if ! [[ "$TTL_SEC" =~ ^[0-9]+$ ]] || (( TTL_SEC <= 0 )); then
    echo "TTL_SEC must be a positive integer" >&2
    exit 1
  fi
  if ! [[ "$HEARTBEAT_SEC" =~ ^[0-9]+$ ]] || (( HEARTBEAT_SEC <= 0 )); then
    echo "HEARTBEAT_SEC must be a positive integer" >&2
    exit 1
  fi
  if ! [[ "$AGENT_STABILIZE_SEC" =~ ^[0-9]+$ ]] || (( AGENT_STABILIZE_SEC < 0 )); then
    echo "AGENT_STABILIZE_SEC must be a non-negative integer" >&2
    exit 1
  fi
  ensure_agent_binary
  maybe_auto_bootstrap

  mkdir -p "$(dirname "$AGENT_PIDS_FILE")" "$LOG_DIR"
  : > "$AGENT_PIDS_FILE"

  trap cleanup_on_exit INT TERM

  for i in $(seq 1 "$AGENTS"); do
    (
      cd "$REPO_ROOT"
      ./agent -ns "$NS" -control-url "$CONTROL_URL" -auth-token "$CONTROL_TOKEN" -bootstrap "$BOOTSTRAP" \
        -ttl-sec "$TTL_SEC" -heartbeat-sec "$HEARTBEAT_SEC" ${AGENT_FLAGS} \
        >> "${LOG_DIR}/agent_${i}.log" 2>&1
    ) &
    # harness stability patch: guard against transient parent-dir disappearance between retries.
    mkdir -p "$(dirname "$AGENT_PIDS_FILE")" "$LOG_DIR"
    echo "$!" >> "$AGENT_PIDS_FILE"
  done

  local live_count=0
  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if kill -0 "$pid" 2>/dev/null; then
      live_count=$((live_count + 1))
    fi
  done < "$AGENT_PIDS_FILE"
  if (( live_count != AGENTS )); then
    echo "failed to launch all agents: expected=${AGENTS} live=${live_count}" >&2
    cleanup_on_exit
    exit 1
  fi

  # Re-check after a short stabilization window to catch immediate bootstrap/DHT exits.
  if (( AGENT_STABILIZE_SEC > 0 )); then
    # harness stability patch: guarantee at least 2s settle time before submissions begin.
    local stabilize_sec="$AGENT_STABILIZE_SEC"
    if (( stabilize_sec < 2 )); then
      stabilize_sec=2
    fi
    sleep "$stabilize_sec"
    live_count=0
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      if kill -0 "$pid" 2>/dev/null; then
        live_count=$((live_count + 1))
      fi
    done < "$AGENT_PIDS_FILE"
    if (( live_count != AGENTS )); then
      echo "agents not stable after ${stabilize_sec}s: expected=${AGENTS} live=${live_count}" >&2
      cleanup_on_exit
      exit 1
    fi
  fi

  local git_hash="unknown"
  if command -v git >/dev/null 2>&1; then
    git_hash="$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  fi
  echo "agents_started=${AGENTS} agent_bin=${REPO_ROOT}/agent git=${git_hash} ttl_sec=${TTL_SEC} heartbeat_sec=${HEARTBEAT_SEC} pids_file=${AGENT_PIDS_FILE}"
}

stop_agents() {
  kill_from_pid_file "$AGENT_PIDS_FILE"
  sleep 1

  local alive=0
  if [[ -f "$AGENT_PIDS_FILE" ]]; then
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      [[ "$pid" =~ ^[0-9]+$ ]] || continue
      if kill -0 "$pid" 2>/dev/null; then
        alive=$((alive + 1))
      fi
    done < "$AGENT_PIDS_FILE"
    rm -f "$AGENT_PIDS_FILE"
  fi
  if (( alive > 0 )); then
    echo "warning: ${alive} agent processes still alive after stop" >&2
    return 1
  fi
}

case "$ACTION" in
  start)
    start_agents
    ;;
  stop)
    stop_agents
    ;;
  *)
    echo "usage: $0 [start|stop]" >&2
    exit 1
    ;;
esac
