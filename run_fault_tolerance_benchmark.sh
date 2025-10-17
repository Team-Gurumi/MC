#!/usr/bin/env bash
set -euo pipefail

# Fault-tolerance benchmark (patched)
# - Adds manifest POST per task (noop by default or PROVIDERS_JSON override)
# - Wires DHT bootstrap from control.log to agents
# - Shorter default timings for fast iteration

# -------- Tunables (env) --------
NS="${NS:-mc}"
START_PORT="${CONTROL_PORT:-8080}"
CONTROL_TOKEN="${CONTROL_TOKEN:-dev}"
DISABLE_AUTH="${MC_DISABLE_AUTH:-0}"

AGENTS="${AGENTS:-30}"
TASKS="${TASKS:-200}"
HB_SEC="${HB_SEC:-5}"          # agent reads if supported
TTL_SEC="${TTL_SEC:-15}"       # agent reads if supported
KILL_PERCENT="${KILL_PERCENT:-10}"
RUNTIME_BEFORE_KILL="${RUNTIME_BEFORE_KILL:-60}"     # ↓ shorter
POST_KILL_OBSERVE="${POST_KILL_OBSERVE:-60}"         # ↓ shorter

# Container policy
DOCKER_IMAGE="${DOCKER_IMAGE:-alpine:latest}"
AGENT_FLAGS="${AGENT_FLAGS:-}"

# Manifest providers JSON (optional). When empty, use noop (no input needed).
# Example:
#   export PROVIDERS_JSON='{"root_cid":"demo.txt","providers":[{"peer_id":"12D3...","addrs":["/ip4/127.0.0.1/tcp/34567/p2p/12D3..."]}]}'
PROVIDERS_JSON="${PROVIDERS_JSON:-}"

# -------- Artifacts --------
TS="$(date +%Y%m%d-%H%M%S)"
OUT="bench_artifacts/${TS}"
LOGDIR="${OUT}/logs"
mkdir -p "${LOGDIR}" "${OUT}/results"

# -------- Docker preflight --------
if ! command -v docker >/dev/null 2>&1; then
  echo "[FATAL] docker not found in PATH"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "[FATAL] docker daemon not running or insufficient permission"; exit 1
fi

echo "==> Docker image pre-pull: ${DOCKER_IMAGE}"
docker pull "${DOCKER_IMAGE}" >/dev/null || { echo "[FATAL] failed to pull ${DOCKER_IMAGE}"; exit 1; }

# -------- Helpers --------
pick_free_port() {
  local p=$1
  while :; do
    if ! (ss -ltn "( sport = :$p )" 2>/dev/null | tail -n +2 | grep -q .) && \
       ! (lsof -i TCP:"$p" -sTCP:LISTEN -P -n 2>/dev/null | grep -q .); then
      echo "$p"; return 0
    fi
    p=$((p+1)); [ "$p" -gt 65535 ] && { echo "no free port"; return 1; }
  done
}

http_ready() {
  local base="$1"
  curl -fsS "${base}/" >/dev/null 2>&1 || curl -fsS "${base}/api/health" >/dev/null 2>&1
}

select_bootstrap() {
  # Prefer 127.0.0.1 address, else first addr line
  local log="${LOGDIR}/control.log"
  local pick=""
  if grep -qE "addr: /ip4/127\.0\.0\.1/.*/p2p/" "$log"; then
    pick="$(grep -m1 -E 'addr: /ip4/127\.0\.0\.1/.*/p2p/' "$log" | sed -E 's/.*addr: ([^ ]+).*/\1/')"
  else
    pick="$(grep -m1 -E 'addr: .*?/p2p/' "$log" | sed -E 's/.*addr: ([^ ]+).*/\1/')"
  fi
  echo "$pick"
}

# -------- Control start --------
PORT="$(pick_free_port "$START_PORT")"
CONTROL_URL="http://127.0.0.1:${PORT}"

cat > "${OUT}/config.txt" <<EOF
### BENCH CONFIG
NS=${NS}
CONTROL_URL=${CONTROL_URL}
AGENTS=${AGENTS}
TASKS=${TASKS}
HB_SEC=${HB_SEC}
TTL_SEC=${TTL_SEC}
KILL_PERCENT=${KILL_PERCENT}
RUNTIME_BEFORE_KILL=${RUNTIME_BEFORE_KILL}s
POST_KILL_OBSERVE=${POST_KILL_OBSERVE}s
DOCKER_IMAGE=${DOCKER_IMAGE}
Artifacts: ${OUT}
EOF

cat "${OUT}/config.txt"

CONTROL_ENV=()
CONTROL_ENV+=(MC_NS="${NS}")
if [[ "${DISABLE_AUTH}" == "1" ]]; then
  CONTROL_ENV+=(MC_DISABLE_AUTH=1)
else
  CONTROL_ENV+=(CONTROL_TOKEN="${CONTROL_TOKEN}")
fi

echo "==> Starting control @ ${CONTROL_URL}"
( exec env -i PATH="$PATH" HOME="$HOME" "${CONTROL_ENV[@]}" \
    go run ./cmd/control -ns "${NS}" -http-port "${PORT}" \
    > "${LOGDIR}/control.log" 2>&1 ) &
CONTROL_PID=$!

# Wait for control HTTP ready
for i in {1..50}; do
  if http_ready "${CONTROL_URL}"; then break; fi
  sleep 0.1
done

# Extract bootstrap from control log
sleep 0.5
BOOTSTRAP="$(select_bootstrap || true)"
if [[ -z "${BOOTSTRAP}" ]]; then
  echo "[WARN] could not detect bootstrap from control.log; agents will start without it"
else
  echo "BOOTSTRAP=${BOOTSTRAP}" >> "${OUT}/config.txt"
  echo "==> Using bootstrap: ${BOOTSTRAP}"
fi

# -------- Agents spawn --------
echo "==> Spawning ${AGENTS} agents"
: > "${OUT}/agents.pids"
for i in $(seq 1 "${AGENTS}"); do
  LOG="${LOGDIR}/agent-${i}.log"

  if [[ -z "${DOCKER_HOST:-}" ]]; then
    if [[ -S "/run/user/$(id -u)/docker.sock" ]]; then
      export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
      export DOCKER_HOST="unix:///run/user/$(id -u)/docker.sock"
    else
      export DOCKER_HOST="unix:///var/run/docker.sock"
    fi
  fi

  EXTRA_ENV=()
  [[ -n "${BOOTSTRAP}" ]] && EXTRA_ENV+=("BOOTSTRAP=${BOOTSTRAP}")

  (
env PATH="${PATH}" HOME="${HOME}" \
        HB_SEC="${HB_SEC}" TTL_SEC="${TTL_SEC}" \
        DOCKER_HOST="${DOCKER_HOST}" \
        MC_NS="${NS}" \
        "${EXTRA_ENV[@]}" \
        go run ./cmd/agent \
          -ns "${NS}" \
          -control-url "${CONTROL_URL}" \
          -auth-token "${CONTROL_TOKEN}" \
          ${BOOTSTRAP:+-bootstrap "${BOOTSTRAP}"} \
          ${AGENT_FLAGS} \
          >> "${LOG}" 2>&1
  ) &
  echo "$!" >> "${OUT}/agents.pids"
  sleep 0.02
done
echo "agents started: $(wc -l < "${OUT}/agents.pids")"

# -------- Submit tasks --------
echo "==> Submitting ${TASKS} tasks"
AUTH_HEADER=()
[[ "${DISABLE_AUTH}" != "1" ]] && AUTH_HEADER=(-H "Authorization: Bearer ${CONTROL_TOKEN}")

TASKS_FILE="${OUT}/tasks.jsonl"
: > "${TASKS_FILE}"

for j in $(seq 1 "${TASKS}"); do
  JOB="job-${TS}-${j}"
body="{\"id\":\"${JOB}\",\"image\":\"${DOCKER_IMAGE}\",\"command\":[\"/bin/sh\",\"-lc\",\"echo agent:$RANDOM && sleep 60 && echo done\"]}"
  http_code="$(curl -sS -o /tmp/resp.$$ -w '%{http_code}' -X POST "${CONTROL_URL}/api/tasks" \
                 -H 'Content-Type: application/json' "${AUTH_HEADER[@]}" -d "${body}" || true)"
  cat /tmp/resp.$$ >> "${TASKS_FILE}"; echo >> "${TASKS_FILE}"

  # Manifest post (noop or user-provided providers)
  if [[ -n "${PROVIDERS_JSON}" ]]; then
    mbody="${PROVIDERS_JSON}"
  else
    mbody='{"root_cid":"noop","providers":[]}'
  fi
  curl -sS -X POST "${CONTROL_URL}/jobs/${JOB}/manifest" \
    -H 'Content-Type: application/json' "${AUTH_HEADER[@]}" -d "${mbody}" >/dev/null || echo "[WARN] manifest failed for ${JOB}"

  if [[ "${http_code}" != "200" && "${http_code}" != "201" ]]; then
    echo "[WARN] submit ${JOB} -> HTTP ${http_code}"
  fi
  rm -f /tmp/resp.$$
  if (( j % 100 == 0 )); then echo "  submitted: ${j}"; fi
done

# -------- Induce failure --------
echo "==> Sleeping ${RUNTIME_BEFORE_KILL}s"
sleep "${RUNTIME_BEFORE_KILL}"

TOTAL_AGENTS="$(wc -l < "${OUT}/agents.pids")"
KILL_COUNT=$(( (TOTAL_AGENTS * KILL_PERCENT + 99) / 100 ))
echo "==> SIGKILL ${KILL_COUNT}/${TOTAL_AGENTS} agents"

shuf -n "${KILL_COUNT}" "${OUT}/agents.pids" > "${OUT}/killed_agents.txt" || cp "${OUT}/agents.pids" "${OUT}/killed_agents.txt"
while read -r pid; do
  kill -9 "${pid}" 2>/dev/null || true
done < "${OUT}/killed_agents.txt"

echo "==> Observing ${POST_KILL_OBSERVE}s"
sleep "${POST_KILL_OBSERVE}"

# -------- Stop remaining agents --------
echo "==> Stopping remaining agents"
while read -r pid; do
  if kill -0 "${pid}" 2>/dev/null; then kill "${pid}" 2>/dev/null || true; fi
done < "${OUT}/agents.pids"
sleep 1

# -------- Analyze (if available) --------
if [[ -x "$(dirname "$0")/analyze_metrics.py" ]]; then
  echo "==> Running metrics analyzer"
  python3 "$(dirname "$0")/analyze_metrics.py" "${OUT}" | tee "${OUT}/results/report.md"
else
  echo "==> Analyzer not found. Skipping metrics summarize."
  echo "(You can run: CONTROL_TOKEN=${CONTROL_TOKEN} python3 analyze_metrics.py ${OUT})"
fi

echo "==> DONE. See ${OUT}"

