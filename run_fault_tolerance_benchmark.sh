#!/usr/bin/env bash
set -euo pipefail


# -------- (환경 변수로) 조정 가능한 값들 --------
NS="${NS:-mc}"
START_PORT="${CONTROL_PORT:-8080}"
CONTROL_TOKEN="${CONTROL_TOKEN:-dev}"
DISABLE_AUTH="${MC_DISABLE_AUTH:-0}"

AGENTS="${AGENTS:-30}"
TASKS="${TASKS:-200}"
HB_SEC="${HB_SEC:-5}"          
TTL_SEC="${TTL_SEC:-15}"       
KILL_PERCENT="${KILL_PERCENT:-10}"
RUNTIME_BEFORE_KILL="${RUNTIME_BEFORE_KILL:-60}"    
POST_KILL_OBSERVE="${POST_KILL_OBSERVE:-60}"        

# 컨테이너 정책
DOCKER_IMAGE="${DOCKER_IMAGE:-alpine:latest}"
AGENT_FLAGS="${AGENT_FLAGS:-}"

# Manifest providers JSON (선택 사항). 비어 있으면 입력 없이 아무 동작 안 함.
# 예시:
#   export PROVIDERS_JSON='{"root_cid":"demo.txt","providers":[{"peer_id":"12D3...","addrs":["/ip4/127.0.0.1/tcp/34567/p2p/12D3..."]}]}'
PROVIDERS_JSON="${PROVIDERS_JSON:-}"

# -------- 결과물 경로 --------
TS="$(date +%Y%m%d-%H%M%S)"
OUT="bench_artifacts/${TS}"
LOGDIR="${OUT}/logs"
mkdir -p "${LOGDIR}" "${OUT}/results"

# -------- 도커 사전 확인 --------
if ! command -v docker >/dev/null 2>&1; then
  echo "[FATAL] docker not found in PATH"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "[FATAL] docker daemon not running or insufficient permission"; exit 1
fi

echo "==> Docker image pre-pull: ${DOCKER_IMAGE}"
docker pull "${DOCKER_IMAGE}" >/dev/null || { echo "[FATAL] failed to pull ${DOCKER_IMAGE}"; exit 1; }

# -------- 헬퍼 함수 --------
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

# -------- 컨트롤러 시작 --------
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
    ./control -ns "${NS}" -http-port "${PORT}" \
    > "${LOGDIR}/control.log" 2>&1 ) &
CONTROL_PID=$!

# 컨트롤러 HTTP 준비될 때까지 대기
for i in {1..50}; do
  if http_ready "${CONTROL_URL}"; then break; fi
  sleep 0.1
done

#부트스트랩 멀티주소 추출 (루프백/tcp 우선)
BOOTSTRAP=""
# 127.0.0.1 주소를 먼저 시도
BOOTSTRAP="$(awk '/addr: .*\/ip4\/127\.0\.0\.1\/tcp\/[^ ]*\/p2p\//{print $NF; exit}' "${LOGDIR}/control.log" 2>/dev/null || true)"
# 찾지 못하면 사용 가능한 첫 번째 P2P 주소로 대체
if [ -z "${BOOTSTRAP}" ]; then
  BOOTSTRAP="$(grep '/p2p/' "${LOGDIR}/control.log" | sed -n 's/.*addr: \([^ ]*\).*/\1/p' | head -n1)"
fi
# 부트스트랩 주소를 찾을 수 없으면 종료
if [ -z "${BOOTSTRAP}" ]; then
  echo "[FATAL] Failed to extract bootstrap multiaddr from ${LOGDIR}/control.log" >&2
  kill "${CONTROL_PID}" 2>/dev/null
  exit 1
fi
echo "==> Using bootstrap: ${BOOTSTRAP}"
echo "BOOTSTRAP=${BOOTSTRAP}" >> "${OUT}/config.txt"

# -------- 에이전트 생성 --------
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
        ./agent \
          -ns "${NS}" \
          -control-url "${CONTROL_URL}" \
          -auth-token "${CONTROL_TOKEN}" \
          -bootstrap "${BOOTSTRAP}" \
          ${AGENT_FLAGS} \
          >> "${LOG}" 2>&1
  ) &
  echo "$!" >> "${OUT}/agents.pids"
  sleep 0.02
done
echo "agents started: $(wc -l < "${OUT}/agents.pids")"

# -------- 작업 제출 --------
echo "==> Submitting ${TASKS} tasks"
AUTH_HEADER=()
[[ "${DISABLE_AUTH}" != "1" ]] && AUTH_HEADER=(-H "Authorization: Bearer ${CONTROL_TOKEN}")

TASKS_FILE="${OUT}/tasks.jsonl"
: > "${TASKS_FILE}"

for j in $(seq 1 "${TASKS}"); do
  JOB="job-${TS}-${j}"
body="{\"id\":\"${JOB}\",\"image\":\"${DOCKER_IMAGE}\",\"command\":[\"/bin/sh\",\"-lc\",\"echo agent:$RANDOM && sleep 10 && echo done\"]}"
  http_code="$(curl -sS -o /tmp/resp.$$ -w '%{http_code}' -X POST "${CONTROL_URL}/api/tasks" \
                 -H 'Content-Type: application/json' "${AUTH_HEADER[@]}" -d "${body}" || true)"
  cat /tmp/resp.$$ >> "${TASKS_FILE}"; echo >> "${TASKS_FILE}"

  # Manifest 전송 (noop 또는 사용자가 제공한 providers)
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

# -------- 장애 유발 --------
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

# -------- 남은 에이전트 중지 --------
echo "==> Stopping remaining agents"
while read -r pid; do
  if kill -0 "${pid}" 2>/dev/null; then kill "${pid}" 2>/dev/null || true; fi
done < "${OUT}/agents.pids"
sleep 1

# -------- 분석 (가능한 경우) --------
if [[ -x "$(dirname "$0")/analyze_metrics.py" ]]; then
  echo "==> Running metrics analyzer"
  python3 "$(dirname "$0")/analyze_metrics.py" "${OUT}" | tee "${OUT}/results/report.md"
else
  echo "==> Analyzer not found. Skipping metrics summarize."
  echo "(You can run: CONTROL_TOKEN=${CONTROL_TOKEN} python3 analyze_metrics.py ${OUT})"
fi

echo "==> DONE. See ${OUT}"
