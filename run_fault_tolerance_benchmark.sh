#!/usr/bin/env bash
set -euo pipefail

########################################
# 0) 기본 설정
########################################
NS="${NS:-mc}"
CONTROL_PORT="${CONTROL_PORT:-8080}"
CONTROL_URL="http://127.0.0.1:${CONTROL_PORT}"

AGENTS="${AGENTS:-30}"
TASKS="${TASKS:-200}"
HB_SEC="${HB_SEC:-5}"
TTL_SEC="${TTL_SEC:-15}"
KILL_PERCENT="${KILL_PERCENT:-10}"
RUNTIME_BEFORE_KILL="${RUNTIME_BEFORE_KILL:-60}"
POST_KILL_OBSERVE="${POST_KILL_OBSERVE:-60}"
DOCKER_IMAGE="${DOCKER_IMAGE:-alpine:latest}"
MC_DISABLE_AUTH="${MC_DISABLE_AUTH:-1}"

# DB 필수
: "${MC_DB_DSN:=postgres://mcuser:mcpw@127.0.0.1:5432/mc?sslmode=disable}"
export MC_DB_DSN

########################################
# 1) 결과 디렉토리
########################################
TS="$(date +%Y%m%d-%H%M%S)"
OUT="bench_artifacts/${TS}"
LOGDIR="${OUT}/logs"
mkdir -p "${LOGDIR}" "${OUT}/results"

{
  echo "### BENCH CONFIG"
  echo "NS=${NS}"
  echo "CONTROL_URL=${CONTROL_URL}"
  echo "AGENTS=${AGENTS}"
  echo "TASKS=${TASKS}"
  echo "HB_SEC=${HB_SEC}"
  echo "TTL_SEC=${TTL_SEC}"
  echo "KILL_PERCENT=${KILL_PERCENT}"
  echo "RUNTIME_BEFORE_KILL=${RUNTIME_BEFORE_KILL}s"
  echo "POST_KILL_OBSERVE=${POST_KILL_OBSERVE}s"
  echo "DOCKER_IMAGE=${DOCKER_IMAGE}"
  echo "MC_DB_DSN=${MC_DB_DSN}"
} | tee "${OUT}/config.txt"

########################################
# 2) 도커 확인 + 이미지 프리풀
########################################
if ! command -v docker >/dev/null 2>&1; then
  echo "[FATAL] docker not found"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "[FATAL] docker daemon not running or insufficient permission"; exit 1
fi

echo "==> Docker image pre-pull: ${DOCKER_IMAGE}"
docker pull "${DOCKER_IMAGE}" >/dev/null || {
  echo "[FATAL] failed to pull ${DOCKER_IMAGE}"
  exit 1
}

########################################
# 3) 컨트롤 시작 (재사용 가능) + 로그
########################################
CONTROL_PID=""
CONTROL_LOG="${LOGDIR}/control.log"

if curl -fsS "${CONTROL_URL}/api/health" >/dev/null 2>&1; then
  echo "==> Control already running @ ${CONTROL_URL}, reusing it"
  # 재사용 모드에선 로그파일이 없을 수 있다
  CONTROL_LOG=""
else
  echo "==> Starting control @ ${CONTROL_URL}"
  (
    MC_DB_DSN="${MC_DB_DSN}" MC_DISABLE_AUTH="${MC_DISABLE_AUTH}" \
      go run ./cmd/control \
        -http-port "${CONTROL_PORT}" \
        -ns "${NS}" \
        -bootstrap "" \
        > "${LOGDIR}/control.log" 2>&1
  ) &
  CONTROL_PID=$!

  # 최대 30초 대기
  ready=0
  for i in $(seq 1 30); do
    if curl -fsS "${CONTROL_URL}/api/health" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "[FATAL] control not responding on ${CONTROL_URL}"
    [ -n "$CONTROL_PID" ] && kill "$CONTROL_PID" 2>/dev/null || true
    echo "[HINT] see ${LOGDIR}/control.log (if created)"
    exit 1
  fi
fi

########################################
# 4) 부트스트랩 추출 (가능할 때만)
########################################
BOOTSTRAP=""
if [ -n "${CONTROL_LOG}" ] && [ -f "${CONTROL_LOG}" ]; then
  # 127.0.0.1/tcp 주소 우선
  BOOTSTRAP="$(awk '/addr: .*\/ip4\/127\.0\.0\.1\/tcp\/[^ ]*\/p2p\//{print $NF; exit}' "${CONTROL_LOG}" 2>/dev/null || true)"
  # 없으면 첫 번째 p2p 주소
  if [ -z "${BOOTSTRAP}" ]; then
    BOOTSTRAP="$(grep '/p2p/' "${CONTROL_LOG}" | sed -n 's/.*addr: \([^ ]*\).*/\1/p' | head -n1)"
  fi
else
  echo "[bench] control log not available (reused existing control), skipping bootstrap extraction"
fi
echo "BOOTSTRAP=${BOOTSTRAP}" >> "${OUT}/config.txt"

########################################
# 5) 에이전트 스폰
########################################
echo "==> Spawning ${AGENTS} agents"
AGENT_PIDS_FILE="${OUT}/agents.pids"
: > "${AGENT_PIDS_FILE}"

for i in $(seq 1 "${AGENTS}"); do
  LOGF="${LOGDIR}/agent-${i}.log"
  (
    MC_NS="${NS}" \
    HB_SEC="${HB_SEC}" \
    TTL_SEC="${TTL_SEC}" \
    go run ./cmd/agent \
      -ns "${NS}" \
      -control-url "${CONTROL_URL}" \
      -heartbeat-sec "${HB_SEC}" \
      -ttl-sec "${TTL_SEC}" \
      ${BOOTSTRAP:+-bootstrap "${BOOTSTRAP}"} \
      > "${LOGF}" 2>&1
  ) &
  echo "$!" >> "${AGENT_PIDS_FILE}"
  sleep 0.02
done
echo "agents started: $(wc -l < "${AGENT_PIDS_FILE}")"

########################################
# 6) 작업 제출 (noop manifest)
########################################
for i in $(seq 1 $TASKS); do
  job_id="job-${i}"

  # 1) 작업 생성
  curl -fsS -X POST "$CONTROL_URL/api/tasks" \
    -H 'Content-Type: application/json' \
    -d "{\"id\":\"$job_id\",\"image\":\"$DOCKER_IMAGE\",\"command\":[\"/bin/sh\",\"-c\",\"echo hi; sleep 1; echo done\"]}" >/dev/null

  # 2) 매니페스트 등록 (noop)
  curl -fsS -X POST "$CONTROL_URL/jobs/$job_id/manifest" \
    -H 'Content-Type: application/json' \
    -d '{"root_cid":"noop","providers":[]}' >/dev/null
done


########################################
# 7) 장애 유발
########################################
echo "==> Sleeping ${RUNTIME_BEFORE_KILL}s ..."
sleep "${RUNTIME_BEFORE_KILL}"

TOTAL_AGENTS="$(wc -l < "${AGENT_PIDS_FILE}")"
KILL_COUNT=$(( (TOTAL_AGENTS * KILL_PERCENT + 99) / 100 ))
echo "==> SIGKILL ${KILL_COUNT}/${TOTAL_AGENTS} agents"

shuf -n "${KILL_COUNT}" "${AGENT_PIDS_FILE}" > "${OUT}/killed_agents.txt" || cp "${AGENT_PIDS_FILE}" "${OUT}/killed_agents.txt"
while read -r pid; do
  kill -9 "${pid}" 2>/dev/null || true
done < "${OUT}/killed_agents.txt"

########################################
# 8) 관찰
########################################
echo "==> Observing ${POST_KILL_OBSERVE}s ..."
sleep "${POST_KILL_OBSERVE}"

########################################
# 9) 상태 수집 (DB 기준)
########################################
echo "==> Fetching final stats ..."
STATS=$(curl -fsS "${CONTROL_URL}/api/stats/tasks")
echo "${STATS}" | tee "${OUT}/results/stats.json"

QUEUED=$(echo "$STATS"    | jq -r '.queued')
ASSIGNED=$(echo "$STATS"  | jq -r '.assigned')
RUNNING=$(echo "$STATS"   | jq -r '.running')
SUCCEEDED=$(echo "$STATS" | jq -r '.succeeded')
FAILED=$(echo "$STATS"    | jq -r '.failed')

TOTAL=$((QUEUED + ASSIGNED + RUNNING + SUCCEEDED + FAILED))

{
  echo "total_tasks: $TOTAL"
  echo "queued     : $QUEUED"
  echo "assigned   : $ASSIGNED"
  echo "running    : $RUNNING"
  echo "succeeded  : $SUCCEEDED"
  echo "failed     : $FAILED"
} | tee "${OUT}/results/summary.txt"

########################################
# 10) 에이전트 정리
########################################
echo "==> Stopping remaining agents"
while read -r pid; do
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi
done < "${AGENT_PIDS_FILE}"
sleep 1

########################################
# 11) 컨트롤 정리 (우리가 띄웠을 때만)
########################################
if [ -n "${CONTROL_PID:-}" ]; then
  echo "==> Stopping control"
  kill "${CONTROL_PID}" 2>/dev/null || true
fi

echo "==> DONE. See ${OUT}"

