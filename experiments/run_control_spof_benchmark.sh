#!/usr/bin/env bash
set -euo pipefail

# -------- (환경 변수로) 조정 가능한 값들 --------
NS="${NS:-mc}"
START_PORT="${CONTROL_PORT:-8080}"
CONTROL_TOKEN="${CONTROL_TOKEN:-dev}"
DISABLE_AUTH="${MC_DISABLE_AUTH:-0}"
MC_DB_DSN="${MC_DB_DSN:-}" 

AGENTS="${AGENTS:-10}"
TASKS="${TASKS:-50}"
HB_SEC="${HB_SEC:-5}"
TTL_SEC="${TTL_SEC:-40}"

# --- 수정됨: 컨트롤 장애 시나리오 변수 ---
TASK_SLEEP_DURATION="${TASK_SLEEP_DURATION:-30}" # 작업이 실행될 시간 (초)
RUNTIME_BEFORE_CONTROL_KILL="${RUNTIME_BEFORE_CONTROL_KILL:-45}" # 컨트롤 죽이기 전 대기
CONTROL_DOWNTIME="${CONTROL_DOWNTIME:-40}" # 컨트롤이 죽어있을 시간
POST_RESTART_OBSERVE="${POST_RESTART_OBSERVE:-10}" # 컨트롤 재시작 후 상태 동기화 대기

# 컨테이너 정책
DOCKER_IMAGE="${DOCKER_IMAGE:-alpine:latest}"
AGENT_FLAGS="${AGENT_FLAGS:-}"

# Manifest providers JSON (선택 사항).
PROVIDERS_JSON="${PROVIDERS_JSON:-}"

# -------- 결과물 경로 --------
TS="$(date +%Y%m%d-%H%M%S)"
OUT="bench_artifacts/control_spof_${TS}"
LOGDIR="${OUT}/logs"
mkdir -p "${LOGDIR}" "${OUT}/results"

# <--- NEW: Seeder 로그 경로 추가
SEEDER_LOG="${LOGDIR}/seeder.log"

# -------- 도커/jq 사전 확인 --------
if ! command -v docker >/dev/null 2>&1; then
  echo "[FATAL] docker not found in PATH"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "[FATAL] docker daemon not running or insufficient permission"; exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "[FATAL] jq not found in PATH. Please install jq."; exit 1
fi
if [[ -n "$MC_DB_DSN" ]] && ! command -v psql >/dev/null 2>&1; then
  echo "[FATAL] MC_DB_DSN is set, but psql client not found in PATH."; exit 1
fi

echo "==> Docker image pre-pull: ${DOCKER_IMAGE}"
docker pull "${DOCKER_IMAGE}" >/dev/null || { echo "[FATAL] failed to pull ${DOCKER_IMAGE}"; exit 1; }

# -------- 헬퍼 함수 (기존과 동일) --------
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
cleanup() {
  # 이 스크립트가 직접 띄운 프로세스/컨테이너 최대한 정리
  # 남은 컨트롤/에이전트 컨테이너 이름 규칙으로 지우기
  docker ps --format '{{.Names}}' 2>/dev/null | grep -E "^${NS}-" | while read -r name; do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done

  # 백그라운드 프로세스도 죽이기
  pkill -P $$ 2>/dev/null || true
}
trap cleanup EXIT INT TERM

run_psql_truncate() {
  if [[ -n "${MC_DB_DSN:-}" ]]; then
    psql "${MC_DB_DSN}" -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE demand_jobs;" || true
  elif [[ -n "${DB_URL:-}" ]]; then
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE demand_jobs;" || true
  else
    echo "[WARN] no DB DSN/URL to TRUNCATE demand_jobs"
  fi
}

http_ready() {
    local url="$1"
    curl -fsS "$url/api/health" >/dev/null 2>&1
}

func_check_status() {
    local MODE=${1:-http}
    local LOG_PREFIX=${2:-"==>"}

    if [[ "$MODE" == "db" && -n "$MC_DB_DSN" ]]; then
        echo "${LOG_PREFIX} 📊 DB 상태 확인 (Directly querying demand_jobs)"
        
        # 에러를 무시하지 않도록 psql 실행
        psql "$MC_DB_DSN" -c "
            SELECT status, manifest_root_cid, COUNT(*) as count,
                   SUM(CASE WHEN lease_token > 1 THEN 1 ELSE 0 END) as competed,
                   MAX(lease_token) as max_lease_token
            FROM demand_jobs
            -- <--- 수정됨: 'ns' 컬럼이 존재하지 않아 오류 발생. 
            -- WHERE ns = '${NS}'
            GROUP BY status, manifest_root_cid
            ORDER BY status;
        " || echo "[WARN] psql query failed."
    
    elif [[ "$MODE" == "http" ]]; then
        echo "${LOG_PREFIX} 📊 HTTP 통계 확인 (Control: ${CONTROL_URL})"
        
        # AUTH_HEADER는 이미 전역으로 설정되어 있어야 함
        if ! curl -fsS "${AUTH_HEADER[@]}" "${CONTROL_URL}/api/stats/tasks" | jq; then
            echo "[WARN] Control 서버($CONTROL_URL)에 연결할 수 없습니다."
        fi
    else
        echo "[INFO] 상태 확인 스킵 (Mode: $MODE, DSN set: ${MC_DB_DSN:-'false'})"
    fi
}


# -------- <--- NEW: 시더(Seeder) 시작 --------
echo "==> Starting seeder (bootstrap anchor)"
# 'exec env -i'는 환경 변수를 초기화하므로 명시적 전달
( exec env -i PATH="$PATH" HOME="$HOME" MC_NS="${NS}" \
    ./seeder -ns "${NS}" \
    > "${SEEDER_LOG}" 2>&1 ) &
SEEDER_PID=$!
sleep 1 # Seeder가 로그를 남길 때까지 잠시 대기
# -------- 부트스트랩 멀티주소 추출 (Seeder 로그에서) --------
BOOTSTRAP=""

# 1) localhost 우선
BOOTSTRAP=$(grep '/ip4/127.0.0.1/' "$SEEDER_LOG" | grep '/p2p/' | head -n1 | sed -E 's/^\[seeder\] addr: //')

# 2) docker bridge 우선순위 2
if [ -z "$BOOTSTRAP" ]; then
  BOOTSTRAP=$(grep '/ip4/172.17.0.1/' "$SEEDER_LOG" | grep '/p2p/' | head -n1 | sed -E 's/^\[seeder\] addr: //')
fi

# 3) 그래도 없으면 첫 줄에서 prefix만 제거
if [ -z "$BOOTSTRAP" ]; then
  BOOTSTRAP=$(grep -m1 '/p2p/' "$SEEDER_LOG" | sed -E 's/^\[seeder\] addr: //')
fi

if [ -z "$BOOTSTRAP" ]; then
  echo "[FATAL] Failed to extract bootstrap multiaddr from ${SEEDER_LOG}" >&2
  kill "${SEEDER_PID}" 2>/dev/null
  exit 1
fi

echo "==> Using bootstrap (from seeder): ${BOOTSTRAP}"
SEEDER_PEER_ID=$(echo "$BOOTSTRAP" | sed -n 's|.*/p2p/||p')
if [ -z "$SEEDER_PEER_ID" ]; then
  echo "[FATAL] Failed to extract Peer ID from BOOTSTRAP address: ${BOOTSTRAP}" >&2
  kill "${SEEDER_PID}" 2>/dev/null
  exit 1
fi
echo "==> Using Seeder Peer ID: ${SEEDER_PEER_ID}"

echo "BOOTSTRAP=${BOOTSTRAP}" >> "${OUT}/config.txt"


# -------- 컨트롤러 시작 --------

#<--- NEW: BOOTSTRAP 주소에서 SEEDER_PEER_ID 추출
PORT="$(pick_free_port "$START_PORT")"
CONTROL_URL="http://127.0.0.1:${PORT}"

cat > "${OUT}/config.txt" <<EOF
### BENCH CONFIG (Control SPOF Test)
NS=${NS}
CONTROL_URL=${CONTROL_URL}
AGENTS=${AGENTS}
TASKS=${TASKS}
TASK_SLEEP_DURATION=${TASK_SLEEP_DURATION}s
RUNTIME_BEFORE_CONTROL_KILL=${RUNTIME_BEFORE_CONTROL_KILL}s
CONTROL_DOWNTIME=${CONTROL_DOWNTIME}s
POST_RESTART_OBSERVE=${POST_RESTART_OBSERVE}s
MC_DB_DSN=${MC_DB_DSN:-"(not set, likely in-memory)"}
BOOTSTRAP=${BOOTSTRAP}
Artifacts: ${OUT}
EOF

cat "${OUT}/config.txt"

CONTROL_ENV=()
CONTROL_ENV+=(MC_NS="${NS}")
[[ -n "${MC_DB_DSN}" ]] && CONTROL_ENV+=(MC_DB_DSN="${MC_DB_DSN}")
if [[ "${DISABLE_AUTH}" == "1" ]]; then
  CONTROL_ENV+=(MC_DISABLE_AUTH=1)
else
  CONTROL_ENV+=(CONTROL_TOKEN="${CONTROL_TOKEN}")
fi

echo "==> Starting control (Attempt 1) @ ${CONTROL_URL}"
# 'exec env -i'는 환경 변수를 초기화하므로 CONTROL_ENV 배열로 명시적 전달
( exec env -i PATH="$PATH" HOME="$HOME" "${CONTROL_ENV[@]}" \
    ./control -ns "${NS}" -http-port "${PORT}" \
    -bootstrap "${BOOTSTRAP}" \
    > "${LOGDIR}/control.log" 2>&1 ) & # <--- MODIFIED: -bootstrap 플래그 추가
CONTROL_PID=$!

# 컨트롤러 HTTP 준비될 때까지 대기
for i in {1..50}; do
  if http_ready "${CONTROL_URL}"; then break; fi
  sleep 0.1
  if ! kill -0 "${CONTROL_PID}" 2>/dev/null; then
    echo "[FATAL] Control server failed to start (Attempt 1). Check ${LOGDIR}/control.log"
    kill "${SEEDER_PID}" 2>/dev/null # <--- NEW: 시더도 정리
    exit 1
  fi
done
if ! http_ready "${CONTROL_URL}"; then
  echo "[FATAL] Control server timed out (Attempt 1)."
  kill "${CONTROL_PID}" 2>/dev/null
  kill "${SEEDER_PID}" 2>/dev/null # <--- NEW: 시더도 정리
  exit 1
fi

# -------- 에이전트 생성 (기존과 동일) --------
# (참고: 이 로직은 이미 BOOTSTRAP 변수를 사용하므로 수정 불필요)
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
done
echo "agents started: $(wc -l < "${OUT}/agents.pids")"

# -------- 작업 제출 --------
DEMAND_URL="${DEMAND_URL:-$CONTROL_URL}"
echo "==> Submitting ${TASKS} tasks (each sleeps ${TASK_SLEEP_DURATION}s)"
AUTH_HEADER=()
[[ "${DISABLE_AUTH}" != "1" ]] && AUTH_HEADER=(-H "Authorization: Bearer ${CONTROL_TOKEN}")

TASKS_FILE="${OUT}/tasks.jsonl"
: > "${TASKS_FILE}"

for j in $(seq 1 "${TASKS}"); do
  JOB="job-${TS}-${j}"

  CMD_STR="echo agent: task ${JOB} running for ${TASK_SLEEP_DURATION}s && sleep ${TASK_SLEEP_DURATION} && echo agent: task ${JOB} done"

  body=$(jq -n \
    --arg id "$JOB" \
    --arg image "$DOCKER_IMAGE" \
    --arg cmd_str "$CMD_STR" \
    --arg peer_id "$SEEDER_PEER_ID" \
    --arg seeder_addr "$BOOTSTRAP" \
    '{
      id: $id,
      image: $image,
      command: ["/bin/sh", "-lc", $cmd_str],
      peer_id: $peer_id,
      addrs: [$seeder_addr]
    }')
  http_code="$(curl -sS -o /tmp/resp.$$ -w '%{http_code}' -X POST "${CONTROL_URL}/api/tasks" \
                 -H 'Content-Type: application/json' "${AUTH_HEADER[@]}" -d "${body}" || true)"
  cat /tmp/resp.$$ >> "${TASKS_FILE}"; echo >> "${TASKS_FILE}"

  # Manifest 전송 (기존과 동일)
  if [[ -n "${PROVIDERS_JSON}" ]]; then
    mbody="${PROVIDERS_JSON}"
  else
    mbody=$(cat <<EOF
{
  
  "root_cid": "noop",
  "providers": [],
  "enc_meta": "",
  "version": 1,
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
)
  fi
curl -fsS -X POST "${DEMAND_URL}/jobs/${JOB}/manifest" \
  -H 'Content-Type: application/json' "${AUTH_HEADER[@]}" \
  -d "${mbody}" \
  >/dev/null || echo "[WARN] manifest failed for ${JOB}"


  if [[ "${http_code}" != "200" && "${http_code}" != "201" ]]; then
    echo "[WARN] submit ${JOB} -> HTTP ${http_CODE}"
  fi
  rm -f /tmp/resp.$$
  if (( j % 100 == 0 )); then echo "  submitted: ${j}"; fi
done

# -------- ⚠ 컨트롤 장애 유발 ⚠ --------
echo "==> (PHASE 1) Waiting ${RUNTIME_BEFORE_CONTROL_KILL}s for agents to claim tasks"
sleep "${RUNTIME_BEFORE_CONTROL_KILL}"

# <--- 수정됨: 장애 직전 상태 확인
echo "--- (상태 1: 장애 직전) ---"
func_check_status "http" "[PHASE 1]" # 컨트롤이 살아있으므로 http 확인
func_check_status "db"   "[PHASE 1]" # DB도 확인
echo "------------------------------"
# ---- save PHASE 1 db snapshot ----
if [[ -n "${MC_DB_DSN}" ]]; then
  PH1_DB_RAW=$(psql "$MC_DB_DSN" -At -c "SELECT status, count, competed FROM (
      SELECT status, COUNT(*) AS count,
             SUM(CASE WHEN lease_token > 1 THEN 1 ELSE 0 END) AS competed
      FROM demand_jobs
      GROUP BY status
  ) t;") || PH1_DB_RAW=""

  # 기본값
  PH1_QUEUED=0; PH1_RUNNING=0; PH1_COMPETED=0

  while IFS='|' read -r st cnt comp; do
    st=$(echo "$st" | xargs)
    cnt=$(echo "$cnt" | xargs)
    comp=$(echo "$comp" | xargs)
    if [[ "$st" == "queued" ]]; then
      PH1_QUEUED=$cnt
    elif [[ "$st" == "running" ]]; then
      PH1_RUNNING=$cnt
      PH1_COMPETED=$comp
    fi
  done <<< "$PH1_DB_RAW"
fi


echo "==> (PHASE 2) SIGKILL Control Server (PID: ${CONTROL_PID})"
kill -9 "${CONTROL_PID}" 2>/dev/null || true
echo "==> Control is DOWN. Observing ${CONTROL_DOWNTIME}s..."
echo "==> (Agents should be finishing tasks during this time without Control)"

# <--- 수정됨: 컨트롤이 죽어있는 동안 DB 상태 확인
echo "--- (상태 2: 장애 발생 중) ---"
echo " (컨트롤이 재시작될 때까지 잠시 대기하며 DB 상태 변화 관찰)"
sleep 2 # DB가 즉시 반영되도록 잠시 대기
func_check_status "http" "[PHASE 2]" # '연결 거부'가 떠야 정상
func_check_status "db"   "[PHASE 2]" # *핵심: 이 DB에서 'succeeded'가 증가해야 함*
echo "------------------------------"

sleep "${CONTROL_DOWNTIME}"


# -------- 🔬 컨트롤 복구 및 검증 🔬 --------
echo "==> (PHASE 3) Restarting control server"
# <--- 수정됨: 컨트롤 서버를 동일한 환경변수와 부트스트랩으로 재시작
( exec env -i PATH="$PATH" HOME="$HOME" "${CONTROL_ENV[@]}" \
    ./control -ns "${NS}" -http-port "${PORT}" \
    -bootstrap "${BOOTSTRAP}" \
    > "${LOGDIR}/control.restarted.log" 2>&1 ) & # <--- MODIFIED: -bootstrap 플래그 추가
CONTROL_PID=$!

# 컨트롤러 HTTP 준비될 때까지 대기
for i in {1..50}; do
  if http_ready "${CONTROL_URL}"; then break; fi
  sleep 0.1
  if ! kill -0 "${CONTROL_PID}" 2>/dev/null; then
    echo "[FATAL] Control server failed to RESTART. Check ${LOGDIR}/control.restarted.log"
    # 에이전트/시더 정리 후 종료
    while read -r pid; do if kill -0 "${pid}" 2>/dev/null; then kill "${pid}" 2>/dev/null || true; fi; done < "${OUT}/agents.pids"
    kill "${SEEDER_PID}" 2>/dev/null # <--- NEW: 시더도 정리
    exit 1
  fi
done
echo "==> Control RESTARTED @ ${CONTROL_URL}"

echo "==> (PHASE 4) Waiting ${POST_RESTART_OBSERVE}s for state sync"
sleep "${POST_RESTART_OBSERVE}"

# <--- 수정됨: 복구 후 상태 확인
echo "--- (상태 3: 복구 완료 후) ---"
echo " (재시작된 컨트롤이 DB 상태를 로드했는지 확인)"
func_check_status "http" "[PHASE 4]" # HTTP 통계가 'succeeded'를 반영해야 함
func_check_status "db"   "[PHASE 4]" # HTTP와 DB 상태가 일치해야 함
echo "------------------------------"
# ---- save PHASE 4 db snapshot ----
if [[ -n "${MC_DB_DSN}" ]]; then
  PH4_DB_RAW=$(psql "$MC_DB_DSN" -At -c "SELECT status, count, competed FROM (
      SELECT status, COUNT(*) AS count,
             SUM(CASE WHEN lease_token > 1 THEN 1 ELSE 0 END) AS competed
      FROM demand_jobs
      GROUP BY status
  ) t;") || PH4_DB_RAW=""

  PH4_QUEUED=0; PH4_RUNNING=0; PH4_COMPETED=0

  while IFS='|' read -r st cnt comp; do
    st=$(echo "$st" | xargs)
    cnt=$(echo "$cnt" | xargs)
    comp=$(echo "$comp" | xargs)
    if [[ "$st" == "queued" ]]; then
      PH4_QUEUED=$cnt
    elif [[ "$st" == "running" ]]; then
      PH4_RUNNING=$cnt
      PH4_COMPETED=$comp
    fi
  done <<< "$PH4_DB_RAW"
fi

echo "==> Fetching final task states from RESTARTED control"
# <--- 참고: 이 API는 여전히 500 오류를 반환할 수 있습니다 (control 서버 내부 문제)
curl -fsS "${AUTH_HEADER[@]}" "${CONTROL_URL}/api/tasks" > "${OUT}/results/final_tasks_state.json"

# -------- 남은 프로세스 중지 --------
echo "==> Stopping remaining agents"
while read -r pid; do
  if kill -0 "${pid}" 2>/dev/null; then kill "${pid}" 2>/dev/null || true; fi
done < "${OUT}/agents.pids"

echo "==> Stopping control and seeder" # <--- MODIFIED
# 재시작된 컨트롤 종료
kill "${CONTROL_PID}" 2>/dev/null || true
# <--- NEW: 시더 종료
kill "${SEEDER_PID}" 2>/dev/null || true

# -------- 분석 (jq) --------
echo "==> (PHASE 5) Analyzing results..."
REPORT_FILE="${OUT}/results/report.md"
touch "${REPORT_FILE}"
echo "==> (PHASE 5) Analyzing results..."

REPORT_FILE="${OUT}/results/report.md"
: > "${REPORT_FILE}"

INC_COMP=$(( PH4_COMPETED - PH1_COMPETED ))
if [[ $INC_COMP -lt 0 ]]; then INC_COMP=0; fi

{
  echo "# Control SPOF Test Report (${RUN_ID:-$(date +%Y%m%d-%H%M%S)})"
  echo
  echo "Test to verify that tasks complete successfully even if the Control server is killed, as long as the Demand(DB) is persistent."
  
  echo
  echo "## Summary"
  echo "| Parameter | Value |"
  echo "|---|---|"
  echo "| Total Tasks Submitted | ${TASKS} |"
  echo "| Task Duration | ${TASK_SLEEP_DURATION} |"
  echo "| Control Downtime | ${CONTROL_DOWNTIME} |"
  echo "| Persistent DB (MC_DB_DSN) | ${MC_DB_DSN} |"
  echo
  echo "### 3.1. PHASE 1: 장애 발생 직전"
  echo
  echo "- **DB 상태:** \`queued: ${PH1_QUEUED}\`, \`running: ${PH1_RUNNING}\`, \`competed: ${PH1_COMPETED}\`"
  echo "- **분석:** 총 ${TASKS}개의 작업을 넣었고, 컨트롤을 죽이기 직전에는 에이전트가 ${PH1_RUNNING}개를 실행 중이었다."
  echo
  echo "### 3.2. PHASE 4: 장애 복구 완료"
  echo

} >> "${REPORT_FILE}"

cat "${REPORT_FILE}"
run_psql_truncate

echo "==> DONE. See ${OUT}"

