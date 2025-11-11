#!/usr/bin/env bash
set -euo pipefail

# -------- (환경 변수로) 조정 가능한 값들 --------
NS="${NS:-mc}"
START_PORT="${CONTROL_PORT:-8080}"
CONTROL_TOKEN="${CONTROL_TOKEN:-dev}"
DISABLE_AUTH="${MC_DISABLE_AUTH:-0}"
MC_DB_DSN="${MC_DB_DSN:-}" 

AGENTS="${AGENTS:-10}" # <--- 실행할 에이전트 수
HB_SEC="${HB_SEC:-5}"
TTL_SEC="${TTL_SEC:-20}"
OBSERVE_SEC="${OBSERVE_SEC:-100}" # <--- 작업 제출 후, 정리 전까지 대기할 시간 (초)


DOCKER_IMAGE="${DOCKER_IMAGE:-dpokidov/imagemagick:7.1.1-17-ubuntu}"
AGENT_FLAGS="${AGENT_FLAGS:-}"


DATA_DIR="${DATA_DIR:-/home/yelin/MC/data}" # <--- 소스 이미지가 있는 seeder의 data 폴더

# -------- 결과물 경로 --------
TS="$(date +%Y%m%d-%H%M%S)"
OUT="bench_artifacts/grayscale_batch_${TS}"
LOGDIR="${OUT}/logs"
mkdir -p "${LOGDIR}" "${OUT}/results"

SEEDER_LOG="${LOGDIR}/seeder.log"
AGENT_PIDS_FILE="${OUT}/agents.pids"

# -------- PID 추적 및 정리 --------
CONTROL_PID=""
SEEDER_PID=""

cleanup() {
  echo "[CLEANUP] Stopping all background processes..."
  
  # 1. 에이전트 종료 (PID 파일 사용)
  if [[ -f "${AGENT_PIDS_FILE}" ]]; then
    while read -r pid; do
      [[ -n "${pid}" ]] || continue
      if kill -0 "${pid}" 2>/dev/null; then
        echo "[CLEANUP] Stopping agent (PID: $pid)"
        kill "${pid}" 2>/dev/null || true
      fi
    done < "${AGENT_PIDS_FILE}"
  fi

  # 2. 컨트롤 종료
  if [[ -n "${CONTROL_PID}" ]]; then
    echo "[CLEANUP] Stopping control (PID: ${CONTROL_PID})"
    kill "${CONTROL_PID}" 2>/dev/null || true
  fi
  
  # 3. 시더 종료
  if [[ -n "${SEEDER_PID}" ]]; then
    echo "[CLEANUP] Stopping seeder (PID: ${SEEDER_PID})"
    kill "${SEEDER_PID}" 2>/dev/null || true
  fi
  
  # 4. 기타 자식 프로세스 정리 (Fallback)
  pkill -P $$ 2>/dev/null || true
  echo "[CLEANUP] Done."
}
trap cleanup EXIT INT TERM

# -------- DB 비우기 헬퍼 --------
run_psql_truncate() {
  if [[ -n "${MC_DB_DSN:-}" ]]; then
    echo "[INFO] Truncating demand_jobs table..."
    psql "${MC_DB_DSN}" -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE demand_jobs;" || true
  fi
}

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

# -------- 포트/HTTP 헬퍼 --------
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
    local url="$1"
    curl -fsS "$url/api/health" >/dev/null 2>&1
}

# -------- 상태 확인 헬퍼 (DB/HTTP) --------
func_check_status() {
    local MODE=${1:-http}
    local LOG_PREFIX=${2:-"==>"}
    AUTH_HEADER=()
    [[ "${DISABLE_AUTH}" != "1" ]] && AUTH_HEADER=(-H "Authorization: Bearer ${CONTROL_TOKEN}")

    if [[ "$MODE" == "db" && -n "$MC_DB_DSN" ]]; then
        echo "${LOG_PREFIX} 📊 DB 상태 확인 (Directly querying demand_jobs)"
        psql "$MC_DB_DSN" -c "
            SELECT status, manifest_root_cid, COUNT(*) as count,
                   SUM(CASE WHEN lease_token > 1 THEN 1 ELSE 0 END) as competed
            FROM demand_jobs
            GROUP BY status, manifest_root_cid
            ORDER BY status;
        " || echo "[WARN] psql query failed."
    
    elif [[ "$MODE" == "http" ]]; then
        echo "${LOG_PREFIX} 📊 HTTP 통계 확인 (Control: ${CONTROL_URL})"
        if ! curl -fsS "${AUTH_HEADER[@]}" "${CONTROL_URL}/api/stats/tasks" | jq; then
            echo "[WARN] Control 서버($CONTROL_URL)에 연결할 수 없습니다."
        fi
    fi
}


# -------- 1. 시더(Seeder) 시작 --------
echo "==> (1/5) Starting seeder (bootstrap anchor)"
( exec env -i PATH="$PATH" HOME="$HOME" MC_NS="${NS}" \
    ./seeder -ns "${NS}" -base "${DATA_DIR}" \
    > "${SEEDER_LOG}" 2>&1 ) &

sleep 2
# -------- 부트스트랩/PeerID 추출 --------
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
  exit 1
fi
echo "==> Using bootstrap (from seeder): ${BOOTSTRAP}"

SEEDER_PEER_ID=$(echo "$BOOTSTRAP" | sed -n 's|.*/p2p/||p')
if [ -z "$SEEDER_PEER_ID" ]; then
  echo "[FATAL] Failed to extract Peer ID from BOOTSTRAP address: ${BOOTSTRAP}" >&2
  exit 1
fi
echo "==> Using Seeder Peer ID: ${SEEDER_PEER_ID}"


# -------- 2. 컨트롤러 시작 --------
PORT="$(pick_free_port "$START_PORT")"
CONTROL_URL="http://127.0.0.1:${PORT}"

echo "==> (2/5) Starting control @ ${CONTROL_URL}"
CONTROL_ENV=()
CONTROL_ENV+=(MC_NS="${NS}")
[[ -n "${MC_DB_DSN}" ]] && CONTROL_ENV+=(MC_DB_DSN="${MC_DB_DSN}")
if [[ "${DISABLE_AUTH}" == "1" ]]; then
  CONTROL_ENV+=(MC_DISABLE_AUTH=1)
else
  CONTROL_ENV+=(CONTROL_TOKEN="${CONTROL_TOKEN}")
fi

( exec env -i PATH="$PATH" HOME="$HOME" "${CONTROL_ENV[@]}" \
    ./control -ns "${NS}" -http-port "${PORT}" \
    -bootstrap "${BOOTSTRAP}" \
    > "${LOGDIR}/control.log" 2>&1 ) &
CONTROL_PID=$!

# 컨트롤러 HTTP 준비될 때까지 대기
for i in {1..50}; do
  if http_ready "${CONTROL_URL}"; then break; fi
  sleep 0.1
  if ! kill -0 "${CONTROL_PID}" 2>/dev/null; then
    echo "[FATAL] Control server failed to start. Check ${LOGDIR}/control.log"
    exit 1
  fi
done
if ! http_ready "${CONTROL_URL}"; then
  echo "[FATAL] Control server timed out."
  exit 1
fi
echo "==> Control server is READY."


# -------- 3. 에이전트 생성 --------
echo "==> (3/5) Spawning ${AGENTS} agents"
: > "${AGENT_PIDS_FILE}"
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
  echo "$!" >> "${AGENT_PIDS_FILE}"
done
echo "==> Agents started: $(wc -l < "${AGENT_PIDS_FILE}")"

# -------- 4. 작업 제출 (이미지 1개당 1개) --------
echo "==> (4/5) Submitting tasks (one per image in $DATA_DIR)..."
AUTH_HEADER=()
[[ "${DISABLE_AUTH}" != "1" ]] && AUTH_HEADER=(-H "Authorization: Bearer ${CONTROL_TOKEN}")

IMAGE_FILES=()
while IFS= read -r file; do
    IMAGE_FILES+=("$(basename "$file")")
done < <(find "$DATA_DIR" -maxdepth 1 -type f \( -iname \*.jpg -o -iname \*.jpeg -o -iname \*.png \) || true)

if [ ${#IMAGE_FILES[@]} -eq 0 ]; then
  echo "[FATAL] No image files (jpg, jpeg, png) found in $DATA_DIR"
  exit 1
fi
echo "==> Found ${#IMAGE_FILES[@]} images. Submitting..."



i=0
for SOURCE_IMAGE in "${IMAGE_FILES[@]}"; do
  i=$((i+1))
  JOB="gray-$TS-$i"
  OUTPUT_NAME="output_gray_${SOURCE_IMAGE}"

  # 1) task 등록
  body=$(jq -n \
  --arg id "$JOB" \
  --arg image "$DOCKER_IMAGE" \
  --arg input_name "input/$SOURCE_IMAGE" \
  --arg output_name "$OUTPUT_NAME" \
  '{
    id: $id,
    image: $image,
    command: [$input_name, "-colorspace", "Gray", $output_name]
  }')


  curl -sS -X POST "${CONTROL_URL}/api/tasks" \
    -H "Content-Type: application/json" "${AUTH_HEADER[@]}" -d "$body" >/dev/null

  # 2) manifest 등록
  mbody=$(jq -n \
    --arg root_cid "$SOURCE_IMAGE" \
    --arg peer_id "$SEEDER_PEER_ID" \
    --arg seeder_addr "$BOOTSTRAP" \
    '{
      root_cid: $root_cid,
      providers: [{
        "peer_id": $peer_id,
        "addrs": [$seeder_addr]
      }]
    }')

  curl -sS -X POST "${CONTROL_URL}/jobs/$JOB/manifest" \
    -H "Content-Type: application/json" "${AUTH_HEADER[@]}" -d "$mbody" >/dev/null

  if (( i % 50 == 0 )); then
    echo "  ... submitted $i tasks (Current: $SOURCE_IMAGE)"
  fi
done
echo "==> Task submission DONE. Total submitted: $i tasks."

echo "==> Waiting a bit for agents to pick up tasks..."
sleep $OBSERVE_SEC

func_check_status "db"   "[FINAL]"

# DB 비우기
run_psql_truncate
COLLECT_DIR="./all_gray_results"
mkdir -p "$COLLECT_DIR"

# 결과 폴더는 탐색 대상에서 제외하고, 덮어쓰기 없이 조용히 복사
find ./work -type f -name "output_gray_*.png" ! -path "./all_gray_results/*" \
  -exec cp -u {} "$COLLECT_DIR"/ \;

