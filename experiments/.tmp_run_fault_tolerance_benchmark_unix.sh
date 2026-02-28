#!/usr/bin/env bash
set -euo pipefail

# -------- (?섍꼍 蹂?섎줈) 議곗젙 媛?ν븳 媛믩뱾 --------
NS="${NS:-mc}"
START_PORT="${CONTROL_PORT:-8080}"
CONTROL_TOKEN="${CONTROL_TOKEN:-dev}"
DISABLE_AUTH="${MC_DISABLE_AUTH:-0}"
MC_DB_DSN="${MC_DB_DSN:-}"

AGENTS="${AGENTS:-30}"
TASKS="${TASKS:-200}"
HB_SEC="${HB_SEC:-5}"
TTL_SEC="${TTL_SEC:-15}"
KILL_PERCENT="${KILL_PERCENT:-10}"
RUNTIME_BEFORE_KILL="${RUNTIME_BEFORE_KILL:-10}"
POST_KILL_OBSERVE="${POST_KILL_OBSERVE:-300}"

# 而⑦뀒?대꼫 ?뺤콉 (?묒뾽???ㅼ젣濡?而⑦뀒?대꼫 ?대?吏瑜??대떎??媛??
DOCKER_IMAGE="${DOCKER_IMAGE:-alpine:latest}"
AGENT_FLAGS="${AGENT_FLAGS:-}"

PROVIDERS_JSON="${PROVIDERS_JSON:-}"
SUBMIT_BATCH="${SUBMIT_BATCH:-20}"
SUBMIT_PAUSE_SEC="${SUBMIT_PAUSE_SEC:-0.15}"
AGENT_WARMUP_SEC="${AGENT_WARMUP_SEC:-3}"

# -------- 寃곌낵臾?寃쎈줈 --------
TS="$(date +%Y%m%d-%H%M%S)"
OUT="bench_artifacts/${TS}"
LOGDIR="${OUT}/logs"
mkdir -p "${LOGDIR}" "${OUT}/results"
SEEDER_LOG="${LOGDIR}/seeder.log"
# -------- cleanup & DB helper --------
CONTROL_PID=""
SEEDER_PID=""
AGENT_PIDS_FILE="${OUT}/agents.pids"

cleanup() {
  # ?⑥븘?덈뒗 ?먯씠?꾪듃??醫낅즺
  if [[ -f "${AGENT_PIDS_FILE}" ]]; then
    while read -r pid; do
      [[ -n "${pid}" ]] || continue
      if kill -0 "${pid}" 2>/dev/null; then
        kill "${pid}" 2>/dev/null || true
      fi
    done < "${AGENT_PIDS_FILE}"
  fi

  # 而⑦듃濡??꾨줈?몄뒪 醫낅즺
  if [[ -n "${CONTROL_PID}" ]]; then
    kill "${CONTROL_PID}" 2>/dev/null || true
  fi
}
  # <--- NEW: ?쒕뜑 ?꾨줈?몄뒪 醫낅즺
  if [[ -n "${SEEDER_PID}" ]]; then
    kill "${SEEDER_PID}" 2>/dev/null || true
  fi
trap cleanup EXIT INT TERM

run_psql_truncate() {
  if [[ -n "${MC_DB_DSN:-}" ]]; then
    psql "${MC_DB_DSN}" -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE demand_jobs;" || true
  elif [[ -n "${DB_URL:-}" ]]; then
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c "TRUNCATE TABLE demand_jobs;" || true
  else
    echo "[WARN] no DB dsn/url to truncate demand_jobs"
  fi
}
# -------- ?꾩빱 ?ъ쟾 ?뺤씤 --------
if ! command -v docker >/dev/null 2>&1; then
  echo "[FATAL] docker not found in PATH"; exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "[FATAL] docker daemon not running or insufficient permission"; exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "[FATAL] jq not found in PATH. Please install jq."; exit 1
fi
echo "==> Checking docker image: ${DOCKER_IMAGE}"
if ! docker image inspect "${DOCKER_IMAGE}" >/dev/null 2>&1; then
  echo "[WARN] image not found locally, trying to pull..."
  if ! docker pull "${DOCKER_IMAGE}"; then
    echo "[FATAL] failed to pull ${DOCKER_IMAGE} and no local image found."
    exit 1
  fi
else
  echo "[INFO] using local image ${DOCKER_IMAGE}"
fi

# -------- ?ы띁 ?⑥닔 --------
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


# -------- <--- NEW: ?쒕뜑(Seeder) ?쒖옉 --------
echo "==> Starting seeder (bootstrap anchor)"
# 'exec env -i'???섍꼍 蹂?섎? 珥덇린?뷀븯誘濡?紐낆떆???꾨떖
( exec env -i PATH="$PATH" HOME="$HOME" MC_NS="${NS}" \
    ./seeder -ns "${NS}" \
    > "${SEEDER_LOG}" 2>&1 ) &
SEEDER_PID=$!


sleep 1 # Seeder媛 濡쒓렇瑜??④만 ?뚭퉴吏 ?좎떆 ?湲?
# -------- 遺?몄뒪?몃옪 硫?곗＜??異붿텧 (Seeder 濡쒓렇?먯꽌) --------
BOOTSTRAP=""

# 1) localhost ?곗꽑
BOOTSTRAP=$(grep '/ip4/127.0.0.1/' "$SEEDER_LOG" | grep '/p2p/' | head -n1 | sed -E 's/^\[seeder\] addr: //')

# 2) docker bridge ?곗꽑?쒖쐞 2
if [ -z "$BOOTSTRAP" ]; then
  BOOTSTRAP=$(grep '/ip4/172.17.0.1/' "$SEEDER_LOG" | grep '/p2p/' | head -n1 | sed -E 's/^\[seeder\] addr: //')
fi

# 3) 洹몃옒???놁쑝硫?泥?以꾩뿉??prefix留??쒓굅
if [ -z "$BOOTSTRAP" ]; then
  BOOTSTRAP=$(grep -m1 '/p2p/' "$SEEDER_LOG" | sed -E 's/^\[seeder\] addr: //')
fi

if [ -z "$BOOTSTRAP" ]; then
  echo "[FATAL] Failed to extract bootstrap multiaddr from ${SEEDER_LOG}" >&2
  kill "${SEEDER_PID}" 2>/dev/null
  exit 1
fi

echo "==> Using bootstrap (from seeder): ${BOOTSTRAP}"
echo "BOOTSTRAP=${BOOTSTRAP}" >> "${OUT}/config.txt"


# -------- 而⑦듃濡ㅻ윭 ?쒖옉 --------
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
BOOTSTRAP=${BOOTSTRAP}
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

echo "==> Starting control @ ${CONTROL_URL}"
(
  exec env -i PATH="$PATH" HOME="$HOME" "${CONTROL_ENV[@]}" \
    ./control -ns "${NS}" -http-port "${PORT}" \
    -bootstrap "${BOOTSTRAP}" \
    > "${LOGDIR}/control.log" 2>&1
) &
CONTROL_PID=$!

# 而⑦듃濡ㅻ윭 HTTP 以鍮꾨맆 ?뚭퉴吏 ?湲?
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

# <--- NEW: BOOTSTRAP 二쇱냼?먯꽌 SEEDER_PEER_ID 異붿텧
# ?? /ip4/127.0.0.1/tcp/51351/p2p/12D3KooW... -> 12D3KooW...
SEEDER_PEER_ID=$(echo "$BOOTSTRAP" | sed -n 's|.*/p2p/||p')

if [ -z "$SEEDER_PEER_ID" ]; then
  echo "[FATAL] Failed to extract Peer ID from BOOTSTRAP address: ${BOOTSTRAP}" >&2
  kill "${SEEDER_PID}" 2>/dev/null


  exit 1
fi
echo "==> Using bootstrap: ${BOOTSTRAP}"
echo "BOOTSTRAP=${BOOTSTRAP}" >> "${OUT}/config.txt"
echo "==> Using Seeder Peer ID: ${SEEDER_PEER_ID}"

# -------- ?먯씠?꾪듃 ?앹꽦 --------
echo "==> Spawning ${AGENTS} agents"
: > "${AGENT_PIDS_FILE}"
for i in $(seq 1 "${AGENTS}"); do
  LOG="${LOGDIR}/agent-${i}.log"

  EXTRA_ENV=()
  [[ -n "${BOOTSTRAP}" ]] && EXTRA_ENV+=("BOOTSTRAP=${BOOTSTRAP}")

  (
    env PATH="${PATH}" HOME="${HOME}" \
        HB_SEC="${HB_SEC}" TTL_SEC="${TTL_SEC}" \
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
  sleep 0.02
done
echo "agents started: $(wc -l < "${AGENT_PIDS_FILE}")"

# -------- ?묒뾽 ?쒖텧 --------
sleep "$AGENT_WARMUP_SEC"
echo "==> Submitting ${TASKS} tasks"
AUTH_HEADER=()
[[ "${DISABLE_AUTH}" != "1" ]] && AUTH_HEADER=(-H "Authorization: Bearer ${CONTROL_TOKEN}")

TASKS_FILE="${OUT}/tasks.jsonl"
: > "${TASKS_FILE}"

for j in $(seq 1 "${TASKS}"); do
  JOB="job-${TS}-${j}"
 
 CMD_STR="echo agent:$RANDOM && sleep 30 && echo done"

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

  # Manifest ?꾩넚
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
  curl -sS -X POST "${CONTROL_URL}/jobs/${JOB}/manifest" \
    -H 'Content-Type: application/json' "${AUTH_HEADER[@]}" -d "${mbody}" >/dev/null || echo "[WARN] manifest failed for ${JOB}"

  if [[ "${http_code}" != "200" && "${http_code}" != "201" ]]; then
    echo "[WARN] submit ${JOB} -> HTTP ${http_code}"
  fi
  rm -f /tmp/resp.$$

  if (( j % 100 == 0 )); then
    echo "  submitted: ${j}"
  fi
done

# -------- ?μ븷 ?좊컻 --------
echo "==> Sleeping ${RUNTIME_BEFORE_KILL}s"
sleep "${RUNTIME_BEFORE_KILL}"

TOTAL_AGENTS="$(wc -l < "${AGENT_PIDS_FILE}")"
KILL_COUNT=$(( (TOTAL_AGENTS * KILL_PERCENT + 99) / 100 ))
echo "==> SIGKILL ${KILL_COUNT}/${TOTAL_AGENTS} agents"

shuf -n "${KILL_COUNT}" "${AGENT_PIDS_FILE}" > "${OUT}/killed_agents.txt" || cp "${AGENT_PIDS_FILE}" "${OUT}/killed_agents.txt"
while read -r pid; do
  kill -9 "${pid}" 2>/dev/null || true
done < "${OUT}/killed_agents.txt"

echo "==> Observing ${POST_KILL_OBSERVE}s"
sleep "${POST_KILL_OBSERVE}"

# -------- ?⑥? ?먯씠?꾪듃 以묒? --------
echo "==> Stopping remaining agents"
while read -r pid; do
  if kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
  fi
done < "${AGENT_PIDS_FILE}"
sleep 1

# -------- 遺꾩꽍 (媛?ν븳 寃쎌슦) --------
if [[ -x "$(dirname "$0")/analyze_metrics.py" ]]; then
  echo "==> Running metrics analyzer"
  python3 "$(dirname "$0")/analyze_metrics.py" "${OUT}" | tee "${OUT}/results/report.md"
else
  echo "==> Analyzer not found. Skipping metrics summarize."
fi

# -------- ?ㅽ뿕 ???뚯씠釉?鍮꾩슦湲?--------
echo "==> Truncating demand_jobs"
run_psql_truncate

echo "==> DONE. See ${OUT}"

