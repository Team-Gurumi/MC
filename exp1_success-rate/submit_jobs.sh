#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
. "${SCRIPT_DIR}/config.sh"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <N>" >&2
  exit 1
fi

N="$1"
if ! [[ "$N" =~ ^[0-9]+$ ]] || [[ "$N" -le 0 ]]; then
  echo "N must be a positive integer" >&2
  exit 1
fi

: "${CONTROL_URL:?CONTROL_URL is required}"

WORKLOAD_TYPE="${WORKLOAD_TYPE:-cpu}"
JOB_PREFIX="${JOB_PREFIX:-exp1-$(date -u +%Y%m%dT%H%M%SZ)}"
WORKLOAD_SEED="${WORKLOAD_SEED:-}"
if [[ -z "$WORKLOAD_SEED" ]]; then
  WORKLOAD_SEED="$(printf '%s' "$JOB_PREFIX" | cksum | awk '{print $1}')"
fi

CONTROL_URL="${CONTROL_URL%/}"
SUBMIT_URL="${CONTROL_URL}/api/tasks"

mkdir -p "$(dirname "${SUBMIT_FAILURE_LOG}")"

AUTH_ARGS=()
if [[ -n "${CONTROL_TOKEN:-}" ]]; then
  AUTH_ARGS=(-H "Authorization: Bearer ${CONTROL_TOKEN}")
fi

log_failure() {
  # Persist submission failures for auditability across runs.
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "${SUBMIT_FAILURE_LOG}"
}

# Build workload command JSON based on scenario type.
# This keeps experiment shape deterministic and easy to review.
build_command_json() {
  case "$WORKLOAD_TYPE" in
    cpu)
      printf '["sh","-lc","WORKLOAD_SEED=%s; i=0; while [ $i -lt 3000000 ]; do i=$((i+1)); done; echo cpu_done"]' "$WORKLOAD_SEED"
      ;;
    io)
      printf '["sh","-lc","WORKLOAD_SEED=%s; dd if=/dev/zero of=/tmp/blob bs=1M count=16 >/dev/null 2>&1; sync; wc -c /tmp/blob >/dev/null; echo io_done"]' "$WORKLOAD_SEED"
      ;;
    *)
      echo "unsupported workload_type: $WORKLOAD_TYPE (use cpu|io)" >&2
      exit 1
      ;;
  esac
}

COMMAND_JSON="$(build_command_json)"

submit_one_job() {
	local job_id="$1"
	local payload
	payload=$(printf '{"id":"%s","image":"%s","command":%s,"manifest_root_cid":"%s","ttl_sec":%s,"heartbeat_sec":%s}' \
		"$job_id" "$IMAGE" "$COMMAND_JSON" "$MANIFEST_ROOT_CID" "$TTL_SEC" "$HEARTBEAT_SEC")

  local attempt=1
  while [[ "$attempt" -le 3 ]]; do
    local resp_file http_code body_oneline echoed_id
    resp_file="$(mktemp)"

    if [[ -n "${CONTROL_TOKEN:-}" ]]; then
      http_code="$(curl -sS -o "$resp_file" -w '%{http_code}' \
        -X POST "$SUBMIT_URL" \
        -H 'Content-Type: application/json' \
        -H "Authorization: Bearer ${CONTROL_TOKEN}" \
        -d "$payload" || true)"
    else
      http_code="$(curl -sS -o "$resp_file" -w '%{http_code}' \
        -X POST "$SUBMIT_URL" \
        -H 'Content-Type: application/json' \
        -d "$payload" || true)"
    fi

    if [[ "$http_code" == "200" || "$http_code" == "201" ]]; then
      echoed_id=$(grep -Eo '"(job_id|id)"[[:space:]]*:[[:space:]]*"[^"]+"' "$resp_file" | head -n1 | sed -E 's/.*"([^"]+)"$/\1/')
      rm -f "$resp_file"
      if [[ -n "$echoed_id" ]]; then
        printf '%s\n' "$echoed_id"
      else
        printf '%s\n' "$job_id"
      fi
      return 0
    fi

    body_oneline="$(tr '\n' ' ' < "$resp_file")"
    rm -f "$resp_file"
    log_failure "job_id=${job_id} attempt=${attempt} status=${http_code} body=${body_oneline}"

    if [[ "$attempt" -lt 3 ]]; then
      sleep $((1 << (attempt - 1)))
    fi
    attempt=$((attempt + 1))
  done

  return 1
}

submitted=0
for ((i=1; i<=N; i++)); do
  job_id="${JOB_PREFIX}-$(printf '%05d' "$i")"
  if out_id="$(submit_one_job "$job_id")"; then
    printf '%s\n' "$out_id"
    submitted=$((submitted + 1))
  fi
  # harness stability patch: tiny throttle to reduce HTTP burst churn.
  sleep 0.02
done

echo "submitted_count=${submitted}" >&2
