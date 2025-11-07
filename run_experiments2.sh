#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# 내결함성 벤치마크 - 자동화 스크립트

# 실험 2: 스트레스 테스트 (Kill % vs Success Rate)
#    - KILL_PERCENT 10, 20, 40, 60%로 늘려가며 테스트
#
# * 모든 상세 로그는 'experiment_logs/' 디렉터리에 저장됩니다.
# * 각 실행 후 핵심 결과가 터미널에 요약됩니다.
#
# 요구 사항:
# - ./run_fault_tolerance_benchmark.sh (실행 가능해야 함)
# - ./analyze_metrics.py (run...sh 스크립트가 호출할 수 있어야 함)
# ==============================================================================

# --- 공통 환경 변수 설정 (Baseline) ---
# AGENTS, TASKS, KILL_PERCENT는 루프 내에서 덮어쓰게 됩니다.
export HB_SEC="${HB_SEC:-5}"
export TTL_SEC="${TTL_SEC:-20}"
export RUNTIME_BEFORE_KILL="${RUNTIME_BEFORE_KILL:-60}"
export POST_KILL_OBSERVE="${POST_KILL_OBSERVE:-300}"
export MC_DISABLE_AUTH="${MC_DISABLE_AUTH:-1}"

# --- 로그 디렉터리 생성 ---
LOG_DIR="experiment_logs"
mkdir -p "${LOG_DIR}"

echo
echo "#######  시작: 실험 2 (스트레스 테스트) #######"
echo "(AGENTS=100, TASKS=1000 고정, KILL_PERCENT 변경)"
echo "--------------------------------------------------------"

KILL_LIST="10 20 40 60 80 90"

for K_PERCENT in $KILL_LIST; do
    LOG_FILE="${LOG_DIR}/exp3_kill_${K_PERCENT}.log"
    
    echo
    echo "==>  실행 중 (AGENTS=100, KILL_PERCENT=${K_PERCENT}%)..."
    echo "    (로그: ${LOG_FILE})"

    ( set -x;
      AGENTS=100 TASKS=1000 KILL_PERCENT=${K_PERCENT} \
      ./run_fault_tolerance_benchmark.sh
    ) > "${LOG_FILE}" 2>&1

    echo "   완료. 결과 요약:"
    grep -E -- "- total tasks:|- completed:|- success_rate:|- MTTR p50:|- MTTR p95:|- false recall rate:" "${LOG_FILE}" | sed 's/^/    /'

    sleep 5
done

echo
echo "####### 모든 실험 완료 #######"
echo "상세 로그는 '${LOG_DIR}' 디렉터리에서 확인하세요."
echo "그래프용 원본 데이터는 'bench_artifacts/' 내의 각 타임스탬프 폴더에서 찾을 수 있습니다."
