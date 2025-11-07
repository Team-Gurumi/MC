#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# 내결함성 벤치마크 - 자동화 스크립트
#
# 1. 실험 1: 확장성 테스트 (N vs MTTR)
#    - AGENTS 50, 100, 200, 500명으로 늘려가며 테스트
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
export POST_KILL_OBSERVE="${POST_KILL_OBSERVE:-200}"
export MC_DISABLE_AUTH="${MC_DISABLE_AUTH:-1}"

# --- 로그 디렉터리 생성 ---
LOG_DIR="experiment_logs"
mkdir -p "${LOG_DIR}"

echo "####### 1. 시작: 실험 1 (확장성 테스트) #######"
echo "(AGENTS와 TASKS 변경, KILL_PERCENT=10% 고정)"
echo "---------------------------------------------------------"

AGENT_LIST="50 100 200 500"

for N_AGENTS in $AGENT_LIST; do
    # 에이전트 1명당 10개의 작업 할당 (100/1000 비율 유지)
    N_TASKS=$((N_AGENTS * 10))
    LOG_FILE="${LOG_DIR}/exp2_agents_${N_AGENTS}.log"
    
    echo
    echo "==> 실행 중 (AGENTS=${N_AGENTS}, TASKS=${N_TASKS})..."
    echo "    (로그: ${LOG_FILE})"
    
    # 벤치마크 실행 및 로그 파일로 리디렉션
    ( set -x; # 실행되는 명령어를 터미널에 보여줌
      AGENTS=${N_AGENTS} TASKS=${N_TASKS} KILL_PERCENT=10 \
      ./run_fault_tolerance_benchmark.sh
    ) > "${LOG_FILE}" 2>&1
    
    echo "  완료. 결과 요약:"
    # 로그 파일에서 최종 리포트 부분만 grep으로 추출하여 출력
    grep -E -- "- total tasks:|- completed:|- success_rate:|- MTTR p50:|- MTTR p95:|- duplicate execution rate:|- false recall rate:" "${LOG_FILE}" | sed 's/^/    /'
    
    sleep 5 # 다음 실행 전 잠시 대기 (리소스 정리 시간)
done


echo
echo "####### 모든 실험 완료 #######"
echo "상세 로그는 '${LOG_DIR}' 디렉터리에서 확인하세요."
echo "그래프용 원본 데이터는 'bench_artifacts/' 내의 각 타임스탬프 폴더에서 찾을 수 있습니다."
