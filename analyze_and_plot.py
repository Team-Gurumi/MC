import json
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone

LOG_DIR = "./logs"
N_TASKS = 1000
HEARTBEAT_INTERVAL = 5  # 초
LEASE_TTL = 15          # 초

def parse_log_file(file_path):
    """단일 로그 파일에서 JSON 라인을 파싱합니다."""
    records = []
    with open(file_path, 'r') as f:
        for line in f:
            try:
                # Go의 표준 로그 포맷 (날짜 시간 {json})을 처리
                if '{' in line:
                    json_str = line[line.find('{'):]
                    records.append(json.loads(json_str))
            except json.JSONDecodeError:
                continue # JSON이 아닌 로그 라인은 무시
    return records

def analyze_logs():
    """로그를 분석하여 지표를 계산하고 결과를 출력합니다."""
    all_logs = []
    for filename in os.listdir(LOG_DIR):
        if filename.startswith("agent_") and filename.endswith(".log"):
            all_logs.extend(parse_log_file(os.path.join(LOG_DIR, filename)))

    if not all_logs:
        print("분석할 로그 데이터가 없습니다.")
        return

    df = pd.DataFrame(all_logs)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values(by='timestamp').reset_index(drop=True)

    # 1. 완료 성공률 계산
    completed = df[df['event'] == 'task_completed']
    succeeded = completed[completed['status'] == 'succeeded']
    success_rate = len(succeeded) / N_TASKS if N_TASKS > 0 else 0

    # 2. MTTR 계산
    killed_agents = df[df['event'] == 'agent_killed']
    
    # 이 실험에서는 SIGKILL을 사용하므로, 실제 'lease_expired' 로그는 없습니다.
    # 대신, 마지막 하트비트와 TTL을 기반으로 만료 시각을 추정합니다.
    mttr_values = []
    
    # 종료된 에이전트가 소유했던 작업을 찾습니다.
    tasks_of_killed_agents = df[df['agent_id'].isin(killed_agents['agent_id'].unique()) & df['task_id'].notna()]
    
    for task_id, group in tasks_of_killed_agents.groupby('task_id'):
        last_heartbeat = group[group['event'] == 'lease_heartbeat_sent'].tail(1)
        
        # 재시작(다시 running 상태가 된) 이벤트를 찾습니다.
        restarts = df[(df['task_id'] == task_id) & (df['event'] == 'task_running') & 
                      ~df['agent_id'].isin(killed_agents['agent_id'].unique())]

        if not last_heartbeat.empty and not restarts.empty:
            t_last_hb = last_heartbeat['timestamp'].iloc[0]
            t_expire = t_last_hb + timedelta(seconds=LEASE_TTL)
            
            # 만료 이후의 첫 재시작을 찾습니다.
            first_restart = restarts[restarts['timestamp'] > t_expire].head(1)
            if not first_restart.empty:
                t_restart = first_restart['timestamp'].iloc[0]
                mttr = (t_restart - t_expire).total_seconds()
                mttr_values.append(mttr)


    # 3. 중복 실행률 계산
    running_events = df[df['event'] == 'task_running'].copy()
    running_events = running_events.drop_duplicates(subset=['task_id', 'agent_id']) # 동일 에이전트의 중복 실행 로그 제거
    
    duplicate_tasks = 0
    for task_id, group in running_events.groupby('task_id'):
        if len(group) > 1:
            # 시간순으로 정렬
            group = group.sort_values(by='timestamp')
            timestamps = group['timestamp'].to_list()
            for i in range(len(timestamps) - 1):
                # TTL/2 (7.5초) 이내에 다른 에이전트가 실행하면 중복으로 간주
                if (timestamps[i+1] - timestamps[i]).total_seconds() <= LEASE_TTL / 2:
                    duplicate_tasks += 1
                    break # 한 작업당 한 번만 카운트

    duplicate_rate = duplicate_tasks / N_TASKS if N_TASKS > 0 else 0
    
    # 4. 오탐 회수율 (이 시나리오에서는 0이 예상됨)
    # "lease_acquired" 이벤트 중, 이전 소유자가 있었던 경우를 찾습니다.
    lease_acquired = df[df.event == 'lease_acquired'].sort_values('timestamp')
    reassigned_tasks = lease_acquired[lease_acquired.duplicated('task_id', keep='first')]
    
    false_recoveries = 0
    for _, row in reassigned_tasks.iterrows():
        task_id = row['task_id']
        reassign_time = row['timestamp']
        
        # 이전 lease 정보를 찾습니다.
        prev_leases = lease_acquired[(lease_acquired['task_id'] == task_id) & (lease_acquired['timestamp'] < reassign_time)]
        if prev_leases.empty:
            continue
            
        last_owner_id = prev_leases.iloc[-1]['agent_id']
        last_heartbeats = df[(df['task_id'] == task_id) & (df['agent_id'] == last_owner_id) & (df['event'] == 'lease_heartbeat_sent')]
        
        if last_heartbeats.empty:
            continue

        t_last_hb = last_heartbeats.iloc[-1]['timestamp']
        t_expire_estimated = t_last_hb + timedelta(seconds=LEASE_TTL)

        # 재선점이 추정된 만료 시각 이전에 발생했다면 오탐일 수 있습니다.
        # 또한, 마지막 하트비트가 만료 직전(HB * 1.2 이내)에 있었다면 오탐 가능성이 높습니다.
        if reassign_time < t_expire_estimated and (t_expire_estimated - t_last_hb).total_seconds() < HEARTBEAT_INTERVAL * 1.2:
            false_recoveries += 1
            
    false_recovery_rate = false_recoveries / N_TASKS if N_TASKS > 0 else 0


    # --- 결과 출력 ---
    print("\n--- 실험 결과 분석 ---")
    print(f"✅ 완료 성공률: {success_rate:.4%} ({len(succeeded)}/{N_TASKS})")
    
    if mttr_values:
        median_mttr = np.median(mttr_values)
        p95_mttr = np.percentile(mttr_values, 95)
        print(f"⏱️ MTTR 중앙값: {median_mttr:.2f}s")
        print(f"⏱️ MTTR p95: {p95_mttr:.2f}s")
    else:
        print("⏱️ MTTR: 계산된 데이터 없음 (복구된 작업 없음)")

    print(f"👯 중복 실행률: {duplicate_rate:.4%} ({duplicate_tasks}/{N_TASKS})")
    print(f"🤔 오탐 회수율: {false_recovery_rate:.4%} ({false_recoveries}/{N_TASKS})")
    
    # --- 그래프 생성 ---
    if mttr_values:
        # 히스토그램
        plt.figure(figsize=(10, 6))
        plt.hist(mttr_values, bins=30, edgecolor='black', alpha=0.7)
        plt.axvline(median_mttr, color='red', linestyle='dashed', linewidth=2, label=f'Median: {median_mttr:.2f}s')
        plt.axvline(p95_mttr, color='orange', linestyle='dashed', linewidth=2, label=f'p95: {p95_mttr:.2f}s')
        plt.title('MTTR Distribution')
        plt.xlabel('Time to Recover (seconds)')
        plt.ylabel('Frequency')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.savefig('mttr_histogram.png')
        print("\n📈 MTTR 히스토그램이 'mttr_histogram.png'로 저장되었습니다.")

        # CDF
        plt.figure(figsize=(10, 6))
        sorted_mttr = np.sort(mttr_values)
        yvals = np.arange(len(sorted_mttr)) / float(len(sorted_mttr))
        plt.plot(sorted_mttr, yvals)
        plt.axvline(median_mttr, color='red', linestyle='dashed', linewidth=1, label=f'Median: {median_mttr:.2f}s')
        plt.axvline(p95_mttr, color='orange', linestyle='dashed', linewidth=1, label=f'p95: {p95_mttr:.2f}s')
        plt.title('MTTR Cumulative Distribution Function (CDF)')
        plt.xlabel('Time to Recover (seconds)')
        plt.ylabel('Cumulative Probability')
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.legend()
        plt.savefig('mttr_cdf.png')
        print("📈 MTTR CDF 그래프가 'mttr_cdf.png'로 저장되었습니다.")


if __name__ == '__main__':
    analyze_logs()
