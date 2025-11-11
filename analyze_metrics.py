#!/usr/bin/env python3
import sys, os, json, re, glob, subprocess, statistics
from datetime import datetime, timezone
from collections import defaultdict

# analyze_metrics.py <ARTIFACT_DIR>
# - tasks.jsonl에서 job id를 읽고, CONTROL_URL/api/tasks/<id>를 조회.
# - status를 방어적으로 추출하여 성공률과 상태 분포를 집계.
# - 로그를 기반으로 MTTR, 중복 실행률, 오탐지율을 계산.

EVENTS = {"lease_acquired", "heartbeat", "lease_expired", "reassigned", "running", "completed"}

def read_control_url(art):
    cfg = os.path.join(art, "config.txt")
    if not os.path.exists(cfg): return None
    with open(cfg) as f:
        for line in f:
            if line.startswith("CONTROL_URL="):
                return line.strip().split("=", 1)[1]
    return None

def curl_json(url, auth_token=None, timeout=5):
    headers = []
    if auth_token:
        headers += ["-H", f"Authorization: Bearer {auth_token}"]
    try:
        out = subprocess.check_output(["curl", "-sS", "--max-time", str(timeout), *headers, url])
        txt = out.decode("utf-8", "ignore").strip()
        if not txt:
            return None
        return json.loads(txt)
    except Exception:
        return None

def extract_status(obj):
   
    if obj is None: return "unknown"
    for k in ("status", "state", "phase"):
        if k in obj:
            v = obj[k]
            if isinstance(v, str): return v
            if isinstance(v, bool): return str(v).lower()
            if isinstance(v, (int, float)): return str(v)
            if isinstance(v, dict):
                for kk in ("status", "state", "phase", "value", "name"):
                    vv = v.get(kk)
                    if isinstance(vv, str): return vv
                try:
                    return v.get("status") or v.get("state") or v.get("phase") or json.dumps(v, ensure_ascii=False)
                except Exception:
                    return "unknown"
    for nest in ("task", "data", "result", "meta"):
        if nest in obj and isinstance(obj[nest], dict):
            s = extract_status(obj[nest])
            if s and s != "unknown":
                return s
    for k in ("done", "completed", "success"):
        if k in obj:
            v = obj[k]
            if v is True: return "completed"
            if v is False: return "running"
    return "unknown"

def parse_json_lines(path):
    out = []
    if not os.path.exists(path): return out
    with open(path, 'r', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                out.append(json.loads(line)); continue
            except Exception:
                m = re.search(r'(\{.*\})', line)
                if m:
                    try:
                        out.append(json.loads(m.group(1)))
                    except Exception:
                        pass
    return out

def parse_timestamp(ts_str):
    if not ts_str:
        return None
    # RFC3339Nano 형식 (e.g., "2023-10-27T10:00:00.123456789Z") 처리
    if ts_str.endswith('Z'):
        ts_str = ts_str[:-1] + "+00:00"
    
    try:
        # 소수점 이하 7자리 이상일 경우 6자리로 자름 (파이썬 3.10 이하 호환)
        if '.' in ts_str:
            parts = ts_str.split('.')
            if len(parts) == 2:
                sec_part = parts[1]
                tz_part = ""
                if '+' in sec_part:
                    sec_parts = sec_part.split('+')
                    sec_part = sec_parts[0]
                    tz_part = '+' + sec_parts[1]
                elif '-' in sec_part and 'e-' not in sec_part:
                    sec_parts = sec_part.split('-')
                    sec_part = sec_parts[0]
                    tz_part = '-' + sec_parts[1]
                
                if len(sec_part) > 6:
                    sec_part = sec_part[:6]
                
                ts_str = parts[0] + '.' + sec_part + tz_part

        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return None

def main():
    if len(sys.argv) < 2:
        print("usage: analyze_metrics.py <ARTIFACT_DIR>", file=sys.stderr);
        sys.exit(2)
    root = sys.argv[1]
    logdir = os.path.join(root, "logs")
    resdir = os.path.join(root, "results")
    os.makedirs(resdir, exist_ok=True)

    control_url = read_control_url(root)
    token = os.environ.get("CONTROL_TOKEN", "dev")

    # 제출된 작업 ID 수집
    tasks_file = os.path.join(root, "tasks.jsonl")
    job_ids = []
    for obj in parse_json_lines(tasks_file):
        jid = obj.get("job_id") or obj.get("id")
        if jid: job_ids.append(jid)

    # API를 통해 상태 조회 및 집계
    status_counts = {}
    completed = set()
    for jid in job_ids:
        obj = curl_json(f"{control_url}/api/tasks/{jid}", auth_token=token, timeout=5)
        st = extract_status(obj)
        status_counts[st] = status_counts.get(st, 0) + 1
        if st in ("completed", "success", "done", "succeeded", "finished"):
            completed.add(jid)

    total = len(job_ids)
    success_rate = (len(completed) / total * 100.0) if total > 0 else 0

   #로그에서 이벤트를 파싱하여 추가 지표 계산
    def try_logs_for_extras():
        try:
            all_events = []
            logs = [os.path.join(logdir, "control.log")] + sorted(glob.glob(os.path.join(logdir, "agent-*.log")))
            for p in logs:
                for obj in parse_json_lines(p):
                    ev = obj.get("event")
                    if ev in EVENTS:
                        ts_str = obj.get("timestamp") or obj.get("ts") or obj.get("time")
                        ts = parse_timestamp(ts_str)
                        jid = obj.get("job_id") or obj.get("task_id") or obj.get("id")
                        aid = obj.get("agent_id") or obj.get("peer_id") or obj.get("agent")
                        
                        if jid and ts:
                            event_data = {"ev": ev, "ts": ts, "job": jid}
                            if aid:
                                event_data["agent"] = aid
                            all_events.append(event_data)
            
            # 시간순으로 이벤트 정렬
            all_events.sort(key=lambda x: x["ts"])

            # --- 지표 계산 로직 ---
            job_failure_times = {}
            recovery_times = []
            recalled_jobs = {}
            agent_completions = {}
            
            # *** 수정된 부분 (중복 실행률 로직 변경) ***
            job_active_agent = {} # K: jid, V: aid (현재 이 작업을 실행 중인 에이전트)
            true_duplicate_jobs = set() # K: jid (진짜 중복 실행이 발생한 작업 ID)
            # *** (기존 job_executions = defaultdict(set) 제거) ***

            for event in all_events:
                jid = event["job"]
                aid = event.get("agent") # .get()을 사용하여 agent_id가 없는 경우를 안전하게 처리
                ev = event["ev"]
                ts = event["ts"]

                # MTTR 계산용
                if ev in ("lease_expired", "reassigned"):
                    if jid not in job_failure_times:
                        job_failure_times[jid] = ts
                    
                    # 오탐지율 계산용 (aid가 있는 경우에만 기록)
                    if ev == 'reassigned' and jid not in recalled_jobs and aid:
                        recalled_jobs[jid] = {"agent": aid, "time": ts}
                    
                    # *** 추가된 부분: 작업이 실패/회수되면 활성 상태 해제 ***
                    # (aid가 명시된 경우) 현재 활성 에이전트와 일치할 때만 해제
                    # (aid가 없거나 reassigned인 경우) 컨트롤러가 회수한 것이므로 강제 해제
                    current_agent = job_active_agent.get(jid)
                    if current_agent and aid and current_agent == aid:
                         del job_active_agent[jid]
                    elif ev == 'reassigned' and jid in job_active_agent:
                         # aid가 없어도 reassigned는 활성 에이전트를 해제해야 함
                         del job_active_agent[jid] 
                
                elif ev in ("lease_acquired", "running"):
                    if jid in job_failure_times:
                        failure_ts = job_failure_times.pop(jid)
                        recovery_duration = (ts - failure_ts).total_seconds()
                        if recovery_duration >= 0:
                            recovery_times.append(recovery_duration)
                    
                    # *** 수정된 부분: 중복 실행률 계산 로직 변경 ***
                    if aid:
                        current_agent = job_active_agent.get(jid)
                        if current_agent is not None and current_agent != aid:
                            # 이미 다른 에이전트가 활성 상태인데, 새 에이전트가 작업을 시작함
                            # 이것이 "진짜" 중복 실행임
                            true_duplicate_jobs.add(jid)
                        
                        # 현재 에이전트를 활성 상태로 등록
                        job_active_agent[jid] = aid
                
                elif ev == "completed":
                    # 오탐지율 계산용 (aid가 있는 경우에만 기록)
                    if aid:
                        agent_completions[jid] = aid
                    
                    # *** 추가된 부분: 작업이 완료되면 활성 상태 해제 ***
                    current_agent = job_active_agent.get(jid)
                    if aid and current_agent and current_agent == aid:
                        del job_active_agent[jid]
                    elif not aid and jid in job_active_agent:
                         # aid 없이 완료 이벤트가 뜰 경우 (드물지만), 일단 해제
                         del job_active_agent[jid]

            # MTTR
            mttr_p50 = statistics.median(recovery_times) if recovery_times else None
            mttr_p95 = None
            if recovery_times:
                recovery_times.sort()
                p95_index = int(len(recovery_times) * 0.95)
                mttr_p95 = recovery_times[p95_index] if p95_index < len(recovery_times) else recovery_times[-1]

           
            dup_exec_count = len(true_duplicate_jobs) # (기존 로직 대신 true_duplicate_jobs 세트의 크기를 사용)
            dup_exec_rate = (dup_exec_count / total * 100.0) if total > 0 else 0

            # 오탐지율 (기존 로직 유지)
            false_recall_count = 0
            for jid, recall_info in recalled_jobs.items():
                if jid in agent_completions and agent_completions[jid] == recall_info["agent"]:
                    false_recall_count += 1
            false_recall_rate = (false_recall_count / len(recalled_jobs) * 100.0) if recalled_jobs else 0

            return {
                "mttr_p50_s": mttr_p50,
                "mttr_p95_s": mttr_p95,
                "dup_exec_rate_percent": dup_exec_rate,
                "false_recall_rate_percent": false_recall_rate,
            }
        except Exception as e:
            print(f"[WARN] Failed to analyze logs for extra metrics: {e}", file=sys.stderr)
            return { "mttr_p50_s": None, "mttr_p95_s": None, "dup_exec_rate_percent": None, "false_recall_rate_percent": None }
    extras = try_logs_for_extras()

    result = {
        "total_tasks": total,
        "completed": len(completed),
        "success_rate_percent": success_rate,
        "status_breakdown": status_counts,
        **extras,
    }

    with open(os.path.join(resdir, "report.json"), "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    def fmt(v, unit=""):
        if v is None: return "-"
        if isinstance(v, float): return f"{v:.4f}{unit}"
        return f"{v}{unit}"

    print("# Benchmark Report (API-based)\n")
    print(f"- total tasks: {result['total_tasks']}")
    print(f"- completed: {result['completed']}")
    print(f"- success_rate: {fmt(result.get('success_rate_percent'), '%')}")
    if result["status_breakdown"]:
        print("- status breakdown:")
        for k in sorted(result["status_breakdown"].keys()):
            print(f"  - {k}: {result['status_breakdown'][k]}")
            remain = sum(v for k,v in result["status_breakdown"].items() if k not in ("succeeded","completed","done"))
    if remain > 0:
        print(f"  ⚠️  remaining (not done): {remain}")

    print(f"- MTTR p50: {fmt(result.get('mttr_p50_s'), 's')}  (target ≤ 120s)")
    print(f"- MTTR p95: {fmt(result.get('mttr_p95_s'), 's')}  (target ≤ 180s)")
    print(f"- duplicate execution rate: {fmt(result.get('dup_exec_rate_percent'), '%')}  (target ≤ 1%)")
    print(f"- false recall rate: {fmt(result.get('false_recall_rate_percent'), '%')}  (target ≤ 0.5%)")


if __name__ == "__main__":
    main()
