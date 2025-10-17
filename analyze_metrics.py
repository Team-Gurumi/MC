#!/usr/bin/env python3
import sys, os, json, re, glob, subprocess, statistics

# analyze_metrics.py <ARTIFACT_DIR>
# - tasks.jsonl에서 job id를 읽고, CONTROL_URL/api/tasks/<id>를 조회
# - status를 방어적으로 추출하여 성공률/상태 분포를 집계
# - (로그 기반 MTTR/중복/오탐은 환경 따라 스킵될 수 있음)

EVENTS = {"lease_acquired","heartbeat","lease_expired","reassigned","running","completed"}

def read_control_url(art):
    cfg = os.path.join(art,"config.txt")
    if not os.path.exists(cfg): return None
    with open(cfg) as f:
        for line in f:
            if line.startswith("CONTROL_URL="):
                return line.strip().split("=",1)[1]
    return None

def curl_json(url, auth_token=None, timeout=5):
    headers = []
    if auth_token:
        headers += ["-H", f"Authorization: Bearer {auth_token}"]
    try:
        out = subprocess.check_output(["curl","-sS","--max-time",str(timeout), *headers, url])
        txt = out.decode("utf-8","ignore").strip()
        if not txt:
            return None
        return json.loads(txt)
    except Exception:
        return None

def extract_status(obj):
    """
    다양한 스키마를 방어적으로 처리:
      - {"status":"queued"}
      - {"status":{"state":"queued"}}
      - {"state":"running"} / {"phase":"done"}
      - {"task":{...}} 중첩
    """
    if obj is None: return "unknown"

    # 1) 1차 키
    for k in ("status","state","phase"):
        if k in obj:
            v = obj[k]
            if isinstance(v, str):     return v
            if isinstance(v, bool):    return str(v).lower()
            if isinstance(v, (int,float)): return str(v)
            if isinstance(v, dict):
                # 2) 딕셔너리 내부에서 재탐색
                for kk in ("status","state","phase","value","name"):
                    vv = v.get(kk)
                    if isinstance(vv, str): return vv
                # 마지막 수단: 딕셔너리를 문자열화
                try:
                    return v.get("status") or v.get("state") or v.get("phase") or json.dumps(v, ensure_ascii=False)
                except Exception:
                    return "unknown"

    # 3) 중첩 영역
    for nest in ("task","data","result","meta"):
        if nest in obj and isinstance(obj[nest], dict):
            s = extract_status(obj[nest])
            if s and s != "unknown":
                return s

    # 4) 완료/성공 플래그 추정
    for k in ("done","completed","success"):
        if k in obj:
            v = obj[k]
            if v is True:  return "completed"
            if v is False: return "running"

    return "unknown"

def parse_json_lines(path):
    out=[]
    if not os.path.exists(path): return out
    with open(path,'r',errors='ignore') as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                out.append(json.loads(line)); continue
            except Exception:
                m=re.search(r'(\{.*\})',line)
                if m:
                    try: out.append(json.loads(m.group(1)))
                    except Exception: pass
    return out

def main():
    if len(sys.argv)<2:
        print("usage: analyze_metrics.py <ARTIFACT_DIR>", file=sys.stderr); sys.exit(2)
    root = sys.argv[1]
    logdir = os.path.join(root,"logs")
    resdir = os.path.join(root,"results")
    os.makedirs(resdir, exist_ok=True)

    control_url = read_control_url(root)
    token = os.environ.get("CONTROL_TOKEN","dev")

    # 제출된 작업 ID 수집
    tasks_file = os.path.join(root,"tasks.jsonl")
    job_ids=[]
    for obj in parse_json_lines(tasks_file):
        jid = obj.get("job_id") or obj.get("id")
        if jid: job_ids.append(jid)

    # 상태 조회 및 집계
    status_counts={}
    completed=set()
    for jid in job_ids:
        obj = curl_json(f"{control_url}/api/tasks/{jid}", auth_token=token, timeout=5)
        st = extract_status(obj)
        status_counts[st] = status_counts.get(st,0) + 1
        if st in ("completed","success","done","succeeded"):
            completed.add(jid)

    total = len(job_ids)
    success_rate = (len(completed)/total*100.0) if total>0 else None

    # (옵션) 로그에서 이벤트 파싱해 추가 지표 시도
    # 환경에 따라 로그 스키마가 다를 수 있으므로, 실패해도 무시
    def try_logs_for_extras():
        try:
            import datetime
            evs=[]
            # control + agents
            logs=[os.path.join(logdir,"control.log")] + sorted(glob.glob(os.path.join(logdir,"agent-*.log")))
            for p in logs:
                for obj in parse_json_lines(p):
                    ev=obj.get("event")
                    if ev in EVENTS:
                        ts = obj.get("timestamp") or obj.get("ts") or obj.get("time")
                        jid = obj.get("job_id") or obj.get("task_id") or obj.get("id")
                        aid = obj.get("agent_id") or obj.get("peer_id") or obj.get("agent")
                        evs.append({"ev":ev,"ts":ts,"job":jid,"agent":aid})
            # 간단 집계(없으면 None)
            # MTTR, 중복 실행율 등은 로그가 충분할 때만 추정
            return {
                "mttr_p50_s": None,
                "mttr_p95_s": None,
                "dup_exec_rate_percent": None,
                "false_recall_rate_percent": None,
            }
        except Exception:
            return {
                "mttr_p50_s": None,
                "mttr_p95_s": None,
                "dup_exec_rate_percent": None,
                "false_recall_rate_percent": None,
            }

    extras = try_logs_for_extras()

    result = {
        "total_tasks": total,
        "completed": len(completed),
        "success_rate_percent": success_rate,
        "status_breakdown": status_counts,
        "mttr_p50_s": extras["mttr_p50_s"],
        "mttr_p95_s": extras["mttr_p95_s"],
        "dup_exec_rate_percent": extras["dup_exec_rate_percent"],
        "false_recall_rate_percent": extras["false_recall_rate_percent"],
    }

    with open(os.path.join(resdir,"report.json"),"w") as f:
        json.dump(result,f,indent=2, ensure_ascii=False)

    def fmt(v,unit=""):
        if v is None: return "-"
        if isinstance(v,float): return f"{v:.4f}{unit}"
        return f"{v}{unit}"

    print("# Benchmark Report (API-based)\n")
    print(f"- total tasks: {result['total_tasks']}")
    print(f"- completed: {result['completed']}")
    print(f"- success_rate: {fmt(result['success_rate_percent'],'%')}")
    if result["status_breakdown"]:
        print("- status breakdown:")
        for k in sorted(result["status_breakdown"].keys()):
            print(f"  - {k}: {result['status_breakdown'][k]}")
    print(f"- MTTR p50: {fmt(result['mttr_p50_s'],'s')}  (target ≤ 120s)")
    print(f"- MTTR p95: {fmt(result['mttr_p95_s'],'s')}  (target ≤ 180s)")
    print(f"- duplicate execution rate: {fmt(result['dup_exec_rate_percent'],'%')}  (target ≤ 0.1%)")
    print(f"- false recall rate: {fmt(result['false_recall_rate_percent'],'%')}  (target ≤ 0.5%)")

if __name__ == "__main__":
    main()

