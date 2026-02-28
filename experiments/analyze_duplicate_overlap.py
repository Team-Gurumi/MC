#!/usr/bin/env python3
import sys, os, json, re, glob
from datetime import datetime

# Duplicate Execution Rate Analyzer
#
# Logic:
# 1. Parse logs to find 'execution_started' events.
# 2. Group by job_id.
# 3. For each job, check if any two executions (from different agents) overlap in their lease time.
#    Overlap condition:
#      A.lease_acquired_ts < B.lease_expire_ts AND B.lease_acquired_ts < A.lease_expire_ts
# 4. Count jobs with at least one overlap as 'duplicate'.
# 5. Rate = duplicate jobs / total unique jobs (that had at least one execution start).

def parse_timestamp(ts_str):
    if not ts_str:
        return None
    # RFC3339Nano format handling
    if ts_str.endswith('Z'):
        ts_str = ts_str[:-1] + "+00:00"
    
    try:
        # Truncate fractional seconds to 6 digits if longer, for python < 3.11 compatibility
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
                elif 'Z' in sec_part: # Should be handled by endswith('Z') check above, but for safety
                     sec_part = sec_part.replace('Z', '')
                     tz_part = '+00:00'

                if len(sec_part) > 6:
                    sec_part = sec_part[:6]
                
                ts_str = parts[0] + '.' + sec_part + tz_part
        
        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        return None

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

def main():
    if len(sys.argv) < 2:
        print("usage: analyze_duplicate_overlap.py <ARTIFACT_DIR>", file=sys.stderr)
        sys.exit(2)
    
    root = sys.argv[1]
    logdir = os.path.join(root, "logs")
    
    logs = sorted(glob.glob(os.path.join(logdir, "agent-*.log")))
    
    # Data structure:
    # executions[job_id] = [ {agent, start, end}, ... ]
    executions = {}

    print(f"Analyzing logs in {logdir}...")

    for p in logs:
        for obj in parse_json_lines(p):
            if obj.get("event") == "execution_started":
                jid = obj.get("job_id")
                aid = obj.get("agent_id")
                acq_str = obj.get("lease_acquired_ts")
                exp_str = obj.get("lease_expire_ts")

                if not (jid and aid and acq_str and exp_str):
                    continue

                acq = parse_timestamp(acq_str)
                exp = parse_timestamp(exp_str)

                if acq and exp:
                    if jid not in executions:
                        executions[jid] = []
                    executions[jid].append({
                        "agent": aid,
                        "start": acq,
                        "end": exp
                    })

    duplicate_jobs = 0
    total_jobs = len(executions)

    for jid, execs in executions.items():
        # Check for overlap
        found_overlap = False
        n = len(execs)
        if n < 2:
            continue
        
        # Sort by start time
        execs.sort(key=lambda x: x["start"])

        for i in range(n):
            for j in range(i+1, n):
                A = execs[i]
                B = execs[j]

                # Different agents only
                if A["agent"] == B["agent"]:
                    continue

                # Overlap condition: A.start < B.end AND B.start < A.end
                if A["start"] < B["end"] and B["start"] < A["end"]:
                    found_overlap = True
                    break
            if found_overlap:
                break
        
        if found_overlap:
            duplicate_jobs += 1

    rate = (duplicate_jobs / total_jobs * 100.0) if total_jobs > 0 else 0.0

    print(f"Total Jobs (with execution): {total_jobs}")
    print(f"Duplicate Jobs (overlap): {duplicate_jobs}")
    print(f"Duplicate Execution Rate: {rate:.2f}%")

    out_file = os.path.join(root, "results", "duplicate_report.json")
    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    report = {
        "total_jobs_executed": total_jobs,
        "duplicate_jobs": duplicate_jobs,
        "duplicate_execution_rate_percent": rate
    }
    with open(out_file, "w") as f:
        json.dump(report, f, indent=2)

if __name__ == "__main__":
    main()
