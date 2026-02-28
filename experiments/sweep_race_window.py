#!/usr/bin/env python3
"""
Race Window Sweep Experiment
- Race windows: 10, 50, 100, 200, 500 ms
- 5 repetitions per window → 25 total runs
- Reports mean ± std duplicate execution rate per window
"""

import os, json, random, math, statistics
from datetime import datetime, timezone, timedelta
from itertools import combinations

# ── Fixed Parameters ──────────────────────────────────────────────────────────
AGENTS       = 100
TASKS        = 200
TTL_SEC      = 10
KILL_PERCENT = 10
P_RACE_ON_NEW_JOB  = 0.04
P_RACE_ON_REASSIGN = 0.12
REPEATS      = 5
RACE_WINDOWS_MS = [10, 50, 100, 200, 500]

# ── Core Simulation ───────────────────────────────────────────────────────────
def run_simulation(race_window_sec: float, seed: int) -> dict:
    rng = random.Random(seed)
    BASE_TIME = datetime(2026, 2, 19, 5, 0, 0, tzinfo=timezone.utc)

    def ts(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00"

    def parse_ts(s):
        return datetime.fromisoformat(s)

    alive = list(range(1, AGENTS + 1))
    kill_count = math.ceil(AGENTS * KILL_PERCENT / 100)
    killed = set(rng.sample(alive, kill_count))
    surviving = [a for a in alive if a not in killed]

    # job_id -> list of {agent, acq, exp}
    job_events: dict[str, list[dict]] = {}

    for j in range(1, TASKS + 1):
        jid = f"job-{j:04d}"
        job_events[jid] = []

        # Primary claim
        pa = rng.choice(alive)
        t_claim = rng.uniform(0.5, 8.0)
        acq = BASE_TIME + timedelta(seconds=t_claim)
        exp = acq + timedelta(seconds=TTL_SEC)
        job_events[jid].append({"agent": pa, "acq": acq, "exp": exp, "killed": pa in killed})

        # Race on fresh job
        if rng.random() < P_RACE_ON_NEW_JOB:
            candidates = [a for a in alive if a != pa]
            if candidates:
                ra = rng.choice(candidates)
                offset = rng.uniform(0.0, race_window_sec)
                acq2 = acq + timedelta(seconds=offset)
                exp2 = acq2 + timedelta(seconds=TTL_SEC)
                job_events[jid].append({"agent": ra, "acq": acq2, "exp": exp2, "killed": ra in killed})

    # Reassignment after kill
    for jid, attempts in job_events.items():
        if not attempts or not attempts[0]["killed"]:
            continue
        orig_exp = attempts[0]["exp"]
        t_r = rng.uniform(0.5, 3.0)
        acq_r = orig_exp + timedelta(seconds=t_r)
        exp_r = acq_r + timedelta(seconds=TTL_SEC)
        if surviving:
            na = rng.choice(surviving)
            attempts.append({"agent": na, "acq": acq_r, "exp": exp_r, "killed": False})
            # Race on reassignment
            if rng.random() < P_RACE_ON_REASSIGN:
                cands = [a for a in surviving if a != na]
                if cands:
                    ra2 = rng.choice(cands)
                    offset2 = rng.uniform(0.0, race_window_sec)
                    acq_r2 = acq_r + timedelta(seconds=offset2)
                    exp_r2 = acq_r2 + timedelta(seconds=TTL_SEC)
                    attempts.append({"agent": ra2, "acq": acq_r2, "exp": exp_r2, "killed": False})

    # ── Overlap Analysis (same logic as analyze_duplicate_overlap.py) ──────────
    total_jobs = len(job_events)
    duplicate_jobs = 0
    dup_job_ids = []

    for jid, attempts in job_events.items():
        if len(attempts) < 2:
            continue
        found = False
        for a, b in combinations(attempts, 2):
            if a["agent"] == b["agent"]:
                continue
            # Overlap: A.acq < B.exp AND B.acq < A.exp
            if a["acq"] < b["exp"] and b["acq"] < a["exp"]:
                found = True
                break
        if found:
            duplicate_jobs += 1
            dup_job_ids.append(jid)

    rate = duplicate_jobs / total_jobs * 100.0 if total_jobs > 0 else 0.0

    return {
        "total_jobs": total_jobs,
        "duplicate_jobs": duplicate_jobs,
        "rate_pct": rate,
        "total_attempts": sum(len(v) for v in job_events.values()),
        "multi_attempt_jobs": sum(1 for v in job_events.values() if len(v) > 1),
    }

# ── Run All 25 Experiments ────────────────────────────────────────────────────
print("=" * 70)
print(f"Duplicate Execution Rate - Race Window Sweep")
print(f"Agents={AGENTS}  Tasks={TASKS}  TTL={TTL_SEC}s  Kill={KILL_PERCENT}%  Repeats={REPEATS}")
print("=" * 70)

all_results = {}
seed_base = 1000

for rw_ms in RACE_WINDOWS_MS:
    rw_sec = rw_ms / 1000.0
    runs = []
    for rep in range(REPEATS):
        seed = seed_base + rw_ms * 10 + rep
        r = run_simulation(rw_sec, seed)
        runs.append(r)
        print(f"  window={rw_ms:4d}ms  rep={rep+1}  dup={r['duplicate_jobs']:3d}/{r['total_jobs']}  rate={r['rate_pct']:.2f}%")

    rates = [r["rate_pct"] for r in runs]
    dups  = [r["duplicate_jobs"] for r in runs]
    all_results[rw_ms] = {
        "runs": runs,
        "rates": rates,
        "mean_rate": statistics.mean(rates),
        "std_rate":  statistics.stdev(rates) if len(rates) > 1 else 0.0,
        "min_rate":  min(rates),
        "max_rate":  max(rates),
        "mean_dups": statistics.mean(dups),
    }
    print()

# ── Summary Table ─────────────────────────────────────────────────────────────
print("=" * 70)
print(f"{'Race Window':>14} | {'Mean Rate':>10} | {'Std':>7} | {'Min':>7} | {'Max':>7} | {'Mean Dups':>10}")
print("-" * 70)
for rw_ms, res in all_results.items():
    print(f"{rw_ms:>12}ms | {res['mean_rate']:>9.2f}% | {res['std_rate']:>6.2f}% | {res['min_rate']:>6.2f}% | {res['max_rate']:>6.2f}% | {res['mean_dups']:>10.1f}")
print("=" * 70)

# ── Save JSON Report ──────────────────────────────────────────────────────────
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "bench_artifacts", "sweep_results")
os.makedirs(OUT_DIR, exist_ok=True)
report_path = os.path.join(OUT_DIR, "sweep_report.json")

report = {
    "config": {
        "agents": AGENTS, "tasks": TASKS, "ttl_sec": TTL_SEC,
        "kill_percent": KILL_PERCENT, "repeats": REPEATS,
        "p_race_new": P_RACE_ON_NEW_JOB, "p_race_reassign": P_RACE_ON_REASSIGN,
    },
    "results": {
        str(rw_ms): {
            "race_window_ms": rw_ms,
            "mean_rate_pct":  res["mean_rate"],
            "std_rate_pct":   res["std_rate"],
            "min_rate_pct":   res["min_rate"],
            "max_rate_pct":   res["max_rate"],
            "mean_duplicate_jobs": res["mean_dups"],
            "per_run": [
                {"rep": i+1, "duplicate_jobs": r["duplicate_jobs"],
                 "rate_pct": r["rate_pct"], "total_attempts": r["total_attempts"]}
                for i, r in enumerate(res["runs"])
            ],
        }
        for rw_ms, res in all_results.items()
    }
}

with open(report_path, "w") as f:
    json.dump(report, f, indent=2)

print(f"\nReport saved: {report_path}")
