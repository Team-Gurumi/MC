#!/usr/bin/env python3
"""
Post-Patch: Race Window Sweep Experiment
- Models the atomic TryClaim: UPDATE ... WHERE ... AND lease_expires_at < now()
- When two agents race, PostgreSQL row-level lock serializes them:
  only the first wins; the second re-evaluates WHERE and fails.
- All other parameters MATCH BASELINE exactly.

Race windows: 10, 50, 100, 200, 500 ms
5 repetitions per window -> 25 total runs
Identical seeds as baseline (seed_base = 1000)
"""

import os, json, random, math, statistics
from datetime import datetime, timezone, timedelta
from itertools import combinations

# ── Fixed Parameters (IDENTICAL TO BASELINE) ──────────────────────────────────
AGENTS       = 100
TASKS        = 200
TTL_SEC      = 10
KILL_PERCENT = 10
P_RACE_ON_NEW_JOB  = 0.04
P_RACE_ON_REASSIGN = 0.12
REPEATS      = 5
RACE_WINDOWS_MS = [10, 50, 100, 200, 500]
SEED_BASE    = 1000  # SAME as baseline

# ── Core Simulation (Post-Patch) ──────────────────────────────────────────────
def run_simulation(race_window_sec: float, seed: int) -> dict:
    rng = random.Random(seed)
    BASE_TIME = datetime(2026, 2, 19, 5, 0, 0, tzinfo=timezone.utc)

    alive = list(range(1, AGENTS + 1))
    kill_count = math.ceil(AGENTS * KILL_PERCENT / 100)
    killed = set(rng.sample(alive, kill_count))
    surviving = [a for a in alive if a not in killed]

    # job_id -> list of {agent, acq, exp}
    job_events: dict[str, list[dict]] = {}
    race_attempts = 0
    race_blocked  = 0   # blocked by atomic UPDATE

    for j in range(1, TASKS + 1):
        jid = f"job-{j:04d}"
        job_events[jid] = []

        # Primary claim (always succeeds — job is fresh/queued)
        pa = rng.choice(alive)
        t_claim = rng.uniform(0.5, 8.0)
        acq = BASE_TIME + timedelta(seconds=t_claim)
        exp = acq + timedelta(seconds=TTL_SEC)
        job_events[jid].append({"agent": pa, "acq": acq, "exp": exp, "killed": pa in killed})

        # Race on fresh job: SAME random draw as baseline
        # But post-patch: the second TryClaim fails because the first UPDATE
        # already set lease_expires_at to now()+TTL (no longer < now())
        if rng.random() < P_RACE_ON_NEW_JOB:
            candidates = [a for a in alive if a != pa]
            if candidates:
                ra = rng.choice(candidates)
                # Consume the same random value as baseline for reproducibility
                _offset = rng.uniform(0.0, race_window_sec)
                # POST-PATCH: this claim is BLOCKED by row-level lock
                # The second UPDATE sees lease_expires_at >= now() -> 0 rows affected
                race_attempts += 1
                race_blocked  += 1
                # Do NOT add to job_events — claim failed

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
            # POST-PATCH: agent directly claims expired lease atomically
            # WHERE status NOT IN ('succeeded','failed') AND lease_expires_at < now()
            # This succeeds because lease has expired
            attempts.append({"agent": na, "acq": acq_r, "exp": exp_r, "killed": False})

            # Race on reassignment: SAME random draw as baseline
            if rng.random() < P_RACE_ON_REASSIGN:
                cands = [a for a in surviving if a != na]
                if cands:
                    ra2 = rng.choice(cands)
                    # Consume same random as baseline
                    _offset2 = rng.uniform(0.0, race_window_sec)
                    # POST-PATCH: BLOCKED — first agent already claimed,
                    # lease_expires_at is now()+TTL, no longer < now()
                    race_attempts += 1
                    race_blocked  += 1
                    # Do NOT add to job_events — claim failed

    # ── Overlap Analysis (IDENTICAL logic) ────────────────────────────────────
    total_jobs = len(job_events)
    duplicate_jobs = 0
    sequential_reassignments = 0
    multi_attempt_jobs = 0

    for jid, attempts in job_events.items():
        if len(attempts) > 1:
            multi_attempt_jobs += 1
        if len(attempts) < 2:
            continue
        found = False
        for a, b in combinations(attempts, 2):
            if a["agent"] == b["agent"]:
                continue
            if a["acq"] < b["exp"] and b["acq"] < a["exp"]:
                found = True
                break
        if found:
            duplicate_jobs += 1
        else:
            sequential_reassignments += 1

    rate = duplicate_jobs / total_jobs * 100.0 if total_jobs > 0 else 0.0

    return {
        "total_jobs":        total_jobs,
        "duplicate_jobs":    duplicate_jobs,
        "rate_pct":          rate,
        "total_attempts":    sum(len(v) for v in job_events.values()),
        "multi_attempt_jobs": multi_attempt_jobs,
        "sequential_reassignments": sequential_reassignments,
        "race_attempts":     race_attempts,
        "race_blocked":      race_blocked,
    }

# ── Run All 25 Experiments ────────────────────────────────────────────────────
print("=" * 78)
print("POST-PATCH: Duplicate Execution Rate - Race Window Sweep")
print(f"Agents={AGENTS}  Tasks={TASKS}  TTL={TTL_SEC}s  Kill={KILL_PERCENT}%  Repeats={REPEATS}")
print("Patch: TryClaim atomic UPDATE (status NOT IN succeeded/failed + lease < now)")
print("=" * 78)

all_results = {}

for rw_ms in RACE_WINDOWS_MS:
    rw_sec = rw_ms / 1000.0
    runs = []
    for rep in range(REPEATS):
        seed = SEED_BASE + rw_ms * 10 + rep  # IDENTICAL to baseline
        r = run_simulation(rw_sec, seed)
        runs.append(r)
        print(f"  window={rw_ms:4d}ms  rep={rep+1}  "
              f"dup={r['duplicate_jobs']:3d}/{r['total_jobs']}  "
              f"rate={r['rate_pct']:.2f}%  "
              f"race_blocked={r['race_blocked']}")

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
print("=" * 78)
print(f"{'Race Window':>14} | {'Mean Rate':>10} | {'Std':>7} | {'Min':>7} | {'Max':>7} | {'Mean Dups':>10}")
print("-" * 78)
for rw_ms, res in all_results.items():
    print(f"{rw_ms:>12}ms | {res['mean_rate']:>9.2f}% | {res['std_rate']:>6.2f}% | "
          f"{res['min_rate']:>6.2f}% | {res['max_rate']:>6.2f}% | {res['mean_dups']:>10.1f}")
print("=" * 78)

# ── Save JSON Report ──────────────────────────────────────────────────────────
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "bench_artifacts", "sim_duplicate", "results")
os.makedirs(OUT_DIR, exist_ok=True)
report_path = os.path.join(OUT_DIR, "duplicate_report_patch.json")

report = {
    "variant": "post-patch (atomic TryClaim)",
    "config": {
        "agents": AGENTS, "tasks": TASKS, "ttl_sec": TTL_SEC,
        "kill_percent": KILL_PERCENT, "repeats": REPEATS,
        "p_race_new": P_RACE_ON_NEW_JOB, "p_race_reassign": P_RACE_ON_REASSIGN,
        "seed_base": SEED_BASE,
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
                {"rep": i+1,
                 "duplicate_jobs": r["duplicate_jobs"],
                 "rate_pct": r["rate_pct"],
                 "total_attempts": r["total_attempts"],
                 "race_attempts": r["race_attempts"],
                 "race_blocked": r["race_blocked"],
                 "sequential_reassignments": r["sequential_reassignments"]}
                for i, r in enumerate(res["runs"])
            ],
        }
        for rw_ms, res in all_results.items()
    }
}

with open(report_path, "w") as f:
    json.dump(report, f, indent=2)

print(f"\nReport saved: {report_path}")
