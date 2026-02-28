#!/usr/bin/env python3
"""
Duplicate Execution Rate Experiment Simulator

Simulates the MC distributed job scheduling system to measure
duplicate execution rate based on lease overlap.

Scenario:
- N agents compete for M jobs
- Each agent discovers jobs and tries to claim a lease
- Due to race conditions (network delay, clock skew), multiple agents
  can sometimes claim the same job within a short window
- We measure: duplicate jobs (lease overlap) / total jobs
"""

import os
import json
import random
import math
from datetime import datetime, timezone, timedelta

# ── Experiment Parameters ──────────────────────────────────────────────────────
AGENTS       = 100
TASKS        = 200
TTL_SEC      = 10        # lease TTL in seconds
HB_SEC       = 3         # heartbeat interval
KILL_PERCENT = 10        # % of agents killed mid-run

# Race window: how long (seconds) after one agent claims a job can another
# agent also succeed in claiming it (simulates network delay / lock race)
RACE_WINDOW_SEC = 0.5    # 500ms race window — realistic for distributed systems

# Probability that a second agent races for the same job when it's "hot"
# (i.e., just became available or was just reassigned after a killed agent)
P_RACE_ON_NEW_JOB   = 0.04   # 4% chance of race on fresh job
P_RACE_ON_REASSIGN  = 0.12   # 12% chance of race on reassigned job (higher: two agents see it simultaneously)

SEED = 405
random.seed(SEED)

# ── Output Paths ───────────────────────────────────────────────────────────────
ARTIFACT_DIR = os.path.join(os.path.dirname(__file__), "..", "bench_artifacts", "sim_duplicate")
LOG_DIR      = os.path.join(ARTIFACT_DIR, "logs")
RES_DIR      = os.path.join(ARTIFACT_DIR, "results")
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(RES_DIR, exist_ok=True)

BASE_TIME = datetime(2026, 2, 19, 5, 0, 0, tzinfo=timezone.utc)

def ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00"

def make_agent_id(i: int) -> str:
    return f"12D3KooWAgent{i:04d}"

# ── Simulation ─────────────────────────────────────────────────────────────────
# Each execution attempt: (job_id, agent_id, lease_acquired_ts, lease_expire_ts)
executions: list[dict] = []

# Track which agents are alive
alive_agents = list(range(1, AGENTS + 1))

# Kill KILL_PERCENT of agents at t=10s
kill_count = math.ceil(AGENTS * KILL_PERCENT / 100)
killed_agents = set(random.sample(alive_agents, kill_count))
surviving_agents = [a for a in alive_agents if a not in killed_agents]

print(f"Agents: {AGENTS}  Tasks: {TASKS}  TTL: {TTL_SEC}s  Kill: {kill_count} agents")
print(f"Race window: {RACE_WINDOW_SEC}s  P(race|new): {P_RACE_ON_NEW_JOB}  P(race|reassign): {P_RACE_ON_REASSIGN}")
print()

# Assign initial jobs to agents (round-robin with some randomness)
# Each job gets a primary agent; some get a racing second agent
job_events: dict[str, list[dict]] = {}  # job_id -> list of execution attempts

for j in range(1, TASKS + 1):
    job_id = f"job-sim-{j:04d}"
    job_events[job_id] = []

    # Primary agent claims the job at a random time in [0, 8]s
    primary_agent_idx = random.choice(alive_agents)
    primary_agent_id  = make_agent_id(primary_agent_idx)
    t_claim = random.uniform(0.5, 8.0)
    acq_dt  = BASE_TIME + timedelta(seconds=t_claim)
    exp_dt  = acq_dt + timedelta(seconds=TTL_SEC)

    job_events[job_id].append({
        "agent_id":         primary_agent_id,
        "lease_acquired_ts": ts(acq_dt),
        "lease_expire_ts":   ts(exp_dt),
        "killed":           primary_agent_idx in killed_agents,
    })

    # Race condition: another agent also claims the same job within RACE_WINDOW_SEC
    p_race = P_RACE_ON_NEW_JOB
    if random.random() < p_race:
        racing_candidates = [a for a in alive_agents if a != primary_agent_idx]
        if racing_candidates:
            racing_agent_idx = random.choice(racing_candidates)
            racing_agent_id  = make_agent_id(racing_agent_idx)
            # Racing agent claims slightly later (within race window)
            t_race_offset = random.uniform(0.0, RACE_WINDOW_SEC)
            acq_dt2 = acq_dt + timedelta(seconds=t_race_offset)
            exp_dt2 = acq_dt2 + timedelta(seconds=TTL_SEC)

            job_events[job_id].append({
                "agent_id":         racing_agent_id,
                "lease_acquired_ts": ts(acq_dt2),
                "lease_expire_ts":   ts(exp_dt2),
                "killed":           racing_agent_idx in killed_agents,
            })

# Reassignment after kill: killed agents' jobs get reassigned to surviving agents
# Some reassignments also race
reassigned_jobs = []
for job_id, attempts in job_events.items():
    if not attempts:
        continue
    primary = attempts[0]
    if primary["killed"]:
        # Primary agent was killed → job gets reassigned after TTL expires
        # Reassignment happens at: primary.expire + random(0, 3)s
        orig_exp = datetime.fromisoformat(primary["lease_expire_ts"])
        t_reassign_offset = random.uniform(0.5, 3.0)
        acq_dt_r = orig_exp + timedelta(seconds=t_reassign_offset)
        exp_dt_r = acq_dt_r + timedelta(seconds=TTL_SEC)

        if surviving_agents:
            new_agent_idx = random.choice(surviving_agents)
            new_agent_id  = make_agent_id(new_agent_idx)

            attempts.append({
                "agent_id":         new_agent_id,
                "lease_acquired_ts": ts(acq_dt_r),
                "lease_expire_ts":   ts(exp_dt_r),
                "killed":           False,
                "reassigned":       True,
            })
            reassigned_jobs.append(job_id)

            # Race on reassignment: higher probability
            if random.random() < P_RACE_ON_REASSIGN:
                racing_candidates = [a for a in surviving_agents if a != new_agent_idx]
                if racing_candidates:
                    racing_agent_idx = random.choice(racing_candidates)
                    racing_agent_id  = make_agent_id(racing_agent_idx)
                    t_race_offset = random.uniform(0.0, RACE_WINDOW_SEC)
                    acq_dt_r2 = acq_dt_r + timedelta(seconds=t_race_offset)
                    exp_dt_r2 = acq_dt_r2 + timedelta(seconds=TTL_SEC)

                    attempts.append({
                        "agent_id":         racing_agent_id,
                        "lease_acquired_ts": ts(acq_dt_r2),
                        "lease_expire_ts":   ts(exp_dt_r2),
                        "killed":           False,
                        "reassigned_race":  True,
                    })

# ── Write Agent Log Files ──────────────────────────────────────────────────────
# Distribute execution_started events across agent log files
agent_logs: dict[str, list[str]] = {}  # agent_id -> list of log lines

for job_id, attempts in job_events.items():
    for attempt in attempts:
        agent_id = attempt["agent_id"]
        acq_ts   = attempt["lease_acquired_ts"]
        exp_ts   = attempt["lease_expire_ts"]

        log_line = json.dumps({
            "event":            "execution_started",
            "timestamp":        acq_ts,
            "job_id":           job_id,
            "agent_id":         agent_id,
            "lease_acquired_ts": acq_ts,
            "lease_expire_ts":   exp_ts,
        })

        if agent_id not in agent_logs:
            agent_logs[agent_id] = []
        agent_logs[agent_id].append(log_line)

# Write one log file per agent (named agent-N.log to match analyzer expectations)
agent_id_to_num = {make_agent_id(i): i for i in range(1, AGENTS + 1)}

for agent_id, lines in agent_logs.items():
    num = agent_id_to_num.get(agent_id, 0)
    log_path = os.path.join(LOG_DIR, f"agent-{num}.log")
    with open(log_path, "w") as f:
        for line in lines:
            f.write(line + "\n")

print(f"Wrote {len(agent_logs)} agent log files to {LOG_DIR}")
print(f"Total execution attempts logged: {sum(len(v) for v in job_events.values())}")
print(f"Jobs with multiple attempts: {sum(1 for v in job_events.values() if len(v) > 1)}")
print(f"Killed agents: {kill_count}  Reassigned jobs: {len(reassigned_jobs)}")
print()

# ── Write config.txt (for compatibility) ──────────────────────────────────────
with open(os.path.join(ARTIFACT_DIR, "config.txt"), "w") as f:
    f.write(f"AGENTS={AGENTS}\nTASKS={TASKS}\nTTL_SEC={TTL_SEC}\nHB_SEC={HB_SEC}\n")

print(f"Artifact dir: {ARTIFACT_DIR}")
print("Simulation complete. Run analyze_duplicate_overlap.py next.")
