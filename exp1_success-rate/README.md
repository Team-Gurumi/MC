# exp1_success-rate

External HTTP + SQL harness for Mutual Cloud success-rate experiments.

## Layout

- `raw/runs/<run_id>/...` raw per-run artifacts
- `raw/summary.csv` raw cumulative summary rows
- `raw/results.csv` raw append-only result rows
- `curated/summary_dedup_latest.csv` latest deduplicated view
- `curated/summary_dedup_passonly.csv` strict pass-only deduplicated view
- `curated/summary_aggregated.csv` aggregated matrix view
- `curated/pending_final105.csv` pending cells against final105
- `analysis/` notebooks/scripts for one-off analysis
- `figures/` generated plots/figures
- `pipeline/` harness scripts/sql entrypoints

Canonical path policy:
- run and data artifacts are authoritative only under `raw/`, `curated/`, and `pipeline/` (no root-level compatibility aliases).

## Configuration

All defaults are in `pipeline/config.sh` and can be overridden by environment variables.

```bash
export CONTROL_URL="http://127.0.0.1:8080"
export MC_DB_DSN="postgres://mc:mcpass@127.0.0.1:5432/mcdb?sslmode=disable"
export CONTROL_TOKEN="dev"        # optional
export TTL_SEC=10
export HEARTBEAT_SEC=3
export FAILURE_INJECT_DELAY_SEC=5
export RUN_TIMEOUT_SEC=600
export QUEUED_TIMEOUT_SEC=180
```

## Single run

```bash
./pipeline/run_matrix.sh 100 cpu
```

Run ID format:

`exp1-<UTC timestamp>-<workload>-N<N>-A<agents>-R<rep>`

## Final matrix run (Section 4.1 / 4.2)

```bash
./pipeline/matrix_runner.sh
```

Default profile is `MATRIX_PROFILE=final105`, which executes:

- Baseline:
  - `A in {10,25,50}`
  - `workload in {cpu,io}`
  - `N=A` (balanced), `N=5A` (overload)
  - `REP=5`
- Crash:
  - `A in {10,25,50}`
  - `failure_rate in {10,20,40}`
  - `workload=cpu`
  - `N=5A`
  - `REP=5`

Total runs: `105`.

Optional:

```bash
CRASH_INCLUDE_IO=1 ./pipeline/matrix_runner.sh
```

This adds IO crash matrix and total becomes `150`.

Legacy matrix can still be executed with:

```bash
MATRIX_PROFILE=legacy ./pipeline/matrix_runner.sh
```

## Metrics

Per run, the harness computes and stores:

- total jobs
- succeeded jobs
- success rate
- makespan seconds
- throughput (total_jobs / makespan)
- retry stats (avg, max)
- latency percentiles p50/p95/p99 from `duration_ms` (if present)

## Failure injection model

- `FAILURE_RATE` is applied as process-crash injection (agent kill), not synthetic task failure.
- For `FAILURE_RATE>0`, `pipeline/run_agents.sh` writes and `pipeline/run_matrix.sh` reads `AGENT_PIDS_FILE`.
- Kill count is `ceil(live_agents * FAILURE_RATE / 100)`.
- Injection artifacts are saved per run:
  - `raw/runs/<run_id>/failure_injection.log`
  - `raw/runs/<run_id>/killed_agents.txt`

## Reproducibility notes

- Uses strict bash (`set -euo pipefail`)
- Uses `psql -v ON_ERROR_STOP=1`
- Does not require `jq`
- Does not modify app code or DB schema

## Common failure causes (observed)

- `401 unauthorized` on submission:
  - `CONTROL_TOKEN` in harness and control process do not match.
- Agents start but do not process jobs:
  - stale/invalid `BOOTSTRAP` value, resulting in DHT bootstrap failure (`no peers in routing table`).
- DB query errors during run:
  - `MC_DB_DSN` not exported in current shell, causing local socket fallback or auth mismatch.
- Baseline success-rate unexpectedly drops:
  - `QUEUED_TIMEOUT_SEC` too low for heavy overload cells (jobs may fail by timeout before execution).
- Run contamination:
  - previous unfinished jobs (`queued/assigned/running`) or mixed old artifacts in `raw/runs/`.

## Pre-run sanity checklist

- Control is running with matching env:
  - `CONTROL_TOKEN`, `RUN_TIMEOUT_SEC`, `QUEUED_TIMEOUT_SEC`.
- Harness shell exports:
  - `CONTROL_URL`, `MC_DB_DSN`, `CONTROL_TOKEN`, `TTL_SEC`, `HEARTBEAT_SEC`.
- Bootstrap is current for this control/seeder session:
  - `export BOOTSTRAP='...'`.
- DB backlog is empty before each run:
  - `queued/assigned/running = 0`.
- Final matrix lock is respected:
  - `MATRIX_PROFILE=final105`, `CRASH_INCLUDE_IO=0`, `REPETITIONS=5`.
