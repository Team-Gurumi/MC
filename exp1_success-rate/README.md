# exp1_success-rate

External HTTP + SQL harness for Mutual Cloud success-rate experiments.

## Layout

- `runs/<run_id>/config.json`
- `runs/<run_id>/job_metrics.csv`
- `runs/<run_id>/summary.json`
- `runs/<run_id>/summary.csv`
- `runs/<run_id>/submission_ids.txt`
- `runs/<run_id>/submission.log`
- `runs/<run_id>/control_snapshot.log`
- `results.csv` (global append-only rows)

## Configuration

All defaults are in `config.sh` and can be overridden by environment variables.

```bash
export CONTROL_URL="http://127.0.0.1:8080"
export MC_DB_DSN="postgres://mcuser:mcpw@127.0.0.1:5432/mc?sslmode=disable"
export CONTROL_TOKEN="dev"        # optional
export AGENTS=10
export FAILURE_RATE=20
export TTL_SEC=15
export HEARTBEAT_SEC=5
export AGENT_PIDS_FILE="/path/to/agents.pids"   # required when FAILURE_RATE>0
export FAILURE_INJECT_DELAY_SEC=5
```

## Single run

```bash
./run_matrix.sh 100 cpu
```

Run ID format:

`exp1-<UTC timestamp>-<workload>-N<N>-A<agents>-R<rep>`

## Matrix run

```bash
./matrix_runner.sh
```

This executes:

- `N in {50,100,200,500}`
- `workload in {cpu,io}`
- `repetitions = 5`

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
- For `FAILURE_RATE>0`, `run_matrix.sh` requires `AGENT_PIDS_FILE` containing one agent PID per line.
- Kill count is `ceil(live_agents * FAILURE_RATE / 100)`.
- Injection artifacts are saved per run:
  - `runs/<run_id>/failure_injection.log`
  - `runs/<run_id>/killed_agents.txt`

## Reproducibility notes

- Uses strict bash (`set -euo pipefail`)
- Uses `psql -v ON_ERROR_STOP=1`
- Does not require `jq`
- Does not modify app code or DB schema
