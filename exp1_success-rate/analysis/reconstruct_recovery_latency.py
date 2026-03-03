#!/usr/bin/env python3
"""
Reconstruct Mutual Cloud crash-recovery traces from existing logs.

Current log model limitations:
- failure_injected_ts exists only at run scope (failure_injection.log), not per job.
- failure_detected_ts is approximated by the first lease_expired event after crash.
- result_received_ts is approximated by finish_reported_ts from job_metrics.csv
  (agent-observed successful delivery/visibility), because no separate requester log exists.

The script is deterministic:
- it uses the earliest kill event in failure_injection.log as t0 for the run
- it keeps only jobs with exactly one post-failure reassignment path
- it excludes incomplete or ambiguous (multi-reassignment) jobs
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Iterable

RFC3339_RE = re.compile(r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)")
FAILURE_LINE_RE = re.compile(r"^\[(?P<ts>[^\]]+)\]\s+injecting failure_rate=(?P<rate>\d+)%")
KILLED_LINE_RE = re.compile(r"^\[(?P<ts>[^\]]+)\]\s+killed pid=(?P<pid>\d+)")


@dataclass
class TraceRow:
    job_id: str
    t_failure_detection_ms: float
    t_task_reassignment_ms: float
    t_task_restart_ms: float
    t_recovery_total_ms: float
    run_id: str
    failure_rate: int


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_ms(delta_seconds: float) -> float:
    return delta_seconds * 1000.0


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    if len(values) == 1:
        return values[0]
    xs = sorted(values)
    idx = (len(xs) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(xs) - 1)
    if lo == hi:
        return xs[lo]
    frac = idx - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def parse_failure_injection(run_dir: Path) -> tuple[int | None, datetime | None]:
    path = run_dir / "failure_injection.log"
    if not path.exists():
        return None, None

    failure_rate = None
    kill_ts: list[datetime] = []
    for line in path.read_text().splitlines():
        m = FAILURE_LINE_RE.match(line)
        if m:
            failure_rate = int(m.group("rate"))
            continue
        m = KILLED_LINE_RE.match(line)
        if m:
            kill_ts.append(parse_ts(m.group("ts")))

    if not kill_ts:
        return failure_rate, None
    return failure_rate, min(kill_ts)


def iter_json_events(path: Path, event_names: set[str]) -> Iterable[dict]:
    if not path.exists():
        return
    for line in path.read_text(errors="ignore").splitlines():
        idx = line.find("{")
        if idx < 0:
            continue
        blob = line[idx:]
        try:
            obj = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if obj.get("event") in event_names and "job_id" in obj and "timestamp" in obj:
            yield obj


def load_control_events(root: Path, run_id: str) -> tuple[dict[str, list[datetime]], dict[str, list[datetime]]]:
    lease_expired = defaultdict(list)
    reassigned = defaultdict(list)
    for path in sorted(root.glob("control*.log")):
        for obj in iter_json_events(path, {"lease_expired", "reassigned"}):
            job_id = obj["job_id"]
            if not job_id.startswith(run_id + "-"):
                continue
            ts = parse_ts(obj["timestamp"])
            if obj["event"] == "lease_expired":
                lease_expired[job_id].append(ts)
            elif obj["event"] == "reassigned":
                reassigned[job_id].append(ts)
    return lease_expired, reassigned


def load_agent_acquired_events(root: Path, run_id: str) -> dict[str, list[datetime]]:
    acquired = defaultdict(list)
    agents_dir = root / "agents"
    if not agents_dir.exists():
        return acquired
    for path in sorted(agents_dir.glob("agent_*.log")):
        for obj in iter_json_events(path, {"lease_acquired"}):
            job_id = obj["job_id"]
            if not job_id.startswith(run_id + "-"):
                continue
            acquired[job_id].append(parse_ts(obj["timestamp"]))
    return acquired


def load_job_metrics(run_dir: Path) -> dict[str, dict]:
    path = run_dir / "job_metrics.csv"
    if not path.exists():
        return {}
    with path.open(newline="") as f:
        return {row["job_id"]: row for row in csv.DictReader(f)}


def build_trace_rows(root: Path, run_dir: Path) -> tuple[list[TraceRow], list[tuple[str, str]]]:
    run_id = run_dir.name
    failure_rate, t0 = parse_failure_injection(run_dir)
    if failure_rate is None or t0 is None:
        return [], [(run_id, "missing_failure_injection")]

    lease_expired, reassigned = load_control_events(root, run_id)
    acquired = load_agent_acquired_events(root, run_id)
    job_metrics = load_job_metrics(run_dir)

    rows: list[TraceRow] = []
    excluded: list[tuple[str, str]] = []

    for job_id, exp_events in lease_expired.items():
        detected_candidates = [ts for ts in exp_events if ts >= t0]
        if not detected_candidates:
            continue
        failure_detected_ts = min(detected_candidates)

        reassigned_after = sorted(ts for ts in reassigned.get(job_id, []) if ts > failure_detected_ts)
        acquired_after = sorted(ts for ts in acquired.get(job_id, []) if ts > failure_detected_ts)
        transition_candidates = reassigned_after or acquired_after
        if not transition_candidates:
            excluded.append((job_id, "missing_reassignment"))
            continue

        if len(reassigned_after) > 1 or (not reassigned_after and len(acquired_after) > 1):
            excluded.append((job_id, "multi_reassignment"))
            continue

        reassignment_ts = transition_candidates[0]
        jm = job_metrics.get(job_id)
        if not jm:
            excluded.append((job_id, "missing_job_metrics"))
            continue

        success = str(jm.get("success", "")).lower() == "true"
        status = jm.get("status", "")
        if not success or status != "succeeded":
            excluded.append((job_id, "not_strict_success"))
            continue

        exec_start = jm.get("exec_start_ts", "")
        exec_end = jm.get("exec_end_ts", "")
        finish_reported = jm.get("finish_reported_ts", "")
        if not exec_start or not exec_end or not finish_reported:
            excluded.append((job_id, "missing_terminal_timestamps"))
            continue

        task_start_ts = parse_ts(exec_start)
        task_complete_ts = parse_ts(exec_end)
        result_received_ts = parse_ts(finish_reported)

        if task_start_ts < reassignment_ts:
            excluded.append((job_id, "task_start_before_reassignment"))
            continue
        if result_received_ts < task_complete_ts:
            excluded.append((job_id, "result_before_complete"))
            continue

        rows.append(
            TraceRow(
                job_id=job_id,
                t_failure_detection_ms=to_ms((failure_detected_ts - t0).total_seconds()),
                t_task_reassignment_ms=to_ms((reassignment_ts - failure_detected_ts).total_seconds()),
                t_task_restart_ms=to_ms((result_received_ts - reassignment_ts).total_seconds()),
                t_recovery_total_ms=to_ms((result_received_ts - t0).total_seconds()),
                run_id=run_id,
                failure_rate=failure_rate,
            )
        )

    return rows, excluded


def summarize(rows: list[TraceRow], run_id: str, failure_rate: int) -> dict[str, str]:
    totals = [r.t_recovery_total_ms for r in rows]
    detects = [r.t_failure_detection_ms for r in rows]
    reassign = [r.t_task_reassignment_ms for r in rows]
    restart = [r.t_task_restart_ms for r in rows]
    return {
        "run_id": run_id,
        "failure_rate": str(failure_rate),
        "trace_count": str(len(rows)),
        "recovery_mean_ms": f"{mean(totals):.3f}",
        "recovery_p50_ms": f"{percentile(totals, 0.50):.3f}",
        "recovery_p95_ms": f"{percentile(totals, 0.95):.3f}",
        "recovery_p99_ms": f"{percentile(totals, 0.99):.3f}",
        "failure_detection_mean_ms": f"{mean(detects):.3f}",
        "task_reassignment_mean_ms": f"{mean(reassign):.3f}",
        "task_restart_mean_ms": f"{mean(restart):.3f}",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs-root", default="exp1_success-rate/raw/runs")
    parser.add_argument("--run-glob", default="exp1-*")
    parser.add_argument("--trace-out", default="exp1_success-rate/analysis/recovery_traces.csv")
    parser.add_argument("--summary-out", default="exp1_success-rate/analysis/recovery_summary.csv")
    parser.add_argument("--excluded-out", default="exp1_success-rate/analysis/recovery_excluded.csv")
    args = parser.parse_args()

    runs_root = Path(args.runs_root).resolve()
    run_dirs = sorted(
        p for p in runs_root.glob(args.run_glob)
        if p.is_dir() and (p / "failure_injection.log").exists()
    )

    trace_rows: list[TraceRow] = []
    excluded_rows: list[dict[str, str]] = []
    summary_rows: list[dict[str, str]] = []

    for run_dir in run_dirs:
        rows, excluded = build_trace_rows(runs_root, run_dir)
        trace_rows.extend(rows)
        for job_id, reason in excluded:
            excluded_rows.append({"run_id": run_dir.name, "job_id": job_id, "reason": reason})
        if rows:
            summary_rows.append(summarize(rows, run_dir.name, rows[0].failure_rate))

    trace_out = Path(args.trace_out)
    trace_out.parent.mkdir(parents=True, exist_ok=True)
    with trace_out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "job_id",
            "T_failure_detection_ms",
            "T_task_reassignment_ms",
            "T_task_restart_ms",
            "T_recovery_total_ms",
            "run_id",
            "failure_rate",
        ])
        for r in trace_rows:
            w.writerow([
                r.job_id,
                f"{r.t_failure_detection_ms:.3f}",
                f"{r.t_task_reassignment_ms:.3f}",
                f"{r.t_task_restart_ms:.3f}",
                f"{r.t_recovery_total_ms:.3f}",
                r.run_id,
                r.failure_rate,
            ])

    with Path(args.summary_out).open("w", newline="") as f:
        fieldnames = [
            "run_id",
            "failure_rate",
            "trace_count",
            "recovery_mean_ms",
            "recovery_p50_ms",
            "recovery_p95_ms",
            "recovery_p99_ms",
            "failure_detection_mean_ms",
            "task_reassignment_mean_ms",
            "task_restart_mean_ms",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(summary_rows)

    with Path(args.excluded_out).open("w", newline="") as f:
        fieldnames = ["run_id", "job_id", "reason"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(excluded_rows)

    print(f"runs_processed={len(run_dirs)} traces={len(trace_rows)} excluded={len(excluded_rows)}")
    print(f"trace_csv={trace_out}")
    print(f"summary_csv={Path(args.summary_out)}")
    print(f"excluded_csv={Path(args.excluded_out)}")


if __name__ == "__main__":
    main()
