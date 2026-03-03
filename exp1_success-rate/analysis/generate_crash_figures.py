#!/usr/bin/env python3
import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CURATED = ROOT / "curated"
FIGURES = ROOT / "figures"

SUMMARY_AGG = CURATED / "summary_aggregated.csv"
SUMMARY_PASS = CURATED / "summary_dedup_passonly.csv"

AGENTS = [10, 25, 50]
FAILURE_RATES = [10, 20, 40]


def parse_int(value):
    return int(float(value))


def parse_float(value):
    return float(value)


def load_csv(path):
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def latest_rows_by_key(rows, keys):
    latest = {}
    for row in rows:
        latest[tuple(row[k] for k in keys)] = row
    return list(latest.values())


def filter_aggregated(rows, *, phase, workload="cpu", load_model="overload"):
    filtered = []
    for row in rows:
        if row["phase"] != phase:
            continue
        if row["workload"] != workload:
            continue
        if row["load_model"] != load_model:
            continue
        filtered.append(row)
    return latest_rows_by_key(
        filtered,
        ["phase", "workload", "load_model", "agents", "failure_rate"],
    )


def index_by_agent_and_failure(rows):
    indexed = {}
    for row in rows:
        agent = parse_int(row["agents"])
        failure = parse_int(row["failure_rate"])
        indexed[(agent, failure)] = row
    return indexed


def save_crash_success_rate(plt, crash_rows):
    grouped = index_by_agent_and_failure(crash_rows)
    x = list(range(len(FAILURE_RATES)))
    width = 0.22

    plt.figure(figsize=(8, 5))
    for idx, agent in enumerate(AGENTS):
        heights = []
        for fr in FAILURE_RATES:
            row = grouped.get((agent, fr))
            heights.append(parse_float(row["mean_success_rate"]) if row else 0.0)
        pos = [v + (idx - 1) * width for v in x]
        plt.bar(pos, heights, width=width, label=f"A={agent}")

    plt.xticks(x, [str(fr) for fr in FAILURE_RATES])
    plt.ylim(0.0, 1.05)
    plt.xlabel("Failure Rate (%)")
    plt.ylabel("Mean Success Rate")
    plt.title("Crash Phase Success Rate vs Failure Rate")
    plt.legend(title="Agents")
    plt.tight_layout()
    plt.savefig(FIGURES / "fig1_crash_success_rate_vs_failure.png", dpi=200)
    plt.close()


def save_tail_line(plt, rows, metric, filename, title):
    grouped = index_by_agent_and_failure(rows)
    plt.figure(figsize=(8, 5))
    for fr in FAILURE_RATES:
        x = []
        y = []
        for agent in AGENTS:
            row = grouped.get((agent, fr))
            if not row:
                continue
            if parse_int(row["accepted_runs"]) == 0:
                continue
            x.append(agent)
            y.append(parse_float(row[metric]))
        if x:
            plt.plot(x, y, marker="o", label=f"failure={fr}%")

    plt.xlabel("Agents")
    plt.ylabel(metric.replace("_", " ").replace("ms", "(ms)"))
    plt.title(title)
    plt.xticks(AGENTS)
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES / filename, dpi=200)
    plt.close()


def save_baseline_vs_crash(plt, baseline_rows, crash_rows):
    baseline = index_by_agent_and_failure(baseline_rows)
    crash = index_by_agent_and_failure(crash_rows)
    x = list(range(len(AGENTS)))
    width = 0.2

    plt.figure(figsize=(9, 5))
    baseline_vals = [parse_float(baseline[(agent, 0)]["e2e_p99_ms"]) for agent in AGENTS]
    plt.bar([v - 1.5 * width for v in x], baseline_vals, width=width, label="baseline")

    for idx, fr in enumerate(FAILURE_RATES):
        vals = []
        for agent in AGENTS:
            row = crash.get((agent, fr))
            vals.append(parse_float(row["e2e_p99_ms"]) if row and parse_int(row["accepted_runs"]) > 0 else 0.0)
        plt.bar([v + (idx - 0.5) * width for v in x], vals, width=width, label=f"crash {fr}%")

    plt.xticks(x, [str(a) for a in AGENTS])
    plt.xlabel("Agents")
    plt.ylabel("e2e p99 (ms)")
    plt.title("Baseline vs Crash e2e p99")
    plt.legend()
    plt.tight_layout()
    plt.savefig(FIGURES / "fig4_baseline_vs_crash_e2e_p99.png", dpi=200)
    plt.close()


def save_retry_vs_tail(plt, pass_rows):
    points = []
    for row in pass_rows:
        agent = parse_int(row["agents"])
        jobs = parse_int(row["jobs"])
        failure = parse_int(row["failure_rate"])
        workload = row["workload"]
        if workload != "cpu":
            continue
        if failure not in FAILURE_RATES:
            continue
        if jobs != agent * 5:
            continue
        points.append(row)

    plt.figure(figsize=(8, 5))
    x = [parse_float(r["avg_retry_count"]) for r in points]
    y = [parse_float(r["e2e_p99_ms"]) for r in points]
    plt.scatter(x, y)
    plt.xlabel("Average Retry Count")
    plt.ylabel("e2e p99 (ms)")
    plt.title("Crash Runs: Retry Count vs e2e Tail")
    plt.tight_layout()
    plt.savefig(FIGURES / "fig5_retry_vs_e2e_tail.png", dpi=200)
    plt.close()


def compute_increase_stats(baseline_rows, crash_rows):
    baseline = index_by_agent_and_failure(baseline_rows)
    crash = index_by_agent_and_failure(crash_rows)

    lines = []
    lines.append("Baseline to Crash e2e_p99 Increase (%)")
    for agent in AGENTS:
        base = parse_float(baseline[(agent, 0)]["e2e_p99_ms"])
        parts = []
        for fr in FAILURE_RATES:
            row = crash.get((agent, fr))
            if not row:
                parts.append(f"{fr}%=N/A (missing row)")
                continue
            if parse_int(row["accepted_runs"]) == 0:
                parts.append(f"{fr}%=N/A (accepted_runs=0)")
                continue
            cur = parse_float(row["e2e_p99_ms"])
            pct = ((cur - base) / base) * 100.0
            parts.append(f"{fr}%={pct:.2f}%")
        lines.append(f"  agents={agent}: " + ", ".join(parts))

    lines.append("")
    lines.append("Failure 10% to 40% e2e_p99 Increase (%)")
    for agent in AGENTS:
        low_row = crash.get((agent, 10))
        high_row = crash.get((agent, 40))
        if not low_row or not high_row:
            lines.append(f"  agents={agent}: N/A (missing row)")
            continue
        if parse_int(low_row["accepted_runs"]) == 0 or parse_int(high_row["accepted_runs"]) == 0:
            lines.append(f"  agents={agent}: N/A (accepted_runs=0 in comparison)")
            continue
        low = parse_float(low_row["e2e_p99_ms"])
        high = parse_float(high_row["e2e_p99_ms"])
        pct = ((high - low) / low) * 100.0
        lines.append(f"  agents={agent}: {pct:.2f}%")
    return "\n".join(lines)


def main():
    FIGURES.mkdir(parents=True, exist_ok=True)
    agg_rows = load_csv(SUMMARY_AGG)
    pass_rows = load_csv(SUMMARY_PASS)

    baseline_rows = filter_aggregated(agg_rows, phase="baseline")
    crash_rows = filter_aggregated(agg_rows, phase="crash")
    stats_text = compute_increase_stats(baseline_rows, crash_rows)

    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError:
        print(stats_text)
        print()
        print("matplotlib is not installed; figure generation skipped.")
        return

    save_crash_success_rate(plt, crash_rows)
    save_tail_line(plt, crash_rows, "e2e_p99_ms", "fig2_e2e_p99_vs_agents.png", "Crash Phase e2e p99 vs Agents")
    save_tail_line(plt, crash_rows, "e2e_p95_ms", "fig3_e2e_p95_vs_agents.png", "Crash Phase e2e p95 vs Agents")
    save_baseline_vs_crash(plt, baseline_rows, crash_rows)
    save_retry_vs_tail(plt, pass_rows)
    print(stats_text)


if __name__ == "__main__":
    main()
