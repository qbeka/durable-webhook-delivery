#!/usr/bin/env python3
"""Turns recorded runs into docs/report.md and site data. Standard library only.

Every number comes from a result file, and the sustained rule is applied by code,
exactly as docs/preregistration.md states it, so no figure is copied by hand.

  python3 scripts/report.py [results/published]
"""
import json
import statistics
import sys
from pathlib import Path

P99_BUDGET_MS = 250
SLOPE_LIMIT = 0.01  # share of the offered rate


def load(path):
    return json.loads(Path(path).read_text())


def repetitions(directory):
    return [load(p) for p in sorted(Path(directory).glob("rep-*/result.json"))]


def sustained(result):
    """The four conditions in docs/preregistration.md, for one repetition."""
    rate = result["config"]["offered_events_per_second"]
    return (
        result["error_rate"] == 0
        and result["arrival_to_effect_ms"]["p99_ms"] < P99_BUDGET_MS
        and abs(result["backlog_slope_per_second"]) <= SLOPE_LIMIT * rate
        and result["reconcile"]["pass"]
        and result["drained"]
    )


def median_range(values, digits=0):
    middle = statistics.median(values)
    return f"{middle:,.{digits}f} ({min(values):,.{digits}f} to {max(values):,.{digits}f})"


def latency_cell(results, key):
    return median_range([r["arrival_to_effect_ms"][key] for r in results], 1)


def contended_note(results):
    count = sum(1 for r in results if r["machine"]["contended"])
    cpu = statistics.median(r["machine"]["background_cpu_percent"] for r in results)
    return f"{count} of {len(results)}, background CPU {cpu:,.0f}%"


def ladder_section(root):
    lines = ["## H2 and H3: the highest sustained rate", ""]
    lines += ["| Workers | Offered per second | Sustained | Completed per second | p50 ms | p99 ms | Backlog slope per second | Contended reps |",
              "|---:|---:|---|---:|---:|---:|---:|---|"]
    best = {}
    for directory in sorted((root / "ladder").glob("ladder-w*"),
                            key=lambda p: tuple(int(x) for x in p.name.replace("ladder-w", "").split("-r"))):
        results = repetitions(directory)
        if not results:
            continue
        workers = results[0]["config"]["options"]["workers"]
        rate = results[0]["config"]["offered_events_per_second"]
        passed = len(results) >= 3 and all(sustained(r) for r in results)
        if passed:
            best[workers] = max(best.get(workers, 0), rate)
        slopes = [r["backlog_slope_per_second"] for r in results]
        lines.append(f"| {workers} | {rate:,.0f} | {'yes' if passed else 'no'} | "
                     f"{median_range([r['completed_rate_per_second'] for r in results])} | "
                     f"{latency_cell(results, 'p50_ms')} | {latency_cell(results, 'p99_ms')} | "
                     f"{median_range(slopes, 1)} | {contended_note(results)} |")
    lines.append("")
    return lines, best


def target_section(root):
    directories = sorted((root / "target").glob("*"))
    lines = ["## H1: the target", ""]
    results = repetitions(directories[0]) if directories else []
    if not results:
        return lines + ["Not run.", ""], None
    lines += ["| Rep | Completed per second | p50 ms | p95 ms | p99 ms | Max ms | Backlog slope | Error rate | Reconciled | Effects counted | Contended |",
              "|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---|"]
    for result in results:
        latency = result["arrival_to_effect_ms"]
        lines.append(f"| {result['repetition']} | {result['completed_rate_per_second']:,.1f} | {latency['p50_ms']:.1f} | "
                     f"{latency['p95_ms']:.1f} | {latency['p99_ms']:.1f} | {latency['max_ms']:.1f} | "
                     f"{result['backlog_slope_per_second']:.2f} | {result['error_rate']:.4f} | "
                     f"{'yes' if result['reconcile']['pass'] else 'no'} | {latency['count']:,} | "
                     f"{'yes' if result['machine']['contended'] else 'no'} |")
    passed = len(results) >= 3 and all(sustained(r) for r in results)
    lines += ["", f"Decision: {'supported' if passed else 'not supported'}.", ""]
    return lines, {"passed": passed, "results": results}


def recovery_section(root):
    lines = ["## H4: isolation", "",
             "| Configuration | Healthy p99 before, ms | Healthy p99 during, ms | Healthy p99 after, ms | Peak backlog | Seconds to catch up | Verdicts | Contended |",
             "|---|---:|---:|---:|---:|---:|---|---|"]
    summary = {}
    for directory in sorted((root / "recovery").glob("*")):
        results = [load(p) for p in sorted(directory.glob("rep-*/recovery.json"))]
        if not results:
            continue
        summary[directory.name] = results
        lines.append(
            f"| {directory.name} | {median_range([r['healthy_before_ms']['p99_ms'] for r in results], 1)} | "
            f"{median_range([r['healthy_during_ms']['p99_ms'] for r in results], 1)} | "
            f"{median_range([r['healthy_after_ms']['p99_ms'] for r in results], 1)} | "
            f"{median_range([r['peak_backlog_failing_destination'] for r in results])} | "
            f"{median_range([r['seconds_to_drain_after_outage'] for r in results], 1)} | "
            f"{', '.join(r['verdict'] for r in results)} | {contended_note(results)} |")
    lines.append("")
    if "slow-limits-on" in summary and "slow-limits-off" in summary:
        on = all(r["healthy_during_ms"]["p99_ms"] < P99_BUDGET_MS for r in summary["slow-limits-on"])
        off = all(r["healthy_during_ms"]["p99_ms"] >= P99_BUDGET_MS for r in summary["slow-limits-off"])
        lines += [f"Decision: {'supported' if on and off else 'not supported'}. With limits on, the healthy "
                  f"p99 stayed under {P99_BUDGET_MS} ms in every repetition: {'yes' if on else 'no'}. "
                  f"With limits off, it did not: {'yes' if off else 'no'}.", ""]
    return lines, summary


def flush_section(root):
    lines = ["## H5: the cost of flushing", "",
             "| `wal_sync_method` | Completed per second | p50 ms | p99 ms | Contended |", "|---|---:|---:|---:|---|"]
    names = {"flush-writethrough": "fsync_writethrough", "flush-default": "open_datasync (macOS default)"}
    for directory, label in names.items():
        results = repetitions(root / "flush" / directory)
        if results:
            lines.append(f"| {label} | {median_range([r['completed_rate_per_second'] for r in results])} | "
                         f"{latency_cell(results, 'p50_ms')} | {latency_cell(results, 'p99_ms')} | {contended_note(results)} |")
    lines.append("")
    return lines


def faults_section(root):
    lines = ["## H6: the failure run", "",
             "| Scenario | Verdict | Faults delivered | Expected effects | Repeated | Missing | Dead | Unfinished | Stale | Expired leases | Drain s |",
             "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    scenarios = []
    for path in sorted((root / "faults").glob("*/result.json")):
        scenarios.append(load(path))
    order = ["control_naive", "control_idempotent"]
    scenarios.sort(key=lambda s: (order.index(s["scenario"]) if s["scenario"] in order else 9, s["scenario"]))
    for scenario in scenarios:
        report = scenario["reconcile"]
        delivered = ", ".join(f"{k} {v}" for k, v in sorted(scenario["delivered_faults"].items()))
        lines.append(f"| {scenario['scenario']} | {scenario['verdict']} | {delivered} | {report['expected_effects']:,} | "
                     f"{report['repeated_effects']:,} | {report['missing_effects']:,} | {report['dead_letters']} | "
                     f"{report['unfinished_jobs']} | {report['stale_attempts']} | {report['expired_leases']} | {scenario['drain_seconds']:.1f} |")
    passed = bool(scenarios) and all(s["verdict"] in ("pass", "control_detected") for s in scenarios)
    lines += ["", f"Decision: {'supported' if passed else 'not supported'}.", ""]
    return lines, scenarios


def generator_section(root):
    path = root / "generator" / "generator.json"
    lines = ["## The generator's own limit", ""]
    if not path.exists():
        return lines + ["Not run.", ""]
    lines += ["Measured with no service behind it, so the service numbers above cannot be its limit.", "",
              "| Offered per second | Achieved per second | Send lag p99 us |", "|---:|---:|---:|"]
    for row in load(path):
        lines.append(f"| {row['rate_per_second']:,.0f} | {row['achieved_rate_per_second']:,.0f} | {row['send_lag_p99_us']:,.0f} |")
    lines.append("")
    return lines


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "results/published")
    manifest_paths = sorted(root.glob("**/manifest.json"))
    commits = sorted({load(p)["commit"][:7] for p in manifest_paths})
    trees = sorted({load(p)["working_tree"] for p in manifest_paths})

    ladder, best = ladder_section(root)
    target, target_summary = target_section(root)
    recovery, recovery_summary = recovery_section(root)
    faults, scenarios = faults_section(root)
    lines = ["# Benchmark report", "",
             "Generated by `scripts/report.py` from the files in `results/published`. Do not edit by hand.", "",
             "Each row with a range shows the median and the range of three repetitions, unless it says otherwise.",
             f"Runs came from commit {', '.join(commits) or 'none'}. Working tree: {', '.join(trees) or 'unknown'}.",
             "Decision rules are in `docs/preregistration.md`.", ""]
    lines += target + ladder
    if best:
        lines += ["Highest sustained rate: " + ", ".join(f"{w} worker{'s' if w > 1 else ''}: {r:,.0f} per second" for w, r in sorted(best.items())) + ".", ""]
    lines += recovery + flush_section(root) + faults + generator_section(root)
    Path("docs/report.md").write_text("\n".join(lines))
    print("\n".join(lines))
    json_path = root / "summary.json"
    json_path.write_text(json.dumps({"best": {str(k): v for k, v in best.items()}, "scenarios": len(scenarios)}, indent=2) + "\n")


if __name__ == "__main__":
    main()
