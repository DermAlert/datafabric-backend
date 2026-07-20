#!/usr/bin/env python3
"""Validate the archived Experiment 1 snapshot used by the article."""

from __future__ import annotations

import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
EXPERIMENT_DIR = SCRIPT_DIR.parent
SCENARIOS = {"bronze_pushdown", "bronze_federated", "silver_persistent"}
TOPOLOGIES = {1, 2, 4, 8}
EXPECTED_FEDERATED_MEANS = {1: 4.5262, 2: 4.6922, 4: 9.4457, 8: 7.2521}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> None:
    metrics_path = EXPERIMENT_DIR / "run_metrics.csv"
    summary_path = EXPERIMENT_DIR / "summary.json"
    rows = list(csv.DictReader(metrics_path.open(encoding="utf-8", newline="")))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    measured = [row for row in rows if row["is_warmup"].lower() == "false"]
    warmups = [row for row in rows if row["is_warmup"].lower() == "true"]
    require(len(rows) == 420, f"expected 420 total runs, found {len(rows)}")
    require(len(warmups) == 60, f"expected 60 warm-ups, found {len(warmups)}")
    require(len(measured) == 360, f"expected 360 measured runs, found {len(measured)}")

    scenario_counts = Counter((int(row["topology_n"]), row["scenario"]) for row in rows)
    measured_counts = Counter(
        (int(row["topology_n"]), row["scenario"]) for row in measured
    )
    expected_pairs = {(n, scenario) for n in TOPOLOGIES for scenario in SCENARIOS}
    require(set(scenario_counts) == expected_pairs, "unexpected topology/scenario set")
    require(all(value == 35 for value in scenario_counts.values()), "scenario is not 35 runs")
    require(all(value == 30 for value in measured_counts.values()), "scenario is not 30 measured runs")

    failures = [row for row in measured if row["success"].lower() != "true"]
    require(len(failures) == 1, f"expected one measured failure, found {len(failures)}")
    failure = failures[0]
    require(
        int(failure["topology_n"]) == 4 and failure["scenario"] == "bronze_federated",
        "measured failure is not N=4 bronze_federated",
    )

    latencies: dict[tuple[int, str], list[float]] = defaultdict(list)
    for row in measured:
        if row["success"].lower() == "true":
            latencies[(int(row["topology_n"]), row["scenario"])].append(
                float(row["latency_seconds_wall"])
            )

    for topology, expected in EXPECTED_FEDERATED_MEANS.items():
        observed = statistics.mean(latencies[(topology, "bronze_federated")])
        require(
            math.isclose(observed, expected, abs_tol=0.00005),
            f"N={topology} federated mean {observed:.6f} != {expected:.4f}",
        )
        archived = summary["scenarios"][str(topology)]["bronze_federated"]
        require(
            math.isclose(observed, float(archived["latency_mean_s"]), abs_tol=1e-12),
            f"N={topology} raw metrics disagree with summary",
        )

    print(
        "Experiment 1 archive: PASS "
        "(420 total, 360 measured, 12 scenarios, 1 measured failure)"
    )


if __name__ == "__main__":
    main()
