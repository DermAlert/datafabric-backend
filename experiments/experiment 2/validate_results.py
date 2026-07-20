#!/usr/bin/env python3
"""Validate the internal consistency and article aggregates of Experiment 2."""

from __future__ import annotations

import csv
import math
import statistics
from pathlib import Path


EXPERIMENT_DIR = Path(__file__).resolve().parent
CONCEPTS = ["sex_gender", "diagnosis", "lesion_type", "anatomical_site", "age_group"]
ARMS = ["baseline", "semantic"]
EXPECTED_MEANS = {
    ("baseline", "f1"): 0.8742,
    ("semantic", "f1"): 0.9680,
    ("baseline", "normalization_accuracy"): 0.7482,
    ("semantic", "normalization_accuracy"): 0.9650,
    ("baseline", "manual_source_columns_selected"): 5.2,
    ("semantic", "manual_source_columns_selected"): 1.6,
    ("baseline", "config_time_sec"): 259.8,
    ("semantic", "config_time_sec"): 93.8,
    ("baseline", "execution_time_seconds"): 0.742,
    ("semantic", "execution_time_seconds"): 0.842,
}


def read(name: str) -> list[dict[str, str]]:
    with (EXPERIMENT_DIR / name).open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def close(observed: float, expected: float, tolerance: float = 0.0006) -> bool:
    return math.isclose(observed, expected, abs_tol=tolerance)


def main() -> None:
    main_rows = read("main_graph_table.csv")
    normalization = read("normalization_summary.csv")
    coverage = read("semantic_coverage.csv")
    agreement = read("agreement_table.csv")

    expected_pairs = {(concept, arm) for concept in CONCEPTS for arm in ARMS}
    main_pairs = {(row["concept"], row["arm"]) for row in main_rows}
    require(len(main_rows) == 10 and main_pairs == expected_pairs, "invalid main task set")

    by_pair = {(row["concept"], row["arm"]): row for row in main_rows}
    for pair, row in by_pair.items():
        tp, fp, fn = (int(row[name]) for name in ("tp", "fp", "fn"))
        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        f1 = 2 * precision * recall / (precision + recall)
        require(int(row["returned_total_rows"]) == tp + fp, f"{pair}: returned rows")
        require(int(row["gold_total_rows"]) == tp + fn, f"{pair}: gold rows")
        require(close(float(row["precision"]), precision), f"{pair}: precision")
        require(close(float(row["recall"]), recall), f"{pair}: recall")
        require(close(float(row["f1"]), f1), f"{pair}: f1")

    normalization_pairs = {(row["concept"], row["arm"]) for row in normalization}
    require(len(normalization) == 10 and normalization_pairs == expected_pairs, "normalization set")
    for row in normalization:
        pair = (row["concept"], row["arm"])
        accuracy = int(row["correct_value_cases"]) / int(row["total_value_cases"])
        require(close(float(row["normalization_accuracy"]), accuracy), f"{pair}: normalization")
        require(
            float(row["normalization_accuracy"])
            == float(by_pair[pair]["normalization_accuracy"]),
            f"{pair}: normalization tables disagree",
        )

    require([row["concept"] for row in coverage] == CONCEPTS, "semantic coverage concepts")
    require([row["concept"] for row in agreement] == CONCEPTS, "agreement concepts")
    for row in coverage:
        require(int(row["columns_count"]) > 0, f"{row['concept']}: no semantic columns")
        require(int(row["value_mappings_count"]) > 0, f"{row['concept']}: no value mappings")
    for row in agreement:
        candidates = int(row["candidate_columns"])
        agreements = int(row["agreements"])
        disagreements = int(row["disagreements"])
        require(candidates == agreements + disagreements, f"{row['concept']}: agreement count")
        require(close(float(row["agreement_rate"]), agreements / candidates), f"{row['concept']}: rate")

    for (arm, field), expected in EXPECTED_MEANS.items():
        values = [float(row[field]) for row in main_rows if row["arm"] == arm]
        observed = statistics.mean(values)
        require(math.isclose(observed, expected, abs_tol=1e-9), f"{arm} {field}: {observed}")

    print(
        "Experiment 2 archive: PASS "
        "(10 task/condition pairs, four internally consistent result tables)"
    )


if __name__ == "__main__":
    main()
