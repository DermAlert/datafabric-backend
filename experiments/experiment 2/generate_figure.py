#!/usr/bin/env python3
"""Regenerate the Experiment 2 article figure from main_graph_table.csv."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


EXPERIMENT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = EXPERIMENT_DIR / "main_graph_table.csv"
DEFAULT_OUTPUT = EXPERIMENT_DIR / "figures" / "semantic_evaluation.png"
CONCEPTS = ["sex_gender", "diagnosis", "lesion_type", "anatomical_site", "age_group"]
LABELS = ["Sex/gender", "Diagnosis", "Lesion type", "Anatomical site", "Age group"]
ARMS = ["baseline", "semantic"]
COLORS = {"baseline": "#1f77b4", "semantic": "#ff7f0e"}


def load(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    indexed = {(row["concept"], row["arm"]): row for row in rows}
    expected = {(concept, arm) for concept in CONCEPTS for arm in ARMS}
    if len(rows) != 10 or set(indexed) != expected:
        raise ValueError("main_graph_table.csv must contain five concepts and two arms")
    return indexed


def values(
    indexed: dict[tuple[str, str], dict[str, str]], arm: str, field: str
) -> list[float]:
    return [float(indexed[(concept, arm)][field]) for concept in CONCEPTS]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    indexed = load(args.input)
    x = np.arange(len(CONCEPTS))
    width = 0.36
    fig, axes = plt.subplots(1, 3, figsize=(20.48, 6.59), dpi=100)
    panels = [
        ("f1", "F1 by concept", "F1", (0.75, 1.02)),
        (
            "normalization_accuracy",
            "Normalization accuracy",
            "Accuracy",
            (0.60, 1.02),
        ),
        ("config_time_sec", "Configuration time", "Time (s)", (0, 380)),
    ]

    for ax, (field, title, ylabel, ylim) in zip(axes, panels):
        for index, arm in enumerate(ARMS):
            offset = (index - 0.5) * width
            ax.bar(
                x + offset,
                values(indexed, arm, field),
                width,
                color=COLORS[arm],
                label=arm.title(),
            )
        ax.set_title(title, fontsize=20, fontweight="bold")
        ax.set_ylabel(ylabel, fontsize=14)
        ax.set_xticks(x, LABELS, rotation=25, ha="right")
        ax.set_ylim(*ylim)
        ax.grid(axis="y", alpha=0.28)
        ax.tick_params(axis="both", labelsize=13)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncols=2,
        frameon=False,
        fontsize=14,
        bbox_to_anchor=(0.5, 0.015),
    )
    fig.subplots_adjust(left=0.05, right=0.99, top=0.92, bottom=0.24, wspace=0.16)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=100)
    plt.close(fig)
    print(f"Generated {args.output}")


if __name__ == "__main__":
    main()
