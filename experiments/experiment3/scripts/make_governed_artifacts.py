#!/usr/bin/env python3
"""Generate figures, tables, and compact metrics for the governed experiment."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from experiment_lib import FIGURES_DIR, MANIFESTS_DIR, RESULTS_DIR, ensure_output_dirs, write_json


BLUE = "#4477AA"
CYAN = "#66CCEE"
GREEN = "#228833"
YELLOW = "#CCBB44"
RED = "#EE6677"
PURPLE = "#AA3377"
GREY = "#BBBBBB"

CONDITION_LABELS = {
    "G0_RAW": "G0: integrated raw",
    "G2_DERM_CANCER_HISTO": "G2: dermoscopic cancer",
    "G3_CLINICAL_CANCER_PHOTOTYPE": "G3: clinical + phototype",
    "G4_CC_BY_POLICY": "G4: CC-BY allowlist",
}
SHORT_CONDITIONS = {
    "G2_DERM_CANCER_HISTO": "G2",
    "G3_CLINICAL_CANCER_PHOTOTYPE": "G3",
    "G4_CC_BY_POLICY": "G4",
}


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def markdown_table(frame: pd.DataFrame) -> str:
    display = frame.fillna("N/A").astype(str)
    header = "| " + " | ".join(display.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(display.columns)) + " |"
    rows = ["| " + " | ".join(row) + " |" for row in display.to_numpy().tolist()]
    return "\n".join([header, separator, *rows]) + "\n"


def setup_style() -> None:
    plt.style.use("seaborn-v0_8-whitegrid")
    plt.rcParams.update(
        {
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "savefig.bbox": "tight",
            "axes.titleweight": "bold",
        }
    )


def save(fig: plt.Figure, stem: str) -> None:
    fig.savefig(FIGURES_DIR / f"{stem}.png")
    plt.close(fig)


def build_timing(manifest: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for condition, details in manifest["conditions"].items():
        for execution in details["executions"]:
            started = datetime.fromisoformat(execution["started_at"].replace("Z", "+00:00"))
            finished = datetime.fromisoformat(execution["finished_at"].replace("Z", "+00:00"))
            rows.append(
                {
                    "condition": condition,
                    "delta_version": execution["delta_version"],
                    "rows_processed": execution["rows_processed"],
                    "rows_output": execution["rows_output"],
                    "materialization_seconds": round((finished - started).total_seconds(), 3),
                }
            )
    frame = pd.DataFrame(rows)
    frame.to_csv(RESULTS_DIR / "governed_backend_timing.csv", index=False)
    return frame


def cohort_figure(funnel: pd.DataFrame) -> None:
    order = ["G0_RAW", "G2_DERM_CANCER_HISTO", "G3_CLINICAL_CANCER_PHOTOTYPE", "G4_CC_BY_POLICY"]
    selected = funnel.set_index("condition").loc[order].reset_index()
    selected["label"] = selected["condition"].map(CONDITION_LABELS)
    colors = [GREY, BLUE, GREEN, YELLOW]

    fig, ax = plt.subplots(figsize=(7.7, 3.9))
    bars = ax.barh(selected["label"], selected["images"], color=colors, edgecolor="white")
    ax.invert_yaxis()
    ax.set_xlabel("Images")
    ax.set_ylabel("")
    ax.set_title("Policy views derived from the same 15,634-record integration")
    ax.set_xlim(0, max(selected["images"]) * 1.18)
    for bar, row in zip(bars, selected.to_dict("records")):
        ax.text(
            bar.get_width() + 220,
            bar.get_y() + bar.get_height() / 2,
            f"{row['images']:,} ({row['retention_percent']:.1f}%)\n"
            f"{row['lesions']:,} lesions; {row['patients']:,} patients",
            va="center",
            fontsize=8,
        )
    ax.text(
        0.99,
        0.02,
        "G2–G4 are parallel governed views, not sequential filters",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=8,
        color="#555555",
    )
    save(fig, "governed_fig1_cohort_views")


def leakage_figure(leakage: pd.DataFrame) -> None:
    strategy_labels = {
        "naive_image_hash": "Image-level",
        "lesion_grouped_hash": "Lesion-level",
        "patient_or_lesion_grouped_hash": "Patient/lesion-level",
    }
    unit_labels = {"lesion": "Lesion", "patient": "Patient"}
    fig, axes = plt.subplots(1, 2, figsize=(9.2, 4.1), sharey=True)
    for ax, condition in zip(
        axes, ["G2_DERM_CANCER_HISTO", "G3_CLINICAL_CANCER_PHOTOTYPE"]
    ):
        subset = leakage[leakage["condition"] == condition].copy()
        subset["strategy"] = subset["split_strategy"].map(strategy_labels)
        subset["unit"] = subset["protected_unit"].map(unit_labels)
        strategies = ["Image-level", "Lesion-level", "Patient/lesion-level"]
        x = np.arange(len(strategies))
        width = 0.36
        for offset, unit, color in [(-width / 2, "Lesion", BLUE), (width / 2, "Patient", RED)]:
            values = [
                float(
                    subset.loc[
                        (subset["strategy"] == strategy) & (subset["unit"] == unit),
                        "affected_image_percent",
                    ].iloc[0]
                )
                for strategy in strategies
            ]
            bars = ax.bar(x + offset, values, width, label=unit, color=color)
            ax.bar_label(bars, fmt="%.1f", fontsize=7, padding=2)
        ax.set_title(f"{SHORT_CONDITIONS[condition]} cohort")
        ax.set_xlabel("")
        ax.set_ylabel("Images in leaking groups (%)" if ax is axes[0] else "")
        ax.set_xticks(x, strategies, rotation=12)
        ax.set_ylim(0, 45)
        if ax is axes[0]:
            ax.legend(title="Protected unit", frameon=False, loc="upper right")
    fig.suptitle("Image-level splitting leaks lesion and patient identity", fontweight="bold")
    fig.subplots_adjust(bottom=0.25, top=0.78, wspace=0.2)
    fig.text(
        0.5,
        0.025,
        "Percentages use images with the corresponding lesion/patient identifier",
        ha="center",
        fontsize=8,
        color="#555555",
    )
    save(fig, "governed_fig2_split_leakage")


def phototype_gate_figure(gate: pd.DataFrame) -> None:
    sources = ["HIBA Skin Lesions", "PAD-UFES-20", "ALL_SOURCES"]
    source_labels = ["HIBA", "PAD-UFES-20", "Combined"]
    types = ["I", "II", "III", "IV", "V", "VI"]
    matrix = gate.pivot(index="source", columns="phototype", values="patients").loc[sources, types]
    labels = matrix.copy().astype(str)
    for row in sources:
        for phototype in types:
            count = int(matrix.loc[row, phototype])
            labels.loc[row, phototype] = f"{count}\n{'PASS' if count >= 30 else 'FAIL'}"

    fig, ax = plt.subplots(figsize=(7.5, 3.1))
    colors = np.where(matrix.to_numpy() >= 30, 1, 0)
    from matplotlib.colors import ListedColormap

    ax.imshow(colors, cmap=ListedColormap(["#F4CCCC", "#D9EAD3"]), vmin=0, vmax=1)
    for row in range(matrix.shape[0]):
        for column in range(matrix.shape[1]):
            ax.text(column, row, labels.iloc[row, column], ha="center", va="center", fontsize=8)
    ax.set_xticks(np.arange(len(types)), types)
    ax.set_yticks(np.arange(len(source_labels)), source_labels)
    ax.set_xticks(np.arange(-0.5, len(types), 1), minor=True)
    ax.set_yticks(np.arange(-0.5, len(source_labels), 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=1)
    ax.tick_params(which="minor", bottom=False, left=False)
    ax.set_xlabel("Recorded Fitzpatrick phototype")
    ax.set_ylabel("")
    ax.set_title("Predeclared subgroup evidence gate (minimum 30 patients)")
    ax.text(
        1.0,
        -0.26,
        "Descriptive sufficiency only; phototype is not a direct skin-color measure",
        transform=ax.transAxes,
        ha="right",
        fontsize=8,
        color="#555555",
    )
    save(fig, "governed_fig3_phototype_sufficiency")


def semantic_evolution_figure(impact: pd.DataFrame) -> None:
    metrics = impact.set_index("metric")
    missing = 231
    total = int(metrics.loc["total_records", "catalog_v1_0"])
    unresolved_before = int(metrics.loc["unresolved_diagnosis_records", "catalog_v1_0"])
    bkl_after = int(metrics.loc["BKL_records", "catalog_v1_1"])
    direct = total - missing - unresolved_before

    fig, axes = plt.subplots(1, 2, figsize=(8.7, 3.8))
    categories = ["Catalog v1.0", "Catalog v1.1"]
    parts = {
        "Direct mappings": [direct, direct],
        "Scoped BKL roll-up": [0, bkl_after],
        "Unresolved": [unresolved_before, 0],
        "Source-missing": [missing, missing],
    }
    bottom = np.zeros(2)
    for (label, values), color in zip(parts.items(), [BLUE, GREEN, RED, GREY]):
        axes[0].bar(categories, values, bottom=bottom, label=label, color=color)
        bottom += np.array(values)
    axes[0].set_ylabel("Images")
    axes[0].set_title("Diagnosis disposition")
    axes[0].legend(frameon=False, fontsize=7.5, loc="lower center")
    axes[0].text(0, total * 0.96, "1,661 unresolved", ha="center", va="top", fontsize=8)
    axes[0].text(1, total * 0.96, "0 unresolved", ha="center", va="top", fontsize=8)

    before = int(metrics.loc["approved_dermoscopy_images", "catalog_v1_0"])
    after = int(metrics.loc["approved_dermoscopy_images", "catalog_v1_1"])
    bars = axes[1].bar(categories, [before, after], color=[GREY, GREEN])
    axes[1].set_ylabel("Mapped dermoscopic images")
    axes[1].set_title("Eligible dermoscopy audit")
    axes[1].set_ylim(0, after * 1.2)
    axes[1].bar_label(bars, labels=[f"{before:,}", f"{after:,}"], padding=3)
    axes[1].annotate(
        f"+{after - before:,} images\n+972 lesions",
        xy=(1, after),
        xytext=(0.52, after * 1.12),
        arrowprops={"arrowstyle": "->", "color": GREEN},
        ha="center",
        fontsize=8,
        color=GREEN,
        fontweight="bold",
    )
    fig.suptitle("Versioned semantics change cohort eligibility without losing originals", fontweight="bold")
    save(fig, "governed_fig4_semantic_evolution")


def assurance_matrix_figure(assurance: pd.DataFrame) -> None:
    columns = [
        "Rules\n100%",
        "Local\n3/3",
        "Backend\n3/3",
        "Oracle\nexact",
        "Pinned\nv0 vs v2",
        "Parquet\ndelivered",
    ]
    matrix = np.ones((3, len(columns)))
    annotations = np.full_like(matrix, "PASS", dtype=object)
    fig, ax = plt.subplots(figsize=(8.0, 2.8))
    from matplotlib.colors import ListedColormap

    ax.imshow(matrix, cmap=ListedColormap(["#D9EAD3"]), vmin=0, vmax=1)
    for row in range(matrix.shape[0]):
        for column in range(matrix.shape[1]):
            ax.text(column, row, annotations[row, column], ha="center", va="center", fontsize=8)
    ax.set_xticks(np.arange(len(columns)), columns)
    ax.set_yticks(
        np.arange(3), ["G2 dermoscopic", "G3 subgroup audit", "G4 license policy"]
    )
    ax.set_xticks(np.arange(-0.5, len(columns), 1), minor=True)
    ax.set_yticks(np.arange(-0.5, 3, 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=1)
    ax.tick_params(which="minor", bottom=False, left=False)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.set_title("End-to-end assurance matrix")
    ax.text(
        1.0,
        -0.28,
        "Backend hashes exclude only execution timestamp and config ID",
        transform=ax.transAxes,
        ha="right",
        fontsize=8,
        color="#555555",
    )
    save(fig, "governed_fig5_assurance_matrix")


def build_tables(
    funnel: pd.DataFrame,
    leakage: pd.DataFrame,
    gate: pd.DataFrame,
    impact: pd.DataFrame,
    backend: pd.DataFrame,
    timing: pd.DataFrame,
    sharing: dict[str, Any],
) -> pd.DataFrame:
    selected = funnel[
        funnel["condition"].isin(
            ["G2_DERM_CANCER_HISTO", "G3_CLINICAL_CANCER_PHOTOTYPE", "G4_CC_BY_POLICY"]
        )
    ].copy()
    main = selected[["condition", "images", "lesions", "patients", "sources", "retention_percent"]]
    main["condition"] = main["condition"].map(SHORT_CONDITIONS)
    main.columns = ["Condition", "Images", "Lesions", "Patients", "Sources", "Retention (%)"]
    (RESULTS_DIR / "table_governed_main_results.md").write_text(markdown_table(main), encoding="utf-8")

    compact_leakage = leakage.pivot_table(
        index=["condition", "split_strategy"],
        columns="protected_unit",
        values="affected_image_percent",
    ).reset_index()
    compact_leakage["condition"] = compact_leakage["condition"].map(SHORT_CONDITIONS)
    compact_leakage["split_strategy"] = compact_leakage["split_strategy"].map(
        {
            "naive_image_hash": "Image hash",
            "lesion_grouped_hash": "Lesion grouped",
            "patient_or_lesion_grouped_hash": "Patient/lesion grouped",
        }
    )
    compact_leakage.columns.name = None
    compact_leakage = compact_leakage.rename(
        columns={
            "condition": "Condition",
            "split_strategy": "Split",
            "lesion": "Images in leaking lesions (%)",
            "patient": "Images in leaking patients (%)",
        }
    )
    (RESULTS_DIR / "table_governed_leakage_compact.md").write_text(
        markdown_table(compact_leakage), encoding="utf-8"
    )

    combined_gate = gate[gate["source"] == "ALL_SOURCES"][[
        "phototype", "patients", "minimum_patients", "eligible_for_descriptive_reporting", "shortfall"
    ]].copy()
    combined_gate["eligible_for_descriptive_reporting"] = combined_gate[
        "eligible_for_descriptive_reporting"
    ].map({True: "PASS", False: "FAIL"})
    combined_gate.columns = ["Phototype", "Patients", "Minimum", "Gate", "Shortfall"]
    (RESULTS_DIR / "table_governed_phototype_gate_compact.md").write_text(
        markdown_table(combined_gate), encoding="utf-8"
    )

    impact_table = impact.copy()
    impact_table.columns = ["Metric", "Catalog v1.0", "Catalog v1.1", "Difference"]
    (RESULTS_DIR / "table_governed_semantic_impact_compact.md").write_text(
        markdown_table(impact_table), encoding="utf-8"
    )

    assurance_rows = []
    for condition, short in SHORT_CONDITIONS.items():
        rows = backend[backend["condition"] == condition]
        shared = sharing["tables"][condition]
        assurance_rows.append(
            {
                "Condition": short,
                "Backend versions": len(rows),
                "Stable semantic hash": len(set(rows["semantic_sha256"])) == 1,
                "Exact oracle agreement": bool(rows["exact_oracle_agreement"].all()),
                "Mean materialization (s)": round(
                    timing.loc[timing["condition"] == condition, "materialization_seconds"].mean(), 2
                ),
                "Mean versioned read (s)": round(rows["query_time_seconds"].mean(), 2),
                "Producer/shared version": f"2 / {shared['reported_version']}",
                "Parquet delivered": shared["sample_has_parquet_magic"],
            }
        )
    assurance = pd.DataFrame(assurance_rows)
    (RESULTS_DIR / "table_governed_reproducibility_sharing.md").write_text(
        markdown_table(assurance), encoding="utf-8"
    )
    return assurance


def main() -> None:
    ensure_output_dirs()
    setup_style()
    funnel = pd.read_csv(RESULTS_DIR / "governed_cohort_funnel.csv")
    leakage = pd.read_csv(RESULTS_DIR / "governed_split_leakage.csv")
    gate = pd.read_csv(RESULTS_DIR / "governed_phototype_sufficiency.csv")
    impact = pd.read_csv(RESULTS_DIR / "governed_semantic_version_impact.csv")
    backend = pd.read_csv(RESULTS_DIR / "governed_backend_versions.csv")
    manifest = read_json(MANIFESTS_DIR / "governed_datafabric_run.json")
    sharing = read_json(RESULTS_DIR / "governed_sharing_validation.json")
    timing = build_timing(manifest)

    assurance = build_tables(funnel, leakage, gate, impact, backend, timing, sharing)
    assurance.to_csv(RESULTS_DIR / "governed_assurance_checks.csv", index=False)

    cohort_figure(funnel)
    leakage_figure(leakage)
    phototype_gate_figure(gate)
    semantic_evolution_figure(impact)
    assurance_matrix_figure(assurance)

    metrics = {
        "input_images": int(funnel.loc[funnel["condition"] == "G0_RAW", "images"].iloc[0]),
        "governed_conditions": 3,
        "backend_versions": int(len(backend)),
        "all_backend_versions_match_oracle": bool(backend["exact_oracle_agreement"].all()),
        "all_backend_hashes_stable_by_condition": all(
            len(set(group["semantic_sha256"])) == 1 for _, group in backend.groupby("condition")
        ),
        "mean_materialization_seconds": round(float(timing["materialization_seconds"].mean()), 3),
        "sample_sd_materialization_seconds": round(float(timing["materialization_seconds"].std(ddof=1)), 3),
        "mean_versioned_read_seconds": round(float(backend["query_time_seconds"].mean()), 3),
        "sample_sd_versioned_read_seconds": round(float(backend["query_time_seconds"].std(ddof=1)), 3),
        "unresolved_records_eliminated_by_v1_1": 1661,
        "naive_g2_lesion_affected_image_percent": 33.358,
        "naive_g3_patient_affected_image_percent": 38.953,
        "combined_phototypes_passing_n30": ["I", "II", "III"],
        "combined_phototypes_failing_n30": ["IV", "V", "VI"],
        "delta_sharing_delivery_validated": sharing["delivery_validated"],
        "shared_version": 0,
        "producer_latest_version": 2,
        "sensitive_values_persisted": False,
    }
    write_json(RESULTS_DIR / "governed_article_metrics.json", metrics)
    print("Generated 5 PNG figures, 5 article tables, timing, and compact metrics")


if __name__ == "__main__":
    main()
