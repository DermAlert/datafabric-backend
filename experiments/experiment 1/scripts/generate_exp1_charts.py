from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = SCRIPT_DIR.parent


SCENARIO_ORDER = ["bronze_pushdown", "bronze_federated", "silver_persistent"]
SCENARIO_LABELS = {
    "bronze_pushdown": "Bronze Pushdown",
    "bronze_federated": "Bronze Federated",
    "silver_persistent": "Persistent Silver",
}
SCENARIO_COLORS = {
    "bronze_pushdown": "#C65D2E",
    "bronze_federated": "#2F7D6D",
    "silver_persistent": "#244C8A",
}

ARTICLE_SCENARIO_ORDER = [
    "bronze_federated",
    "bronze_pushdown",
    "silver_persistent",
]
ARTICLE_SCENARIO_COLORS = {
    "bronze_federated": "#2374AB",
    "bronze_pushdown": "#F28E2B",
    "silver_persistent": "#2BAE66",
}
ARTICLE_SCENARIO_LABELS = {
    "bronze_federated": "Bronze federated",
    "bronze_pushdown": "Bronze pushdown",
    "silver_persistent": "Silver persistent",
}


def load_summary(path: Path) -> dict:
    return json.loads(path.read_text())


def ordered_topologies(summary: dict) -> list[int]:
    return sorted(int(key) for key in summary["scenarios"].keys())


def metric_points(summary: dict, scenario: str, metric: str) -> list[float | None]:
    values: list[float | None] = []
    for topology in ordered_topologies(summary):
        value = summary["scenarios"][str(topology)][scenario][metric]
        values.append(float(value) if value is not None else None)
    return values


def safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def percent_delta(start: float | None, end: float | None) -> float | None:
    if start in (None, 0) or end is None:
        return None
    return ((end - start) / start) * 100.0


def fmt_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}"


def fmt_percent(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "N/A"
    return f"{value:.{digits}f}%"


def setup_axes_grid(fig_title: str, ncols: int = 2):
    fig, axes = plt.subplots(1, ncols, figsize=(14, 5.8), constrained_layout=True)
    fig.suptitle(fig_title, fontsize=16, fontweight="bold")
    return fig, axes


def style_axis(ax, ylabel: str):
    ax.set_xlabel("Topology (N connections)")
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.25, linestyle="--")
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)


def plot_lines(ax, x_values: list[int], y_values: list[float | None], scenario: str, marker: str = "o"):
    xs = [x for x, y in zip(x_values, y_values) if y is not None]
    ys = [y for y in y_values if y is not None]
    ax.plot(
        xs,
        ys,
        marker=marker,
        linewidth=2.4,
        markersize=7,
        color=SCENARIO_COLORS[scenario],
        label=SCENARIO_LABELS[scenario],
    )


def save_figure(fig, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def add_shared_legend(fig, scenarios: list[str] | None = None, ncols: int | None = None):
    selected = scenarios or SCENARIO_ORDER
    handles = [
        plt.Line2D(
            [],
            [],
            color=SCENARIO_COLORS[scenario],
            marker="o",
            linewidth=2.4,
            markersize=7,
            label=SCENARIO_LABELS[scenario],
        )
        for scenario in selected
    ]
    fig.legend(
        handles=handles,
        labels=[handle.get_label() for handle in handles],
        loc="upper center",
        ncols=ncols or len(selected),
        frameon=False,
        bbox_to_anchor=(0.5, 1.02),
    )


def chart_article_performance(summary: dict, out_dir: Path) -> None:
    """Regenerate Figure 7 using the archived Experiment 1 summary."""
    topologies = ordered_topologies(summary)
    panels = [
        ("latency_mean_s", "Mean latency by workload", "Latency (s)"),
        ("rows_processed_mean", "Rows processed", "Rows"),
        ("cpu_avg_percent_mean", "Mean CPU usage", "CPU (%)"),
    ]

    # The fixed canvas and subplot geometry reproduce the 2048 x 640 article
    # layout while keeping every plotted value sourced from summary.json.
    with plt.style.context("default"):
        fig, axes = plt.subplots(1, 3, figsize=(20.48, 6.4), dpi=100)
        fig.patch.set_facecolor("white")
        fig.subplots_adjust(
            left=0.0375,
            right=0.994,
            top=0.935,
            bottom=0.198,
            wspace=0.17,
        )

        for ax, (metric, title, ylabel) in zip(axes, panels):
            ax.set_facecolor("white")
            for scenario in ARTICLE_SCENARIO_ORDER:
                values = metric_points(summary, scenario, metric)
                ax.plot(
                    topologies,
                    values,
                    color=ARTICLE_SCENARIO_COLORS[scenario],
                    marker="o",
                    linewidth=3.0,
                    markersize=9.0,
                    label=ARTICLE_SCENARIO_LABELS[scenario],
                )

            ax.set_title(title, fontsize=17, fontweight="bold", pad=10)
            ax.set_xlabel("Remote connections (N)", fontsize=14)
            ax.set_ylabel(ylabel, fontsize=14)
            ax.set_xticks(topologies)
            ax.tick_params(axis="both", labelsize=14, width=1.0)
            ax.grid(True, color="#B0B0B0", alpha=0.30, linewidth=1.0)
            ax.set_axisbelow(True)

            for spine in ax.spines.values():
                spine.set_color("black")
                spine.set_linewidth(1.0)

        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(
            handles,
            labels,
            loc="lower center",
            bbox_to_anchor=(0.5, 0.015),
            ncol=3,
            frameon=False,
            fontsize=14,
            handlelength=2.0,
            columnspacing=2.0,
        )

        output_path = out_dir / "federated_performance.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=100, facecolor="white")
        plt.close(fig)


def chart_latency(summary: dict, out_dir: Path) -> None:
    topologies = ordered_topologies(summary)
    fig, axes = setup_axes_grid("Latency and Tail Behavior")

    ax_mean, ax_p95 = axes
    for scenario in SCENARIO_ORDER:
        plot_lines(ax_mean, topologies, metric_points(summary, scenario, "latency_mean_s"), scenario)
        plot_lines(ax_p95, topologies, metric_points(summary, scenario, "latency_p95_s"), scenario)

    style_axis(ax_mean, "Mean latency (s)")
    style_axis(ax_p95, "P95 (s)")
    ax_mean.set_title("Mean latency by topology")
    ax_p95.set_title("P95 latency by topology")
    add_shared_legend(fig)
    save_figure(fig, out_dir / "01_latency_scaling.png")


def chart_efficiency(summary: dict, out_dir: Path) -> None:
    topologies = ordered_topologies(summary)
    fig, axes = setup_axes_grid("Efficiency and Selectivity")
    ax_throughput, ax_selectivity = axes

    for scenario in SCENARIO_ORDER:
        throughput = [
            safe_div(
                summary["scenarios"][str(n)][scenario]["rows_output_mean"],
                summary["scenarios"][str(n)][scenario]["latency_mean_s"],
            )
            for n in topologies
        ]
        selectivity = [
            safe_div(
                summary["scenarios"][str(n)][scenario]["rows_output_mean"],
                summary["scenarios"][str(n)][scenario]["rows_processed_mean"],
            )
            for n in topologies
        ]
        plot_lines(ax_throughput, topologies, throughput, scenario)
        plot_lines(ax_selectivity, topologies, selectivity, scenario)

    style_axis(ax_throughput, "Output rows per second")
    style_axis(ax_selectivity, "Output rows / processed rows")
    ax_throughput.set_title("Effective throughput")
    ax_selectivity.set_title("Pipeline selectivity")
    ax_selectivity.set_ylim(bottom=0)
    add_shared_legend(fig)
    save_figure(fig, out_dir / "02_efficiency_selectivity.png")


def chart_storage(summary: dict, out_dir: Path) -> None:
    topologies = ordered_topologies(summary)
    bronze_only = ["bronze_pushdown", "bronze_federated"]
    fig, axes = setup_axes_grid("Bronze Persistence Footprint")
    ax_size, ax_files = axes

    for scenario in bronze_only:
        size_kib = [safe_div(v, 1024.0) for v in metric_points(summary, scenario, "size_bytes_mean")]
        files = metric_points(summary, scenario, "num_files_mean")
        plot_lines(ax_size, topologies, size_kib, scenario)
        plot_lines(ax_files, topologies, files, scenario)

    style_axis(ax_size, "Mean persisted size (KiB)")
    style_axis(ax_files, "Mean number of files")
    ax_size.set_title("Persisted artifact size")
    ax_files.set_title("File fragmentation")
    add_shared_legend(fig, scenarios=bronze_only, ncols=2)
    save_figure(fig, out_dir / "03_storage_footprint.png")


def chart_resources(summary: dict, out_dir: Path) -> None:
    topologies = ordered_topologies(summary)
    fig, axes = setup_axes_grid("Aggregate Resource Consumption")
    ax_cpu, ax_mem = axes

    for scenario in SCENARIO_ORDER:
        cpu = metric_points(summary, scenario, "cpu_avg_percent_mean")
        mem_gib = [safe_div(v, 1024.0**3) for v in metric_points(summary, scenario, "mem_avg_bytes_mean")]
        plot_lines(ax_cpu, topologies, cpu, scenario)
        plot_lines(ax_mem, topologies, mem_gib, scenario)

    style_axis(ax_cpu, "Mean aggregate CPU (%)")
    style_axis(ax_mem, "Mean aggregate memory (GiB)")
    ax_cpu.set_title("CPU observed during measured runs")
    ax_mem.set_title("Memory observed during measured runs")
    add_shared_legend(fig)
    save_figure(fig, out_dir / "04_resource_profile.png")


def chart_stability(summary: dict, out_dir: Path) -> None:
    topologies = ordered_topologies(summary)
    fig, axes = setup_axes_grid("Stability and Tail Risk")
    ax_failure, ax_tail = axes

    for scenario in SCENARIO_ORDER:
        failure = [metric * 100.0 for metric in metric_points(summary, scenario, "failure_rate")]
        tail_ratio = [
            safe_div(
                summary["scenarios"][str(n)][scenario]["latency_p95_s"],
                summary["scenarios"][str(n)][scenario]["latency_mean_s"],
            )
            for n in topologies
        ]
        plot_lines(ax_failure, topologies, failure, scenario)
        plot_lines(ax_tail, topologies, tail_ratio, scenario)

    style_axis(ax_failure, "Failure rate (%)")
    style_axis(ax_tail, "P95 / mean")
    ax_failure.set_title("Measured failures")
    ax_tail.set_title("Tail amplification")
    ax_failure.set_ylim(bottom=0)
    add_shared_legend(fig)
    save_figure(fig, out_dir / "05_stability_tail_risk.png")


def fastest_scenario_by_topology(summary: dict) -> list[str]:
    findings: list[str] = []
    for topology in ordered_topologies(summary):
        best_scenario = min(
            SCENARIO_ORDER,
            key=lambda scenario: summary["scenarios"][str(topology)][scenario]["latency_mean_s"],
        )
        latency = summary["scenarios"][str(topology)][best_scenario]["latency_mean_s"]
        findings.append(f"N={topology}: {SCENARIO_LABELS[best_scenario]} ({latency:.2f}s)")
    return findings


def build_analysis(summary: dict) -> str:
    topologies = ordered_topologies(summary)
    bp = summary["scenarios"]

    scaling_pushdown = percent_delta(
        bp["1"]["bronze_pushdown"]["latency_mean_s"],
        bp["8"]["bronze_pushdown"]["latency_mean_s"],
    )
    scaling_federated = percent_delta(
        bp["1"]["bronze_federated"]["latency_mean_s"],
        bp["8"]["bronze_federated"]["latency_mean_s"],
    )
    scaling_silver = percent_delta(
        bp["1"]["silver_persistent"]["latency_mean_s"],
        bp["8"]["silver_persistent"]["latency_mean_s"],
    )

    pushdown_files_delta = percent_delta(
        bp["1"]["bronze_pushdown"]["num_files_mean"],
        bp["8"]["bronze_pushdown"]["num_files_mean"],
    )
    federated_files_n8 = bp["8"]["bronze_federated"]["num_files_mean"]

    silver_selectivity_n4 = safe_div(
        bp["4"]["silver_persistent"]["rows_output_mean"],
        bp["4"]["silver_persistent"]["rows_processed_mean"],
    )
    silver_selectivity_n8 = safe_div(
        bp["8"]["silver_persistent"]["rows_output_mean"],
        bp["8"]["silver_persistent"]["rows_processed_mean"],
    )

    bronze_fed_failure = bp["4"]["bronze_federated"]["failure_rate"] * 100.0
    bronze_fed_tail_n4 = safe_div(
        bp["4"]["bronze_federated"]["latency_p95_s"],
        bp["4"]["bronze_federated"]["latency_mean_s"],
    )

    peak_cpu_scenario = max(
        (
            (
                topology,
                scenario,
                bp[str(topology)][scenario]["cpu_avg_percent_mean"],
            )
            for topology in topologies
            for scenario in SCENARIO_ORDER
        ),
        key=lambda item: item[2],
    )
    peak_mem_scenario = max(
        (
            (
                topology,
                scenario,
                bp[str(topology)][scenario]["mem_avg_bytes_mean"],
            )
            for topology in topologies
            for scenario in SCENARIO_ORDER
        ),
        key=lambda item: item[2],
    )

    fastest_lines = "\n".join(f"- {line}" for line in fastest_scenario_by_topology(summary))

    return f"""# Chart Analysis - {summary['run_id']}

Source: `summary.json` and `run_metrics.csv` from this run directory.

## Quick Read

- Fastest workloads by topology:
{fastest_lines}
- `Bronze Federated` delivered the best overall performance balance.
- `Bronze Pushdown` degraded the most as topology size increased.
- `Persistent Silver` remained functional after the join fix, but with a higher materialization cost.

## Chart 1 - Latency and Tail Behavior

![Grafico 1](figures/01_latency_scaling.png)

What it shows:
- The left panel shows mean latency by topology.
- The right panel shows P95 latency, highlighting tail behavior.

Insights:
- `Bronze Federated` moved from `4.53s` at `N=1` to `7.25s` at `N=8`, a `{fmt_percent(scaling_federated)}` increase.
- `Bronze Pushdown` rose from `5.05s` to `32.65s` between `N=1` and `N=8`, a `{fmt_percent(scaling_pushdown)}` increase, far worse than the federated path.
- `Persistent Silver` grew from `7.09s` to `25.25s` over the same interval, a `{fmt_percent(scaling_silver)}` increase, consistent with the extra read, join, and write cost.
- At `N=8`, `Bronze Federated` still delivered the best mean latency, showing that the federated path outperformed pushdown persistence in this source mesh.
- At `N=4`, `Bronze Federated` had a mean (`9.45s`) much higher than its median (`5.28s`), another sign of an outlier or transient instability.

Interpretation:
- The key finding is not just who is faster, but who scales with the smallest penalty. By that criterion, `Bronze Federated` stands out.

## Chart 2 - Efficiency and Selectivity

![Grafico 2](figures/02_efficiency_selectivity.png)

What it shows:
- The left panel shows effective throughput in `output rows per second`.
- The right panel shows selectivity, that is, the ratio `rows_output / rows_processed`.

Insights:
- `Bronze Federated` kept relatively stable throughput because its output row count stayed nearly flat across topologies.
- `Persistent Silver` shows a strong selectivity drop as topology size grows: `{fmt_percent((silver_selectivity_n4 or 0) * 100.0)}` at `N=4` and `{fmt_percent((silver_selectivity_n8 or 0) * 100.0)}` at `N=8`.
- This suggests that Silver is acting as an integration and curation layer rather than a simple replication pipeline. It processes more rows but emits a more consolidated final dataset.

Interpretation:
- This chart prevents an unfair reading of latency. A workload may be slower because it is doing more useful integration work, not necessarily because it is less efficient.

## Chart 3 - Bronze Persistence Footprint

![Grafico 3](figures/03_storage_footprint.png)

What it shows:
- Mean persisted size for Bronze workloads.
- Mean number of files generated by each topology.

Insights:
- `Bronze Pushdown` increased its mean file count from `1` to `8` between `N=1` and `N=8`, a `{fmt_percent(pushdown_files_delta)}` increase.
- At the same `N=8`, `Bronze Federated` remained at `{fmt_number(federated_files_n8, 0)}` mean file even while persisted size increased.
- Persisted artifact size grows in both workloads, but fragmentation grows much more sharply in `Pushdown`.

Interpretation:
- The cost of pushdown is not only latency. It also spreads output across more files, which can hurt future reads, metadata operations, and table maintenance.

## Chart 4 - Aggregate Resource Consumption

![Grafico 4](figures/04_resource_profile.png)

What it shows:
- Mean aggregate CPU across all monitored containers.
- Mean aggregate memory observed during measured runs.

Insights:
- The peak mean CPU appeared at `N={peak_cpu_scenario[0]}` in `{SCENARIO_LABELS[peak_cpu_scenario[1]]}`, at `{fmt_number(peak_cpu_scenario[2])}%`.
- The peak mean memory appeared at `N={peak_mem_scenario[0]}` in `{SCENARIO_LABELS[peak_mem_scenario[1]]}`, at `{fmt_number(peak_mem_scenario[2] / (1024.0**3))} GiB`.
- At larger topologies, all workloads converge toward a high aggregate resource band, but `Persistent Silver` tends to concentrate the largest peaks because it materializes final output.

Interpretation:
- Performance elasticity cannot be read without resource cost. Here, the advantage of `Bronze Federated` matters because it happens without a proportional memory jump like the one seen in `Pushdown`.

## Chart 5 - Stability and Tail Risk

![Grafico 5](figures/05_stability_tail_risk.png)

What it shows:
- Measured failure rate for each workload.
- The ratio `P95 / mean`, used as a simple indicator of tail behavior and variability.

Insights:
- There was only one measured failure in the entire `v2` run: `Bronze Federated` at `N=4`, equivalent to `{fmt_percent(bronze_fed_failure)}`.
- Even so, the main instability signal was not the failure rate itself but the tail: at `N=4`, `Bronze Federated` reached `P95/mean = {fmt_number(bronze_fed_tail_n4)}`.
- This suggests a meaningful transient event or outlier at `N=4`. That is an inference from the observed distribution; the API did not return an `error_message` for the single recorded failure.

Interpretation:
- For academic and engineering discussion, this chart matters because it separates “fails often” from “varies a lot.” Even with near-zero failure rate, a workload can still have a problematic tail.

## Practical Conclusions

- If the priority is lower latency with multiple connections, `Bronze Federated` is the strongest candidate in this run.
- If the priority is to materialize a more integrated and curated result, `Persistent Silver` becomes reasonable, as long as the extra cost is acceptable.
- `Bronze Pushdown` became less attractive as topology size grew, both in latency and in persisted artifact fragmentation.
- `N=4` deserves extra monitoring in `Bronze Federated` because of the single failure and widened tail, even though the overall `v2` outcome still favors that workload.

## Reading Limits

- `size_bytes` and `num_files` for `Persistent Silver` are still unavailable in this API, so the persistence charts include Bronze workloads only.
- CPU and memory are aggregate metrics across monitored containers, not metrics from a single isolated service.
- The interpretation of a transient event in `N=4 Bronze Federated` is an inference from measured data, not a confirmed root cause.
"""


def write_analysis(summary: dict, out_dir: Path) -> None:
    analysis_path = out_dir.parent / "figures_analysis.md"
    analysis_path.write_text(build_analysis(summary))


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera graficos e analise para um resultado do experimento 1.")
    parser.add_argument(
        "--results-dir",
        default=str(DEFAULT_RESULTS_DIR),
        help="Diretorio com summary.json e run_metrics.csv",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir).resolve()
    summary_path = results_dir / "summary.json"
    if not summary_path.exists():
        raise SystemExit(f"summary.json nao encontrado em {results_dir}")

    summary = load_summary(summary_path)
    figures_dir = results_dir / "figures"

    plt.style.use("tableau-colorblind10")
    chart_article_performance(summary, figures_dir)
    chart_latency(summary, figures_dir)
    chart_efficiency(summary, figures_dir)
    chart_storage(summary, figures_dir)
    chart_resources(summary, figures_dir)
    chart_stability(summary, figures_dir)
    write_analysis(summary, figures_dir)

    print(f"Graficos gerados em {figures_dir}")
    print(f"Analise gerada em {results_dir / 'figures_analysis.md'}")


if __name__ == "__main__":
    main()
