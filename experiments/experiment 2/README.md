# Experiment 2 — Semantic Retrieval and Normalization

This experiment compares two interfaces over the same federated dataset:

- **Baseline:** keyword-oriented retrieval with manual source-column selection;
- **Semantic:** retrieval using concept hierarchies, data-dictionary terms,
  value mappings, and transitive-equivalence groups.

The evaluation covers five predefined concepts: sex/gender, diagnosis, lesion
type, anatomical site, and age group. Each concept has one baseline and one
semantic result, for ten evaluated task/condition pairs.

## Contents

| File | Purpose |
|---|---|
| `main_graph_table.csv` | Main task-level retrieval, normalization, execution-time, and configuration-effort results |
| `normalization_summary.csv` | Correct and total normalized values by concept and condition |
| `semantic_coverage.csv` | Semantic groups, mappings, columns, and source coverage |
| `agreement_table.csv` | Agreement audit for candidate columns associated with each concept |
| `validate_results.py` | Internal-consistency and article-aggregate validation |
| `generate_figure.py` | Deterministic regeneration of the article figure |
| `figures/semantic_evaluation.png` | Figure 8 in PNG format |

These CSV files are the frozen aggregate outputs used by the article. They
support recalculation of the reported metrics without requiring the DataFabric
services.

## Requirements

- Python 3.10 or newer;
- `matplotlib==3.10.8` for figure generation;
- any RFC 4180-compatible CSV reader for manual inspection.

## Installation

From the repository root:

```bash
python3 -m venv .venv-exp2
. .venv-exp2/bin/activate
python -m pip install --upgrade pip
python -m pip install -r "experiments/experiment 2/requirements.txt"
```

## Validate the reported aggregates

```bash
python "experiments/experiment 2/validate_results.py"
```

## Regenerate Figure 8

```bash
python "experiments/experiment 2/generate_figure.py"
```

## Main results

| Metric (mean over five concepts) | Baseline | Semantic |
|---|---:|---:|
| Retrieval F1 | 0.8742 | 0.9680 |
| Normalization accuracy | 0.7482 | 0.9650 |
| Manually selected source columns | 5.2 | 1.6 |
| Configuration time | 259.8 s | 93.8 s |
| Query execution time | 0.742 s | 0.842 s |

The semantic condition improved retrieval F1 and normalization accuracy for
all five concepts while reducing manual configuration effort. Query execution
increased by approximately 0.10 s on average.

## Scope

This artifact supports independent recalculation of the reported aggregate
metrics and regeneration of Figure 8. The original row-level task collection
and labeling process is outside the scope of this package.
