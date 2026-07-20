# Experiment 1 — Federated Query Performance

This experiment evaluates DataFabric under controlled federated topologies
with 1, 2, 4, and 8 registered data connections. It compares three workloads:

- **Bronze pushdown:** source-local reads with filtering close to the source;
- **Bronze federated:** federated reads across the selected connections;
- **Silver persistent:** materialization of the curated result.

Each topology/workload pair was executed 35 times. The first five executions
were treated as warm-ups, leaving 30 measured runs per scenario: 420 total
executions and 360 measured executions.

## Contents

| Path | Purpose |
|---|---|
| `run_metrics.csv` | One record per execution, including warm-up status, latency, rows, storage, CPU, memory, and failures |
| `summary.json` | Aggregated statistics by topology and workload |
| `final_report.md` | Consolidated protocol, result table, resource limits, and observations |
| `artifacts/executions/` | DataFabric execution responses for the 12 scenarios |
| `artifacts/previews/` | Representative output previews |
| `artifacts/topologies.json` | Frozen connection composition for each topology |
| `artifacts/workloads.json` | Frozen Bronze and Silver configurations |
| `artifacts/manual_inter_relationships.json` | Cross-source relationships registered for the run |
| `figures/` | Five PNG figures regenerated from the archived summary |
| `scripts/` | Public validation, plotting, portable runner, and SQL fixtures |

Connection snapshots are archival evidence. Environment-bound encrypted
values inside them are not reusable credentials.

## Requirements

- Linux with Docker Engine and Docker Compose v2;
- Python 3.10 or newer;
- a working DataFabric stack from the repository root;
- Python dependencies listed in `scripts/requirements.txt`;
- enough memory for the resource limits recorded in `summary.json` (at least
  16 GB of host RAM is recommended for the complete local deployment).

## Installation

From the repository root:

```bash
python3 -m venv .venv-exp1
. .venv-exp1/bin/activate
python -m pip install --upgrade pip
python -m pip install -r "experiments/experiment 1/scripts/requirements.txt"
```

## Validate and regenerate the article results

The archived article snapshot can be validated and its PNG figures regenerated
without starting Docker:

```bash
python "experiments/experiment 1/scripts/validate_exp1_results.py"
python "experiments/experiment 1/scripts/generate_exp1_charts.py"
```

The archived article run is identified as `exp1_20260315_v2`. Its selected
outputs were copied into this directory so the reported values can be checked
without rerunning the container stack.

## Execute the portable replication profile

The artifact includes eight synthetic PostgreSQL/MySQL sources and a portable
runner for the same N={1,2,4,8}, three-workload, 35-run protocol:

```bash
DATAFABRIC_EXP1_RUN_ID=exp1_artifact_replication \
  python "experiments/experiment 1/scripts/run_exp1_sql_quick.py"
```

This portable profile replaces the institution-specific source projects used
by the historical run. It reproduces the experimental protocol and exercises
the same DataFabric operations, but new latency values are not expected to be
identical across hardware or source deployments.

## Validate the archived run

From the repository root:

```bash
python "experiments/experiment 1/scripts/validate_exp1_results.py"
```

## Main results

Bronze federated mean latency was 4.53 s at N=1, 4.69 s at N=2,
9.45 s at N=4, and 7.25 s at N=8. The only measured failure occurred
for Bronze federated at N=4, corresponding to 3.33% of that scenario.
Bronze pushdown and Silver persistent became more expensive as the number of
connections increased. Full means and P95 values are available in
`final_report.md` and `summary.json`.

## Scope

This artifact supports validation of the reported small-scale reliability and
engineering-performance results. Measurements reflect the tested deployment;
the portable profile may produce different latency and resource values across
hardware and source environments.
