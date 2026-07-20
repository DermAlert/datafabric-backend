# Experiment 1 Scripts

This directory contains the public scripts used to validate and visualize the
article snapshot and to execute a portable local replication profile.

## Public files

- `validate_exp1_results.py`: validates the archived article metrics;
- `generate_exp1_charts.py`: regenerates five PNG figures from `summary.json`;
- `run_exp1_sql_quick.py`: runs the same topology/workload protocol against
  the bundled synthetic SQL source stack;
- `sql/`: Docker Compose and deterministic SQL fixtures for eight local
  PostgreSQL/MySQL sources;
- `requirements.txt`: pinned Python dependencies.

The historical `run_exp1.py` runner is not part of the public artifact because
it depends on institution-specific sibling repositories and absolute host
paths. The portable SQL runner replaces those dependencies for artifact use.

## Install

From the repository root:

```bash
python3 -m venv .venv-exp1
. .venv-exp1/bin/activate
python -m pip install --upgrade pip
python -m pip install -r "experiments/experiment 1/scripts/requirements.txt"
```

## Validate and regenerate figures

```bash
python "experiments/experiment 1/scripts/validate_exp1_results.py"
python "experiments/experiment 1/scripts/generate_exp1_charts.py"
```

## Portable execution

The complete protocol uses 35 runs per scenario and discards five warm-ups:

```bash
DATAFABRIC_EXP1_RUN_ID=exp1_artifact_replication \
  python "experiments/experiment 1/scripts/run_exp1_sql_quick.py"
```

For a short installation test:

```bash
DATAFABRIC_EXP1_RUN_ID=exp1_smoke \
DATAFABRIC_EXP1_TOPOLOGIES=1 \
DATAFABRIC_EXP1_SCENARIOS=bronze_federated \
DATAFABRIC_EXP1_TOTAL_RUNS=2 \
DATAFABRIC_EXP1_WARMUPS=1 \
  python "experiments/experiment 1/scripts/run_exp1_sql_quick.py"
```

The runner creates its outputs under
`experiments/experiment 1/scripts/results/<run-id>/`.
