# Experiment 3 — Governed Dermatology Data Integration

This artifact evaluates DataFabric with three public dermatology metadata
collections: HAM10000, HIBA Skin Lesions, and PAD-UFES-20. It integrates
15,634 records and derives governed cohorts for diagnostic evidence,
patient/lesion-safe splitting, recorded Fitzpatrick phototype sufficiency,
semantic catalog evolution, access control, and version-pinned sharing.

Only collection metadata is included; the artifact does not redistribute
clinical images. Source identifiers, snapshot dates, record counts, URLs, and
declared licenses are frozen in [`config/sources.json`](config/sources.json).

## Contents

| Path | Purpose |
|---|---|
| `config/` | Source manifest, semantic mappings, and governed-cohort protocol |
| `raw/` | Frozen public metadata snapshots used by the experiment |
| `source-db/` | PostgreSQL/MySQL initialization for the three source systems |
| `scripts/` | Cohort construction, DataFabric execution, figure generation, and validation |
| `derived/` | Deterministic integrated and governed datasets |
| `results/` | Machine-readable metrics and validation evidence |
| `figures/` | The five article figures in PNG format |
| `manifests/` | Environment and backend execution provenance |
| `logs/` | Sanitized API event metadata without credentials or response bodies |

## Requirements

- Linux x86-64;
- Python 3.13;
- packages pinned in `requirements.txt`;
- Docker Engine with Docker Compose v2 for the full backend execution;
- at least 8 GB RAM and 15 GB free storage for the complete DataFabric stack;
- free ports listed in the repository root `README.md`, plus `3311`, `5441`,
  and `5442` for the isolated source databases.

The local cohort construction and validation path does not require Docker.

## Installation

From this directory:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Confirm the published package and results:

```bash
python scripts/validate_governed_artifacts.py
```

A successful check ends with `Governed artifact package: PASS`.

## Reproduce the local analysis

The following commands rebuild the governed cohorts, machine-readable tables,
and the five PNG figures from the frozen metadata snapshots:

```bash
python scripts/build_governed_cohorts.py
python scripts/make_governed_artifacts.py
python scripts/validate_governed_artifacts.py
```

## Reproduce the full DataFabric execution

Run these commands from the repository root after completing the installation
above. The first command creates the shared Docker network when needed.

```bash
docker network inspect shared-network >/dev/null 2>&1 || docker network create shared-network
docker compose up -d --build
docker compose -f experiments/experiment3/docker-compose.yml up -d --build

experiments/experiment3/.venv/bin/python experiments/experiment3/scripts/run_datafabric.py
experiments/experiment3/.venv/bin/python experiments/experiment3/scripts/build_governed_cohorts.py
experiments/experiment3/.venv/bin/python experiments/experiment3/scripts/run_governed_datafabric.py
experiments/experiment3/.venv/bin/python experiments/experiment3/scripts/make_governed_artifacts.py
experiments/experiment3/.venv/bin/python experiments/experiment3/scripts/validate_governed_artifacts.py
```

The full run creates three isolated source databases, executes the integration
through the DataFabric API, materializes three governed views three times each,
checks their exact agreement with a local oracle, validates access control and
version pinning, and regenerates the article results.

## Expected outputs

- five PNG files under `figures/`;
- cohort, leakage, phototype, semantic-version, and backend metrics under
  `results/`;
- exact backend/oracle agreement for all three governed views and all three
  executions;
- a final validation status of `PASS`.

The backend execution evidence and the current package-validation report are
available as machine-readable files under `results/`.

## Data use and limitations

The snapshots contain public research metadata with pseudonymous collection,
image, lesion, and patient identifiers. They must not be interpreted as direct
patient identifiers or as clinical advice. Fitzpatrick phototype is a recorded
metadata field and is not treated as a direct measurement of skin color.

HAM10000 is declared as CC BY-NC, while HIBA Skin Lesions and PAD-UFES-20 are
declared as CC BY in the frozen source manifest. Users remain responsible for
following the original collection terms and attribution requirements.
