# Data Fabric Backend

This repository runs a data platform stack built around:

- FastAPI backend
- PostgreSQL for the application database
- MinIO for object storage
- Trino for query federation
- Spark for processing / Delta work
- Airflow for orchestration
- Redis for Airflow Celery

The recommended way to run the project is with Docker Compose so every service comes up with the expected network names and ports.

The evaluation artifacts are documented in:

- [Experiment 1 — Federated Query Performance](./experiments/experiment%201/README.md)
- [Experiment 2 — Semantic Retrieval and Normalization](./experiments/experiment%202/README.md)
- [Experiment 3 — Governed Dermatology Data Integration](./experiments/experiment3/README.md)

## Services

| Service | What it does | Host port(s) |
| --- | --- | --- |
| `dermalert-backend` | FastAPI API server | `8004` |
| `postgres-backend` | Main application database | `5434` |
| `cmpd-postgres` | CMPD PostgreSQL demo source | `5435` |
| `ida-mysql` | IDA MySQL demo source | `3307` |
| `reh-postgres` | REH PostgreSQL demo source | `5436` |
| `cdi-postgres` | CDI PostgreSQL demo source | `5437` |
| `rfa-mysql` | RFA MySQL demo source | `3308` |
| `dermexp_ham_postgres` | HAM10000 PostgreSQL demo source | `5441` |
| `dermexp_hiba_postgres` | HIBA PostgreSQL demo source | `5442` |
| `dermexp_pad_mysql` | PAD-UFES-20 MySQL demo source | `3311` |
| `minio` | Object storage API and console | `9000`, `9001` |
| `minio-init` | Creates the Bronze, Silver, and metastore buckets | one-off job |
| `trino` | SQL query engine | `8089` |
| `spark-master` | Spark master + UI | `7077`, `8082` |
| `spark-worker` | Spark worker UI | `8083` |
| `postgres-airflow` | Airflow metadata database | `5433` |
| `redis` | Celery broker for Airflow | internal only |
| `airflow-init` | First-run Airflow setup and bootstrap | one-off job |
| `airflow-webserver` | Airflow UI | `8080` |
| `airflow-scheduler` | Airflow scheduler | internal only |
| `airflow-worker` | Airflow Celery worker | internal only |
| `airflow-triggerer` | Airflow triggerer | internal only |

Note: there is a commented `dataset-service` stub in [`docker-compose.yml`](./docker-compose.yml), but it is not part of the active stack right now.

## Requirements

- Docker
- Docker Compose v2 (`docker compose`)
- At least 4 GB of Docker memory for Airflow alone
- 8 GB or more recommended for the full stack because Spark and Trino are also running
- These ports should be free on your machine: `5433`, `5434`, `7077`, `7078`, `7079`, `8004`, `8080`, `8082`, `8083`, `8089`, `9000`, `9001`, `4040`

## Environment

The root `.env` file is optional. Docker Compose includes local development
defaults, so a fresh checkout starts with `docker compose up --build` without
creating or editing any configuration file. Add `.env` only to override those
defaults (for example, to enable an external LLM provider).

Important:

- Use `KEY=value` format with no spaces around `=`
- Values inside `.env` should use container-to-container addresses, not browser `localhost` addresses
- Example: inside Docker the backend should reach Trino at `trino:8080`, even though from your browser you access Trino at `http://localhost:8089`

A good starting point for Docker is:

```env
AIRFLOW_UID=1000

DATABASE_URL=postgresql+asyncpg://postgres:postgres@postgres-backend:5432/dermalert_backend

MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
MINIO_SECURE=False

TRINO_HOST=trino
TRINO_PORT=8080

AIRFLOW_BASE_URL=http://airflow-webserver:8080
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow

SPARK_MODE=cluster
SPARK_MASTER=spark://spark-master:7077
SPARK_MASTER_WEBUI_PORT=8082

BRONZE_BUCKET=datafabric-bronze
SILVER_BUCKET=datafabric-silver
INTERNAL_METASTORE_BUCKET=datafabric-metastore
```

If you leave `SPARK_MODE` unset, the backend falls back to local Spark mode. For the Docker Compose cluster in this repo, `SPARK_MODE=cluster` is the expected setting.

## Installation

From the repository root:

```bash
docker compose up -d --build
```

What happens during startup:

- `airflow-init` bootstraps Airflow and creates default resources
- On the first startup of a new `backend_pgdata` volume, PostgreSQL restores the anonymized development snapshot from `docker/postgres/init/`
- Five synthetic hospital databases and three public dermatology metadata databases are created and populated automatically
- The restored DataFabric connections already use the Compose service names and matching local development credentials
- `minio-init` creates the Bronze, Silver, and internal metastore buckets
- The backend waits for every source database and bucket to be ready before it starts
- `dermalert-backend` runs Alembic migrations automatically before starting Uvicorn
- Spark and Trino may take a little while on the first boot
- The first Spark-related startup can be slower because dependencies and JARs are cached

Check status:

```bash
docker compose ps
```

Follow logs:

```bash
docker compose logs -f dermalert-backend
docker compose logs -f airflow-webserver airflow-scheduler airflow-worker
docker compose logs -f spark-master spark-worker trino
```

## Open the Services

- Backend API: `http://localhost:8004`
- Swagger UI: `http://localhost:8004/docs`
- ReDoc: `http://localhost:8004/redoc`
- Backend health: `http://localhost:8004/`
- Internal health: `http://localhost:8004/api/internal/health`
- Airflow UI: `http://localhost:8080`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Trino: `http://localhost:8089`
- Spark Master UI: `http://localhost:8082`
- Spark Worker UI: `http://localhost:8083`

Default dev credentials in the compose file:

- Airflow: `airflow` / `airflow`
- MinIO: `minio` / `minio123`
- PostgreSQL: `postgres` / `postgres`

## Start Services In Groups

If you do not want to bring everything up at once, these are the main groups.

Core data services:

```bash
docker compose up -d postgres-backend minio trino spark-master spark-worker
```

Backend API:

```bash
docker compose up -d --build dermalert-backend
```

Airflow services:

```bash
docker compose up -d postgres-airflow redis airflow-init airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
```

Tip: `docker compose up -d --build dermalert-backend` will also start the backend dependencies declared in Compose if they are not already running.

## Useful Commands

Stop everything:

```bash
docker compose down
```

Stop everything and delete volumes:

```bash
docker compose down -v
```

Rebuild specific images:

```bash
docker compose build dermalert-backend spark-master spark-worker
```

Open a shell in the backend container:

```bash
docker compose exec dermalert-backend bash
```

Run backend migrations manually:

```bash
docker compose exec dermalert-backend poetry run alembic upgrade head
```

## Data Persistence

Persistent data is stored in:

- Docker named volumes for the application database, all source databases, Spark recovery, and Spark Ivy cache
- [`./data`](./data) for MinIO object data

If you run `docker compose down -v`, the PostgreSQL databases and Docker-managed caches are removed.

The application database is automatically populated only when PostgreSQL creates
a new `backend_pgdata` volume. Existing volumes are never overwritten. To recreate
the test database from the bundled snapshot:

```bash
docker compose down
docker volume rm datafabric-backend_backend_pgdata
docker compose up -d --build
```

The volume name assumes the default Compose project name. Check it first with
`docker volume ls` if you use `COMPOSE_PROJECT_NAME` or `docker compose -p`.

### Refreshing the development snapshot

With `postgres-backend` running and containing the desired reference data, run:

```bash
./docker/postgres/export-seed.sh
```

The exporter clones the database into a temporary database, anonymizes sensitive
fields there, writes `docker/postgres/init/20-demo-data.sql.gz`, validates the
result, and removes the temporary database. It does not modify the source database.
Commit the refreshed snapshot together with intentional schema changes.

## Troubleshooting

- If `airflow-init` fails, check Docker memory and CPU allocation first
- If the backend cannot talk to Trino or Postgres, confirm the `.env` values use Docker service names like `trino` and `postgres-backend`
- If ports are already in use, change the host side of the mappings in [`docker-compose.yml`](./docker-compose.yml)
- If Spark jobs are slow on the first run, wait for dependency download and cache warm-up
- If you want a clean restart of the whole stack, run `docker compose down -v` and then `docker compose up -d --build`
