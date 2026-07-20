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
| `minio` | Object storage API and console | `9000`, `9001` |
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

The root [`.env`](./.env) file is loaded by Docker Compose and by the backend container.

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

- Docker named volumes for PostgreSQL, Spark recovery, and Spark Ivy cache
- [`./data`](./data) for MinIO object data

If you run `docker compose down -v`, the PostgreSQL databases and Docker-managed caches are removed.

## Troubleshooting

- If `airflow-init` fails, check Docker memory and CPU allocation first
- If the backend cannot talk to Trino or Postgres, confirm the `.env` values use Docker service names like `trino` and `postgres-backend`
- If ports are already in use, change the host side of the mappings in [`docker-compose.yml`](./docker-compose.yml)
- If Spark jobs are slow on the first run, wait for dependency download and cache warm-up
- If you want a clean restart of the whole stack, run `docker compose down -v` and then `docker compose up -d --build`
