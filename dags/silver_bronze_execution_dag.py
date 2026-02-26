"""
DAG: spark_execution
====================
Orquestra a execução de jobs Spark Silver via pool do Airflow.

Bronze usa Trino (aiotrino — totalmente async) e não precisa de fila;
somente Silver é despachado para cá para isolar CPU do FastAPI.

Fluxo:
  1. FastAPI recebe POST /silver/.../execute → cria execution record (QUEUED) →
     dispara este DAG via Airflow REST API → retorna 202 imediatamente.
  2. spark_pool (padrão: 3 slots) garante que no máximo N jobs Spark
     rodam ao mesmo tempo, protegendo o backend de CPU starvation.
  3. A task execute_spark_job chama POST /api/internal/spark/execute
     no backend, que executa o job de fato (synchronously).
  4. O cliente faz polling em GET /api/silver/persistent/configs/{id}/executions.

Configuração no Airflow:
  Pool:        spark_pool (criado pelo airflow-init via docker-compose)
  Connection:  datafabric_backend → http://dermalert-backend:8000

Variáveis de ambiente (via Airflow Variables ou .env):
  SPARK_POOL_SIZE        Número de slots do spark_pool  (padrão: 3)
  AIRFLOW_API_BASE_URL   URL base do backend             (padrão: http://dermalert-backend:8000)

Parâmetros do DAG run (conf):
  job_type       sempre "silver"
  config_id      int — ID da TransformConfig Silver
  execution_id   int — ID do execution record criado pelo FastAPI
  priority       int — prioridade do job (1 = normal, 10 = urgente)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Pool, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SPARK_POOL     = "spark_pool"
BACKEND_CONN   = "datafabric_backend"   # HTTP Connection configurada no Airflow
BACKEND_URL    = Variable.get("AIRFLOW_API_BASE_URL", default_var="http://dermalert-backend:8000")

default_args = {
    "owner":            "datafabric",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "retry_exponential_backoff": True,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ensure_spark_pool(**context):
    """
    Cria o spark_pool caso não exista (idempotente).
    O airflow-init já faz isso, mas esta task serve como fallback.
    """
    pool_size = int(Variable.get("SPARK_POOL_SIZE", default_var="3"))
    existing  = Pool.get_pool(SPARK_POOL)
    if not existing:
        Pool.create_or_update_pool(
            name=SPARK_POOL,
            slots=pool_size,
            description="Controls max concurrent Silver Spark jobs",
            include_deferred=False,
        )
        logger.info(f"Created spark_pool with {pool_size} slots")
    else:
        logger.info(f"spark_pool already exists ({existing.slots} slots)")


def _validate_params(**context):
    """Valida e normaliza os parâmetros do DAG run."""
    conf        = context["dag_run"].conf or {}
    job_type    = conf.get("job_type")
    config_id   = conf.get("config_id")
    execution_id = conf.get("execution_id")

    if job_type != "silver":
        raise ValueError(f"job_type must be 'silver', got: {job_type!r}")
    if not isinstance(config_id, int) or config_id <= 0:
        raise ValueError(f"config_id must be a positive int, got: {config_id!r}")
    if not isinstance(execution_id, int) or execution_id <= 0:
        raise ValueError(f"execution_id must be a positive int, got: {execution_id!r}")

    priority  = int(conf.get("priority", 1))
    priority  = max(1, min(10, priority))

    ti = context["task_instance"]
    ti.xcom_push(key="job_type",     value=job_type)
    ti.xcom_push(key="config_id",    value=config_id)
    ti.xcom_push(key="execution_id", value=execution_id)
    ti.xcom_push(key="priority",     value=priority)

    logger.info(
        f"spark_execution DAG: job_type={job_type} config_id={config_id} "
        f"execution_id={execution_id} priority={priority}"
    )
    return {"job_type": job_type, "config_id": config_id,
            "execution_id": execution_id, "priority": priority}


def _build_execute_payload(**context):
    """Prepara o payload JSON para o endpoint interno do FastAPI."""
    ti           = context["task_instance"]
    job_type     = ti.xcom_pull(key="job_type",     task_ids="validate_params")
    config_id    = ti.xcom_pull(key="config_id",    task_ids="validate_params")
    execution_id = ti.xcom_pull(key="execution_id", task_ids="validate_params")
    priority     = ti.xcom_pull(key="priority",     task_ids="validate_params")

    payload = {
        "job_type":     job_type,
        "config_id":    config_id,
        "execution_id": execution_id,
        "priority":     priority,
    }
    logger.info(f"Built execute payload: {payload}")
    return json.dumps(payload)


def _log_result(**context):
    """Loga o resultado e levanta exceção se o job falhou."""
    ti       = context["task_instance"]
    response = ti.xcom_pull(task_ids="execute_spark_job")
    job_type = ti.xcom_pull(key="job_type",     task_ids="validate_params")
    cid      = ti.xcom_pull(key="config_id",    task_ids="validate_params")
    eid      = ti.xcom_pull(key="execution_id", task_ids="validate_params")

    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError:
            logger.error(f"Could not parse response: {response}")
            raise RuntimeError(f"Spark job returned unparseable response: {response[:200]}")

    job_status = response.get("status", "unknown")
    rows       = response.get("rows_output") or response.get("rows_processed") or 0
    duration   = response.get("duration_seconds", 0)

    if job_status == "success":
        logger.info(
            f"✅ Spark job succeeded | {job_type} config={cid} execution={eid} "
            f"rows={rows:,} duration={duration:.1f}s"
        )
    else:
        error = response.get("message") or response.get("error") or "unknown error"
        logger.error(
            f"❌ Spark job failed | {job_type} config={cid} execution={eid} "
            f"status={job_status} error={error}"
        )
        raise RuntimeError(f"Spark job failed: {error}")

    return response


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="spark_execution",
    default_args=default_args,
    description="Executa jobs Spark Silver via pool controlado",
    schedule_interval=None,       # Triggered via API only
    catchup=False,
    max_active_runs=50,           # Cada usuário pode ter seu próprio DAG run
    max_active_tasks=50,          # Pool spark_pool limita concorrência real
    tags=["spark", "silver", "etl"],
    is_paused_upon_creation=False,
    params={
        "job_type":     "silver",
        "config_id":    1,
        "execution_id": 1,
        "priority":     1,
    },
) as dag:

    # ------------------------------------------------------------------
    # Task 0: garante que o pool existe (fast, sem pool constraint)
    # ------------------------------------------------------------------
    ensure_pool = PythonOperator(
        task_id="ensure_spark_pool",
        python_callable=_ensure_spark_pool,
    )

    # ------------------------------------------------------------------
    # Task 1: valida parâmetros (fast, sem pool constraint)
    # ------------------------------------------------------------------
    validate = PythonOperator(
        task_id="validate_params",
        python_callable=_validate_params,
    )

    # ------------------------------------------------------------------
    # Task 2: prepara payload (fast, sem pool constraint)
    # ------------------------------------------------------------------
    build_payload = PythonOperator(
        task_id="build_payload",
        python_callable=_build_execute_payload,
    )

    # ------------------------------------------------------------------
    # Task 3: executa o job Spark via FastAPI internal endpoint
    #
    # pool=SPARK_POOL + pool_slots=1 → max N jobs simultâneos (N=spark_pool slots)
    # execution_timeout=2h → jobs muito longos são interrompidos
    # ------------------------------------------------------------------
    execute_job = HttpOperator(
        task_id="execute_spark_job",
        http_conn_id=BACKEND_CONN,
        endpoint="/api/internal/spark/execute",
        method="POST",
        headers={"Content-Type": "application/json"},
        data="{{ task_instance.xcom_pull(task_ids='build_payload') }}",
        response_check=lambda response: response.status_code == 200,
        extra_options={"timeout": 7200},  # 2h max per job
        # ↓ O guardião de concorrência: no máximo spark_pool.slots jobs simultâneos
        pool=SPARK_POOL,
        pool_slots=1,
        execution_timeout=timedelta(hours=2),
        dag=dag,
    )

    # ------------------------------------------------------------------
    # Task 4: loga resultado (fast, sem pool constraint)
    # ------------------------------------------------------------------
    log_result = PythonOperator(
        task_id="log_result",
        python_callable=_log_result,
    )

    # Pipeline
    ensure_pool >> validate >> build_payload >> execute_job >> log_result
