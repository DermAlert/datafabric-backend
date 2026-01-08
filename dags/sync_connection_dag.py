from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import json
import logging

# Define São Paulo timezone (UTC-3)
SAO_PAULO_TZ = timezone(timedelta(hours=-3))

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'datafabric',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'sync_data_connection',
    default_args=default_args,
    description='Processa sincronização de metadados de conexões de dados via microserviço',
    schedule_interval=None,  # Triggered manually or via API
    catchup=False,
    max_active_runs=3,  # Permite até 3 DAG runs simultâneos (para testar fila)
    max_active_tasks=10,  # Permite múltiplas tarefas ativas por DAG run
    tags=['microservice', 'sync', 'metadata'],
    is_paused_upon_creation=False,  # Start unpaused
    start_date=days_ago(1)  # Ensure start_date is in the past
)

def validate_sync_params(**context):
    """Valida os parâmetros do job de sync"""
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    connection_id = conf.get('connection_id')
    if not connection_id:
        raise ValueError("connection_id is required in DAG configuration")
    
    if not isinstance(connection_id, int) or connection_id <= 0:
        raise ValueError("connection_id must be a positive integer")
    
    job_id = conf.get('job_id', f"sync_{connection_id}_{int(datetime.now().timestamp())}")
    priority = conf.get('priority', 1)
    
    logger.info(f"Validated sync job parameters: connection_id={connection_id}, job_id={job_id}, priority={priority}")
    
    # Store validated params for next tasks
    context['task_instance'].xcom_push(key='connection_id', value=connection_id)
    context['task_instance'].xcom_push(key='job_id', value=job_id)
    context['task_instance'].xcom_push(key='priority', value=priority)
    
    return {
        'connection_id': connection_id,
        'job_id': job_id,
        'priority': priority
    }

def prepare_sync_request(**context):
    """Prepara o payload para o microserviço de sync"""
    ti = context['task_instance']
    
    connection_id = ti.xcom_pull(key='connection_id', task_ids='validate_params')
    job_id = ti.xcom_pull(key='job_id', task_ids='validate_params')
    priority = ti.xcom_pull(key='priority', task_ids='validate_params')
    
    payload = {
        'connection_id': connection_id,
        'job_id': job_id,
        'priority': priority
    }
    
    logger.info(f"Prepared sync request payload: {payload}")
    return json.dumps(payload)

# Task 1: Validate parameters
validate_params_task = PythonOperator(
    task_id='validate_params',
    python_callable=validate_sync_params,
    # Não usa pool - validação é rápida
    dag=dag
)

# Task 2: Prepare request
prepare_request_task = PythonOperator(
    task_id='prepare_request',
    python_callable=prepare_sync_request,
    # Não usa pool - preparação é rápida
    dag=dag
)

# Task 3: Call FastAPI internal endpoint
sync_microservice_task = HttpOperator(
    task_id='call_sync_microservice',
    http_conn_id='sync_service_conn',  # Conexão HTTP para o FastAPI backend
    endpoint='/api/internal/process-sync',  # Endpoint interno do FastAPI (com prefixo /api)
    method='POST',
    headers={'Content-Type': 'application/json'},
    data="{{ task_instance.xcom_pull(task_ids='prepare_request') }}",
    pool='sync_pool',  # Pool para controlar concorrência de syncs
    pool_slots=1,  # Cada sync job ocupa 1 slot
    dag=dag
)

def log_sync_completion(**context):
    """Log completion and extract results"""
    ti = context['task_instance']
    response = ti.xcom_pull(task_ids='call_sync_microservice')
    
    logger.info(f"Sync microservice response: {response}")
    
    # Parse response if it's a string
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse response: {response}")
            raise
    
    job_id = response.get('job_id', 'unknown')
    status = response.get('status', 'unknown')
    message = response.get('message', 'No message')
    
    if status == 'completed':
        logger.info(f"✅ Sync job {job_id} completed successfully: {message}")
    else:
        logger.error(f"❌ Sync job {job_id} failed: {message}")
        raise Exception(f"Sync job failed: {message}")
    
    return response

# Task 4: Log completion
log_completion_task = PythonOperator(
    task_id='log_completion',
    python_callable=log_sync_completion,
    dag=dag
)

# Define task dependencies
validate_params_task >> prepare_request_task >> sync_microservice_task >> log_completion_task

# Add task documentation
validate_params_task.doc_md = """
## Validate Sync Parameters

Valida os parâmetros necessários para o job de sincronização:
- `connection_id`: ID da conexão a ser sincronizada
- `job_id`: ID único do job (opcional, gerado automaticamente se não fornecido)
- `priority`: Prioridade do job (opcional, padrão = 1)
"""

sync_microservice_task.doc_md = """
## Call Sync Microservice

Chama o microserviço de sincronização para processar a extração de metadados.
Este task usa o pool 'sync_pool' para controlar a concorrência.
"""
