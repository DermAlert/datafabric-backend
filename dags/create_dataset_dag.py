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
    'retry_delay': timedelta(minutes=10),  # Longer retry delay for dataset creation
}

# Create DAG
dag = DAG(
    'create_unified_dataset',
    default_args=default_args,
    description='Processa criação de datasets unificados via microserviço',
    schedule_interval=None,  # Triggered manually or via API
    catchup=False,
    max_active_runs=2,  # Allow up to 2 concurrent dataset creation DAGs
    tags=['microservice', 'dataset', 'unification'],
    is_paused_upon_creation=False,  # Start unpaused
    start_date=days_ago(1)  # Ensure start_date is in the past
)

def validate_dataset_params(**context):
    """Valida os parâmetros do job de criação de dataset"""
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    # Extract dataset_data from configuration
    dataset_data = conf.get('dataset_data')
    if not dataset_data:
        raise ValueError("dataset_data is required in DAG configuration")
    
    # Validate required fields
    required_fields = ['name', 'selection_mode']
    for field in required_fields:
        if field not in dataset_data:
            raise ValueError(f"dataset_data.{field} is required")
    
    # Validate selection mode and corresponding fields
    selection_mode = dataset_data.get('selection_mode')
    if selection_mode not in ['tables', 'columns']:
        raise ValueError("selection_mode must be 'tables' or 'columns'")
    
    if selection_mode == 'tables':
        if not dataset_data.get('selected_tables'):
            raise ValueError("selected_tables is required when selection_mode is 'tables'")
    elif selection_mode == 'columns':
        if not dataset_data.get('selected_columns'):
            raise ValueError("selected_columns is required when selection_mode is 'columns'")
    
    job_id = conf.get('job_id', f"dataset_{dataset_data['name']}_{int(datetime.now().timestamp())}")
    priority = conf.get('priority', 1)
    
    logger.info(f"Validated dataset job parameters:")
    logger.info(f"  - Dataset name: {dataset_data['name']}")
    logger.info(f"  - Selection mode: {selection_mode}")
    logger.info(f"  - Job ID: {job_id}")
    logger.info(f"  - Priority: {priority}")
    
    # Store validated params for next tasks
    context['task_instance'].xcom_push(key='dataset_data', value=dataset_data)
    context['task_instance'].xcom_push(key='job_id', value=job_id)
    context['task_instance'].xcom_push(key='priority', value=priority)
    
    return {
        'dataset_data': dataset_data,
        'job_id': job_id,
        'priority': priority
    }

def prepare_dataset_request(**context):
    """Prepara o payload para o microserviço de dataset"""
    ti = context['task_instance']
    
    dataset_data = ti.xcom_pull(key='dataset_data', task_ids='validate_params')
    job_id = ti.xcom_pull(key='job_id', task_ids='validate_params')
    priority = ti.xcom_pull(key='priority', task_ids='validate_params')
    
    payload = {
        'dataset_data': dataset_data,
        'job_id': job_id,
        'priority': priority
    }
    
    logger.info(f"Prepared dataset request payload for job {job_id}")
    return json.dumps(payload)

def check_dataset_preview(**context):
    """Opcionalmente gera preview do dataset antes da criação"""
    ti = context['task_instance']
    
    # Check if preview is requested
    dag_run = context['dag_run']
    conf = dag_run.conf or {}
    
    if conf.get('generate_preview', False):
        logger.info("Preview generation requested - calling preview endpoint")
        return True
    else:
        logger.info("Skipping preview generation")
        return False

# Task 1: Validate parameters
validate_params_task = PythonOperator(
    task_id='validate_params',
    python_callable=validate_dataset_params,
    dag=dag
)

# Task 2: Check if preview is needed
check_preview_task = PythonOperator(
    task_id='check_preview_needed',
    python_callable=check_dataset_preview,
    dag=dag
)

# Task 3: Prepare request
prepare_request_task = PythonOperator(
    task_id='prepare_request',
    python_callable=prepare_dataset_request,
    dag=dag
)

# Task 4: Generate preview (optional)
generate_preview_task = HttpOperator(
    task_id='generate_preview',
    http_conn_id='dataset_service_conn',  # Configure this connection in Airflow
    endpoint='/preview-dataset-creation',
    method='POST',
    headers={'Content-Type': 'application/json'},
    data="{{ task_instance.xcom_pull(task_ids='prepare_request') }}",
    dag=dag
)

# Task 5: Call dataset microservice
create_dataset_task = HttpOperator(
    task_id='call_dataset_microservice',
    http_conn_id='dataset_service_conn',  # Configure this connection in Airflow
    endpoint='/process-dataset-creation',
    method='POST',
    headers={'Content-Type': 'application/json'},
    data="{{ task_instance.xcom_pull(task_ids='prepare_request') }}",
    pool='dataset_pool',  # Use dataset pool for resource management
    pool_slots=1,  # Each dataset job takes 1 slot
    dag=dag
)

def log_dataset_completion(**context):
    """Log completion and extract results"""
    ti = context['task_instance']
    response = ti.xcom_pull(task_ids='call_dataset_microservice')
    
    logger.info(f"Dataset microservice response: {response}")
    
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
    dataset_id = response.get('dataset_id')
    
    if status == 'completed':
        logger.info(f"✅ Dataset creation job {job_id} completed successfully")
        logger.info(f"   Dataset ID: {dataset_id}")
        logger.info(f"   Message: {message}")
        
        # Log additional details if available
        details = response.get('details', {})
        if details:
            logger.info(f"   Processing time: {details.get('processing_time_seconds', 'unknown')}s")
            logger.info(f"   Storage type: {details.get('storage_type', 'unknown')}")
            logger.info(f"   Tables processed: {details.get('tables_count', 'unknown')}")
            logger.info(f"   Columns processed: {details.get('columns_count', 'unknown')}")
    else:
        logger.error(f"❌ Dataset creation job {job_id} failed: {message}")
        raise Exception(f"Dataset creation job failed: {message}")
    
    return response

# Task 6: Log completion
log_completion_task = PythonOperator(
    task_id='log_completion',
    python_callable=log_dataset_completion,
    dag=dag
)

# Define task dependencies
validate_params_task >> check_preview_task >> prepare_request_task

# Conditional preview generation
prepare_request_task >> generate_preview_task >> create_dataset_task
prepare_request_task >> create_dataset_task  # Direct path without preview

create_dataset_task >> log_completion_task

# Add task documentation
validate_params_task.doc_md = """
## Validate Dataset Parameters

Valida os parâmetros necessários para criação do dataset unificado:
- `dataset_data`: Dados do dataset (nome, modo de seleção, tabelas/colunas, etc.)
- `job_id`: ID único do job (opcional)
- `priority`: Prioridade do job (opcional)
"""

create_dataset_task.doc_md = """
## Call Dataset Microservice

Chama o microserviço de criação de dataset para processar a unificação.
Este task usa o pool 'dataset_pool' para controlar a concorrência.

O processamento inclui:
- Extração de dados das fontes
- Aplicação de mapeamentos de colunas
- Aplicação de mapeamentos de valores
- Salvamento no MinIO como Delta Lake
"""
