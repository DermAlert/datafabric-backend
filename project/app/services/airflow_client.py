"""
Cliente para comunicação com a API do Airflow.
Usado para disparar DAGs dos microserviços.
"""

import httpx
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import os

logger = logging.getLogger(__name__)

class AirflowClient:
    """Cliente para interagir com a API do Airflow"""
    
    def __init__(self):
        self.base_url = os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v1")
        self.username = os.getenv("AIRFLOW_USERNAME", "airflow")
        self.password = os.getenv("AIRFLOW_PASSWORD", "airflow")
        
    async def trigger_dag(
        self, 
        dag_id: str, 
        dag_run_id: str, 
        conf: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Dispara um DAG do Airflow
        
        Args:
            dag_id: ID do DAG a ser executado
            dag_run_id: ID único para esta execução
            conf: Configurações para passar ao DAG
            
        Returns:
            Resposta da API do Airflow com detalhes da execução
        """
        url = f"{self.base_url}/dags/{dag_id}/dagRuns"
        
        from datetime import timezone, timedelta
        
        # São Paulo timezone (UTC-3)
        sao_paulo_tz = timezone(timedelta(hours=-3))
        
        # Use current São Paulo time but convert to UTC for Airflow
        local_time = datetime.now(sao_paulo_tz)
        utc_time = local_time.astimezone(timezone.utc)
        
        payload = {
            "dag_run_id": dag_run_id,
            "conf": conf or {},
            "execution_date": utc_time.isoformat(),
            "note": f"Triggered via API at {local_time.strftime('%Y-%m-%d %H:%M:%S')} (São Paulo)"
        }
        
        try:
            async with httpx.AsyncClient() as client:
                logger.info(f"Triggering Airflow DAG: {dag_id} with run_id: {dag_run_id}")
                
                response = await client.post(
                    url,
                    json=payload,
                    auth=(self.username, self.password),
                    headers={"Content-Type": "application/json"},
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"✅ DAG {dag_id} triggered successfully: {dag_run_id}")
                    return result
                elif response.status_code == 409:
                    # DAG run already exists
                    logger.warning(f"DAG run {dag_run_id} already exists for {dag_id}")
                    raise ValueError(f"DAG run {dag_run_id} already exists")
                else:
                    error_msg = f"Failed to trigger DAG {dag_id}: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                    
        except httpx.RequestError as e:
            error_msg = f"Network error triggering DAG {dag_id}: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
        except Exception as e:
            logger.error(f"Unexpected error triggering DAG {dag_id}: {str(e)}")
            raise
    
    async def get_dag_run_status(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """
        Obtém o status de uma execução de DAG
        
        Args:
            dag_id: ID do DAG
            dag_run_id: ID da execução
            
        Returns:
            Status da execução do DAG
        """
        url = f"{self.base_url}/dags/{dag_id}/dagRuns/{dag_run_id}"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    auth=(self.username, self.password),
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    error_msg = f"Failed to get DAG run status: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                    
        except httpx.RequestError as e:
            error_msg = f"Network error getting DAG run status: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
    
    async def get_dag_runs(self, dag_id: str, limit: int = 10) -> Dict[str, Any]:
        """
        Lista execuções recentes de um DAG
        
        Args:
            dag_id: ID do DAG
            limit: Número máximo de execuções para retornar
            
        Returns:
            Lista de execuções do DAG
        """
        url = f"{self.base_url}/dags/{dag_id}/dagRuns"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    params={"limit": limit, "order_by": "-execution_date"},
                    auth=(self.username, self.password),
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    error_msg = f"Failed to get DAG runs: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                    
        except httpx.RequestError as e:
            error_msg = f"Network error getting DAG runs: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
    
    async def get_task_instances(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """
        Obtém instâncias de tasks de uma execução de DAG
        
        Args:
            dag_id: ID do DAG
            dag_run_id: ID da execução
            
        Returns:
            Lista de task instances
        """
        url = f"{self.base_url}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    auth=(self.username, self.password),
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    error_msg = f"Failed to get task instances: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                    
        except httpx.RequestError as e:
            error_msg = f"Network error getting task instances: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)
    
    async def health_check(self) -> bool:
        """
        Verifica se o Airflow está disponível
        
        Returns:
            True se o Airflow estiver disponível, False caso contrário
        """
        url = f"{self.base_url.replace('/api/v1', '')}/health"
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=5.0)
                return response.status_code == 200
        except Exception:
            return False
