"""
Airflow API Client for triggering DAGs
"""
import os
import logging
from typing import Optional, Dict, Any
from datetime import datetime

import httpx

from ..core.config import (
    AIRFLOW_BASE_URL,
    AIRFLOW_USERNAME,
    AIRFLOW_PASSWORD,
    AIRFLOW_SYNC_DAG_ID
)

logger = logging.getLogger(__name__)

# Pool configuration
SYNC_POOL_NAME = "sync_pool"
SYNC_POOL_SLOTS = 10
SYNC_POOL_DESCRIPTION = "Pool para controlar concorrência de jobs de sincronização de metadados"

# HTTP Connection configuration (for Airflow to call FastAPI)
SYNC_CONN_ID = "sync_service_conn"
SYNC_CONN_HOST = os.getenv("BACKEND_HOST", "host.docker.internal")
SYNC_CONN_PORT = int(os.getenv("BACKEND_PORT", "8004"))

# Cache para evitar verificações repetidas de pool/connection
_resources_ensured = False


class AirflowClientError(Exception):
    """Exception raised when Airflow API call fails"""
    pass


class AirflowClient:
    """Client for interacting with Airflow REST API"""
    
    def __init__(
        self,
        base_url: str = AIRFLOW_BASE_URL,
        username: str = AIRFLOW_USERNAME,
        password: str = AIRFLOW_PASSWORD
    ):
        self.base_url = base_url.rstrip('/')
        self.auth = (username, password)
        
    async def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[Dict[str, Any]] = None,
        logical_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Trigger a DAG run via Airflow REST API
        
        Args:
            dag_id: The DAG ID to trigger
            conf: Configuration parameters to pass to the DAG
            logical_date: Optional logical date for the DAG run
            
        Returns:
            Dict with DAG run information
            
        Raises:
            AirflowClientError: If the API call fails
        """
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"
        
        payload = {}
        if conf:
            payload["conf"] = conf
        if logical_date:
            payload["logical_date"] = logical_date.isoformat()
            
        logger.info(f"Triggering DAG {dag_id} with conf: {conf}")
        
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    url,
                    json=payload,
                    auth=self.auth,
                    headers={"Content-Type": "application/json"}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"DAG {dag_id} triggered successfully. Run ID: {result.get('dag_run_id')}")
                    return result
                elif response.status_code == 404:
                    raise AirflowClientError(f"DAG '{dag_id}' not found in Airflow")
                elif response.status_code == 401:
                    raise AirflowClientError("Airflow authentication failed. Check credentials.")
                elif response.status_code == 409:
                    # DAG run already exists for this logical_date
                    raise AirflowClientError(f"DAG run already exists: {response.text}")
                else:
                    raise AirflowClientError(
                        f"Failed to trigger DAG. Status: {response.status_code}, Response: {response.text}"
                    )
                    
        except httpx.ConnectError as e:
            logger.error(f"Failed to connect to Airflow at {self.base_url}: {e}")
            raise AirflowClientError(f"Cannot connect to Airflow at {self.base_url}") from e
        except httpx.TimeoutException as e:
            logger.error(f"Timeout connecting to Airflow: {e}")
            raise AirflowClientError("Airflow request timed out") from e
            
    async def get_dag_run_status(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """
        Get the status of a DAG run
        
        Args:
            dag_id: The DAG ID
            dag_run_id: The DAG run ID
            
        Returns:
            Dict with DAG run status information
        """
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
        
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.get(url, auth=self.auth)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    raise AirflowClientError(f"DAG run '{dag_run_id}' not found")
                else:
                    raise AirflowClientError(
                        f"Failed to get DAG run status. Status: {response.status_code}"
                    )
                    
        except httpx.ConnectError as e:
            raise AirflowClientError(f"Cannot connect to Airflow") from e
            
    async def ensure_connection_exists(
        self,
        conn_id: str,
        host: str,
        port: int,
        conn_type: str = "http",
        schema: str = "http"
    ) -> bool:
        """
        Ensure an HTTP connection exists in Airflow, creating it if necessary.
        
        Args:
            conn_id: Connection ID
            host: Host address
            port: Port number
            conn_type: Connection type (default: http)
            schema: URL schema (default: http)
            
        Returns:
            True if connection exists or was created, False on error
        """
        url = f"{self.base_url}/api/v1/connections/{conn_id}"
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Check if connection exists
                response = await client.get(url, auth=self.auth)
                
                if response.status_code == 200:
                    logger.debug(f"Connection '{conn_id}' already exists")
                    return True
                elif response.status_code == 404:
                    # Connection doesn't exist, create it
                    logger.info(f"Creating HTTP connection '{conn_id}' -> {host}:{port}")
                    
                    create_url = f"{self.base_url}/api/v1/connections"
                    create_response = await client.post(
                        create_url,
                        json={
                            "connection_id": conn_id,
                            "conn_type": conn_type,
                            "host": host,
                            "port": port,
                            "schema": schema,
                            "description": f"Auto-created connection to FastAPI backend at {host}:{port}"
                        },
                        auth=self.auth,
                        headers={"Content-Type": "application/json"}
                    )
                    
                    if create_response.status_code == 200:
                        logger.info(f"Connection '{conn_id}' created successfully")
                        return True
                    else:
                        logger.error(f"Failed to create connection: {create_response.status_code} - {create_response.text}")
                        return False
                else:
                    logger.error(f"Unexpected response checking connection: {response.status_code}")
                    return False
                    
        except Exception as e:
            logger.warning(f"Could not ensure connection exists: {e}")
            return False

    async def ensure_pool_exists(self, pool_name: str, slots: int, description: str) -> bool:
        """
        Ensure a pool exists in Airflow, creating it if necessary.
        
        Args:
            pool_name: Name of the pool
            slots: Number of slots for the pool
            description: Pool description
            
        Returns:
            True if pool exists or was created, False on error
        """
        url = f"{self.base_url}/api/v1/pools/{pool_name}"
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                # Check if pool exists
                response = await client.get(url, auth=self.auth)
                
                if response.status_code == 200:
                    logger.debug(f"Pool '{pool_name}' already exists")
                    return True
                elif response.status_code == 404:
                    # Pool doesn't exist, create it
                    logger.info(f"Creating pool '{pool_name}' with {slots} slots")
                    
                    create_url = f"{self.base_url}/api/v1/pools"
                    create_response = await client.post(
                        create_url,
                        json={
                            "name": pool_name,
                            "slots": slots,
                            "description": description
                        },
                        auth=self.auth,
                        headers={"Content-Type": "application/json"}
                    )
                    
                    if create_response.status_code == 200:
                        logger.info(f"Pool '{pool_name}' created successfully")
                        return True
                    else:
                        logger.error(f"Failed to create pool: {create_response.status_code} - {create_response.text}")
                        return False
                else:
                    logger.error(f"Unexpected response checking pool: {response.status_code}")
                    return False
                    
        except Exception as e:
            logger.warning(f"Could not ensure pool exists: {e}")
            return False

    async def trigger_sync_dag(self, connection_id: int, priority: int = 1) -> Dict[str, Any]:
        """
        Convenience method to trigger the sync_data_connection DAG
        
        Args:
            connection_id: The data connection ID to sync
            priority: Job priority (1-10)
            
        Returns:
            Dict with DAG run information
        """
        global _resources_ensured
        
        # Verificar pool/connection apenas uma vez por instância do app
        if not _resources_ensured:
            # Ensure the HTTP connection exists (for DAG to call FastAPI)
            await self.ensure_connection_exists(
                SYNC_CONN_ID,
                SYNC_CONN_HOST,
                SYNC_CONN_PORT
            )
            
            # Ensure the sync pool exists before triggering
            await self.ensure_pool_exists(
                SYNC_POOL_NAME,
                SYNC_POOL_SLOTS,
                SYNC_POOL_DESCRIPTION
            )
            _resources_ensured = True
            logger.info("Airflow resources (pool + connection) verified - will not check again")
        
        job_id = f"sync_{connection_id}_{int(datetime.now().timestamp())}"
        
        conf = {
            "connection_id": connection_id,
            "job_id": job_id,
            "priority": priority
        }
        
        return await self.trigger_dag(AIRFLOW_SYNC_DAG_ID, conf=conf)


# Singleton instance
_airflow_client: Optional[AirflowClient] = None


def get_airflow_client() -> AirflowClient:
    """Get or create the Airflow client singleton"""
    global _airflow_client
    if _airflow_client is None:
        _airflow_client = AirflowClient()
    return _airflow_client
