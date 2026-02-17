from sqlalchemy.future import select
from fastapi import HTTPException, status
from typing import Optional
from datetime import datetime
import logging
from app.api.schemas.data_connection import (
    DataConnectionCreate, DataConnectionUpdate, DataConnectionResponse, ConnectionTestResult, ConnectionTestRequest
)
from app.api.schemas.search import BaseSearchRequest, SearchDataResult

from app.database.utils import DatabaseService
from app.database.models import core
from app.services.connection_manager import test_connection
from app.utils.connection_validators import validate_metadata_connection
from app.services.airflow_client import get_airflow_client
from app.services.credential_service import get_credential_service

logger = logging.getLogger(__name__)

class DataConnectionService:
    def __init__(self, db):
        self.db = db
        self.db_service = DatabaseService(db)

    async def create(self, data: DataConnectionCreate, background_tasks=None):
        stmt = select(core.DataConnection).where(
            (core.DataConnection.name == data.name) &
            (core.DataConnection.organization_id == data.organization_id)
        )
        existing = await self.db_service.scalars_first(stmt)
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail=f"Connection with name '{data.name}' already exists in this organization")
        stmt_ct = select(core.ConnectionType).where(core.ConnectionType.id == data.connection_type_id)
        ct = await self.db_service.scalars_first(stmt_ct)
        if not ct:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection type not found")
        
        # SEGURANÇA: Criptografar credenciais antes de salvar
        credential_service = get_credential_service()
        data_dict = data.model_dump()
        
        if data_dict.get("connection_params"):
            data_dict["connection_params"] = credential_service.encrypt_for_storage(
                data_dict["connection_params"],
                connection_type=ct.name,
                connection_id=None  # ID ainda não existe
            )
            logger.info(f"[DataConnectionService] Credentials encrypted for new connection '{data.name}'")
        
        obj = core.DataConnection(**data_dict)
        self.db.add(obj)
        await self.db.commit()
        await self.db.refresh(obj)
        return obj

    async def list(self, search: BaseSearchRequest, organization_id: Optional[int] = None,
                   status: Optional[str] = None, connection_type_id: Optional[int] = None,
                   content_type: Optional[str] = None, name: Optional[str] = None):
        stmt = select(core.DataConnection)
        if organization_id:
            stmt = stmt.where(core.DataConnection.organization_id == organization_id)
        if status:
            stmt = stmt.where(core.DataConnection.status == status)
        if connection_type_id:
            stmt = stmt.where(core.DataConnection.connection_type_id == connection_type_id)
        if content_type:
            stmt = stmt.where(core.DataConnection.content_type == content_type)
        if name:
            stmt = stmt.where(core.DataConnection.name.ilike(f"%{name}%"))
        stmt = stmt.order_by(core.DataConnection.id)
        result = await self.db_service.scalars_paginate(search, stmt)
        return result

    async def get(self, id: int):
        stmt = select(core.DataConnection).where(core.DataConnection.id == id)
        obj = await self.db_service.scalars_first(stmt)
        if not obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Data connection with ID {id} not found")
        return obj

    async def update(self, id: int, data: DataConnectionUpdate, background_tasks=None):
        obj = await self.get(id)
        # Name conflict check
        if data.name and data.name != obj.name:
            stmt = select(core.DataConnection).where(
                (core.DataConnection.name == data.name) &
                (core.DataConnection.organization_id == obj.organization_id) &
                (core.DataConnection.id != id)
            )
            existing = await self.db_service.scalars_first(stmt)
            if existing:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"Connection with name '{data.name}' already exists in this organization")
        
        # SEGURANÇA: Criptografar credenciais se estiverem sendo atualizadas
        data_dict = data.model_dump(exclude_unset=True)
        
        if "connection_params" in data_dict and data_dict["connection_params"]:
            # Buscar tipo de conexão para criptografia adequada
            stmt_ct = select(core.ConnectionType).where(core.ConnectionType.id == obj.connection_type_id)
            ct = await self.db_service.scalars_first(stmt_ct)
            connection_type = ct.name if ct else "unknown"
            
            credential_service = get_credential_service()
            data_dict["connection_params"] = credential_service.encrypt_for_storage(
                data_dict["connection_params"],
                connection_type=connection_type,
                connection_id=id
            )
            logger.info(f"[DataConnectionService] Credentials encrypted for connection update id={id}")
        
        for attr, value in data_dict.items():
            if value is not None:
                setattr(obj, attr, value)
        await self.db.commit()
        await self.db.refresh(obj)
        return obj

    async def delete(self, id: int):
        obj = await self.get(id)
        # TODO: check if in use by datasets (implement if you have that logic)
        await self.db.delete(obj)
        await self.db.commit()

    async def _get_connection_type(self, connection_type_id: int) -> "core.ConnectionType":
        """Busca o ConnectionType pelo ID. Lança 404 se não encontrar."""
        stmt = select(core.ConnectionType).where(core.ConnectionType.id == connection_type_id)
        ct = await self.db_service.scalars_first(stmt)
        if not ct:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection type not found")
        return ct

    def _mask_sensitive_params(self, params: dict) -> dict:
        """Mascara valores sensíveis (password, key, secret) para logging seguro."""
        safe_params = {}
        for k, v in params.items():
            if any(word in k.lower() for word in ('key', 'secret', 'password', 'token')):
                safe_params[k] = f"{str(v)[:4]}***" if v else None
            else:
                safe_params[k] = v
        return safe_params

    async def _do_test(self, connection_type_id: int, connection_params: dict, context: str = "") -> ConnectionTestResult:
        """
        Lógica central de teste de conexão.
        
        Args:
            connection_type_id: ID do tipo de conexão
            connection_params: Parâmetros da conexão (podem estar criptografados)
            context: Contexto para logging (ex: "id=5" ou "params only")
        """
        ct = await self._get_connection_type(connection_type_id)
        
        # SEGURANÇA: Descriptografar credenciais para teste
        credential_service = get_credential_service()
        decrypted_params = credential_service.decrypt_for_use(
            connection_params,
            purpose="connection_test"
        )
        
        logger.info(f"[DataConnectionService] Testing connection {context}")
        logger.info(f"[DataConnectionService] Connection type: {ct.name}")
        # SEGURANÇA: Usar mascaramento centralizado
        logger.info(f"[DataConnectionService] Params (masked): {credential_service.mask_for_logging(decrypted_params, ct.name)}")
        
        success, message, details = await test_connection(
            connection_type=ct.name,
            connection_params=decrypted_params
        )
        
        return ConnectionTestResult(success=success, message=message, details=details)

    async def test(self, id: int) -> ConnectionTestResult:
        """Testa conexão existente e atualiza seu status no banco."""
        obj = await self.get(id)
        
        result = await self._do_test(
            connection_type_id=obj.connection_type_id,
            connection_params=obj.connection_params,
            context=f"id={id}"
        )
        
        # Atualiza status no banco
        obj.status = 'active' if result.success else 'error'
        await self.db.commit()
        
        return result

    async def test_params(self, data: ConnectionTestRequest) -> ConnectionTestResult:
        """
        Testa conexão ANTES de salvar no banco.
        Útil para validar parâmetros antes de criar a conexão.
        """
        return await self._do_test(
            connection_type_id=data.connection_type_id,
            connection_params=data.connection_params,
            context="(unsaved)"
        )

    async def sync(self, id: int, background_tasks=None):
        obj = await self.get(id)
        
        # Validate that connection is not of type IMAGE
        validate_metadata_connection(obj, "metadata synchronization")
        
        # Check if sync is already in progress (prevent race conditions)
        if obj.sync_status in ('running', 'pending'):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Synchronization for connection '{obj.name}' is already in progress (status: {obj.sync_status}). "
                       f"Wait for it to complete before starting a new sync."
            )
        
        obj.sync_status = 'pending'
        obj.last_sync_time = datetime.now()
        await self.db.commit()
        
        # Use Airflow for job orchestration (no fallback)
        airflow = get_airflow_client()
        dag_run = await airflow.trigger_sync_dag(connection_id=id)
        
        logger.info(f"Triggered Airflow DAG for connection {id}. Run ID: {dag_run.get('dag_run_id')}")
        
        return {
            "message": f"Metadata synchronization for connection '{obj.name}' has been initiated via Airflow",
            "dag_run_id": dag_run.get("dag_run_id"),
            "orchestrator": "airflow"
        }