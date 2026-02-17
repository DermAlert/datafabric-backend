"""
Serviço de Gerenciamento de Credenciais
=======================================

Este módulo fornece uma camada de abstração para gerenciar credenciais
de forma segura em todo o Data Fabric. Centraliza as operações de:

- Criptografia de credenciais no armazenamento
- Descriptografia sob demanda para uso
- Mascaramento para logging e APIs
- Auditoria de acesso a credenciais

Integração:
-----------
Este serviço deve ser usado por:
- DataConnectionService (ao criar/atualizar conexões)
- TrinoManager (ao criar catálogos)
- SparkManager (ao configurar sessões)
- BronzeIngestionService (ao processar ingestões)
- Qualquer serviço que precise acessar credenciais

Exemplo:
--------
    from app.services.credential_service import CredentialService
    
    service = CredentialService()
    
    # Ao salvar conexão
    safe_params = service.encrypt_for_storage(params, "postgresql")
    
    # Ao usar conexão
    decrypted_params = service.decrypt_for_use(safe_params)
    
    # Para logging
    masked_params = service.mask_for_logging(params)
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from app.core.credential_encryption import (
    CredentialEncryption,
    get_credential_encryption,
    SENSITIVE_FIELDS_BY_CONNECTION_TYPE,
    TUNNEL_SENSITIVE_FIELDS,
)

logger = logging.getLogger(__name__)


class CredentialService:
    """
    Serviço centralizado para gerenciamento seguro de credenciais.
    
    Este serviço atua como ponto único de gerenciamento de credenciais,
    garantindo que todas as operações sensíveis sejam tratadas de forma
    consistente em toda a aplicação.
    """
    
    def __init__(self, crypto: Optional[CredentialEncryption] = None):
        """
        Inicializa o serviço de credenciais.
        
        Args:
            crypto: Instância de CredentialEncryption (opcional).
                   Se não fornecida, usa a instância singleton.
        """
        self._crypto = crypto or get_credential_encryption()
        self._access_log: List[Dict[str, Any]] = []
    
    @property
    def is_encryption_enabled(self) -> bool:
        """Verifica se a criptografia está habilitada."""
        return self._crypto.is_encryption_enabled
    
    def get_sensitive_fields_for_type(self, connection_type: str) -> List[str]:
        """
        Retorna os campos sensíveis para um tipo de conexão.
        
        Args:
            connection_type: Nome do tipo de conexão
            
        Returns:
            Lista de nomes de campos sensíveis
        """
        return self._crypto.get_sensitive_fields(connection_type)
    
    def encrypt_for_storage(
        self, 
        connection_params: Dict[str, Any], 
        connection_type: str,
        connection_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Prepara os parâmetros de conexão para armazenamento seguro.
        
        Esta função deve ser chamada ANTES de salvar connection_params
        no banco de dados.
        
        Args:
            connection_params: Parâmetros de conexão em texto claro
            connection_type: Tipo de conexão (postgres, mysql, s3, etc.)
            connection_id: ID da conexão (para auditoria)
            
        Returns:
            Parâmetros com campos sensíveis criptografados
        """
        if not connection_params:
            return connection_params
        
        encrypted_params, encrypted_fields = self._crypto.encrypt_connection_params(
            connection_params, 
            connection_type
        )
        
        # Log de auditoria (sem valores sensíveis)
        if encrypted_fields:
            self._log_access(
                action="encrypt",
                connection_type=connection_type,
                connection_id=connection_id,
                fields=encrypted_fields
            )
        
        return encrypted_params
    
    def decrypt_for_use(
        self, 
        connection_params: Dict[str, Any],
        connection_id: Optional[int] = None,
        purpose: str = "unknown"
    ) -> Dict[str, Any]:
        """
        Descriptografa parâmetros de conexão para uso.
        
        Esta função deve ser chamada quando os parâmetros são necessários
        para operações que requerem as credenciais em texto claro (ex: 
        conexão com banco, criação de catálogo Trino).
        
        Args:
            connection_params: Parâmetros de conexão (possivelmente criptografados)
            connection_id: ID da conexão (para auditoria)
            purpose: Propósito do acesso (para auditoria)
            
        Returns:
            Parâmetros com campos sensíveis descriptografados
        """
        if not connection_params:
            return connection_params
        
        decrypted_params = self._crypto.decrypt_connection_params(connection_params)
        
        # Log de auditoria (inclui campos do nível raiz e do bloco tunnel)
        decrypted_fields = [
            key for key, value in connection_params.items()
            if key != "tunnel" and value and isinstance(value, str) and self._crypto.is_encrypted(value)
        ]
        tunnel_data = connection_params.get("tunnel")
        if tunnel_data and isinstance(tunnel_data, dict):
            decrypted_fields.extend(
                f"tunnel.{key}" for key, value in tunnel_data.items()
                if value and isinstance(value, str) and self._crypto.is_encrypted(value)
            )
        
        if decrypted_fields:
            self._log_access(
                action="decrypt",
                connection_id=connection_id,
                fields=decrypted_fields,
                purpose=purpose
            )
        
        return decrypted_params
    
    def mask_for_logging(
        self, 
        connection_params: Dict[str, Any],
        connection_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Mascara parâmetros sensíveis para logging seguro.
        
        Use esta função sempre que precisar logar parâmetros de conexão.
        
        Args:
            connection_params: Parâmetros de conexão
            connection_type: Tipo de conexão (opcional, melhora detecção)
            
        Returns:
            Parâmetros com valores sensíveis mascarados
        """
        return self._crypto.mask_connection_params(connection_params, connection_type)
    
    def mask_for_api_response(
        self, 
        connection_params: Dict[str, Any],
        connection_type: Optional[str] = None,
        include_encrypted_indicator: bool = True
    ) -> Dict[str, Any]:
        """
        Prepara parâmetros para resposta de API.
        
        Mascara valores sensíveis e opcionalmente indica quais campos
        estão criptografados.
        
        Args:
            connection_params: Parâmetros de conexão
            connection_type: Tipo de conexão
            include_encrypted_indicator: Se True, adiciona indicador de criptografia
            
        Returns:
            Parâmetros seguros para retorno em API
        """
        if not connection_params:
            return connection_params
        
        safe_params = {}
        sensitive_fields = self._crypto.get_sensitive_fields(connection_type) if connection_type else []
        sensitive_fields_lower = [f.lower() for f in sensitive_fields]
        
        for key, value in connection_params.items():
            if key == "tunnel":
                continue  # Tratado separadamente abaixo
            
            key_lower = key.lower()
            
            # Verifica se é campo sensível:
            # 1. Está na lista de campos sensíveis do tipo de conexão
            # 2. Valor já está criptografado (prefixo ENC:v1:)
            is_sensitive = (
                key_lower in sensitive_fields_lower or
                (isinstance(value, str) and self._crypto.is_encrypted(value))
            )
            
            if is_sensitive and value:
                if include_encrypted_indicator and self._crypto.is_encrypted(str(value)):
                    safe_params[key] = "[ENCRYPTED]"
                else:
                    safe_params[key] = "[REDACTED]"
            else:
                safe_params[key] = value
        
        # Mascarar campos dentro de "tunnel" (se presente)
        tunnel_data = connection_params.get("tunnel")
        if tunnel_data and isinstance(tunnel_data, dict):
            tunnel_sensitive_lower = [f.lower() for f in TUNNEL_SENSITIVE_FIELDS]
            safe_tunnel = {}
            for key, value in tunnel_data.items():
                key_lower = key.lower()
                is_sensitive = (
                    key_lower in tunnel_sensitive_lower or
                    (isinstance(value, str) and self._crypto.is_encrypted(value))
                )
                if is_sensitive and value:
                    if include_encrypted_indicator and self._crypto.is_encrypted(str(value)):
                        safe_tunnel[key] = "[ENCRYPTED]"
                    else:
                        safe_tunnel[key] = "[REDACTED]"
                else:
                    safe_tunnel[key] = value
            safe_params["tunnel"] = safe_tunnel
        
        return safe_params
    
    def validate_encryption_status(
        self, 
        connection_params: Dict[str, Any],
        connection_type: str
    ) -> Dict[str, bool]:
        """
        Valida se os campos sensíveis estão devidamente criptografados.
        
        Útil para auditoria e verificação de conformidade.
        
        Args:
            connection_params: Parâmetros de conexão
            connection_type: Tipo de conexão
            
        Returns:
            Dicionário com status de criptografia por campo sensível
        """
        sensitive_fields = self._crypto.get_sensitive_fields(connection_type)
        sensitive_fields_lower = [f.lower() for f in sensitive_fields]
        status = {}
        
        for key, value in connection_params.items():
            # Verifica apenas campos que estão na lista de sensíveis
            if key.lower() in sensitive_fields_lower and value:
                status[key] = self._crypto.is_encrypted(str(value))
        
        return status
    
    def _log_access(
        self, 
        action: str,
        connection_type: Optional[str] = None,
        connection_id: Optional[int] = None,
        fields: Optional[List[str]] = None,
        purpose: str = "unknown"
    ):
        """
        Registra acesso a credenciais para auditoria.
        
        Em produção, este log deve ser enviado para um sistema de
        auditoria centralizado (ex: SIEM, Elasticsearch).
        """
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "action": action,
            "connection_type": connection_type,
            "connection_id": connection_id,
            "fields_accessed": fields or [],
            "purpose": purpose
        }
        
        self._access_log.append(log_entry)
        
        # Log estruturado para sistemas de monitoramento
        logger.info(
            f"[CredentialAudit] {action.upper()} | "
            f"connection_id={connection_id} | "
            f"type={connection_type} | "
            f"fields={fields} | "
            f"purpose={purpose}"
        )
    
    def get_audit_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Retorna os últimos registros de auditoria."""
        return self._access_log[-limit:]


# ============================================================================
# INSTÂNCIA SINGLETON
# ============================================================================

_credential_service_instance: Optional[CredentialService] = None


def get_credential_service() -> CredentialService:
    """
    Retorna a instância singleton do serviço de credenciais.
    """
    global _credential_service_instance
    
    if _credential_service_instance is None:
        _credential_service_instance = CredentialService()
    
    return _credential_service_instance


# ============================================================================
# DECORADORES PARA SEGURANÇA
# ============================================================================

def secure_credentials(connection_type_param: str = "connection_type"):
    """
    Decorador para criptografar credenciais automaticamente.
    
    Uso:
        @secure_credentials("connection_type")
        async def create_connection(params, connection_type):
            # params já estará criptografado
            ...
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            service = get_credential_service()
            
            # Busca connection_params nos argumentos
            if "connection_params" in kwargs:
                conn_type = kwargs.get(connection_type_param, "unknown")
                kwargs["connection_params"] = service.encrypt_for_storage(
                    kwargs["connection_params"],
                    conn_type
                )
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator
