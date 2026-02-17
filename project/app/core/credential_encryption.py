"""
Serviço de Criptografia de Credenciais
======================================

Este módulo implementa criptografia AES-256-GCM para proteção de credenciais
sensíveis no Data Fabric. Segue as melhores práticas de segurança:

- Criptografia AES-256-GCM (autenticada)
- Derivação de chave usando PBKDF2
- Salt único por credencial
- IV (nonce) único por operação de criptografia
- Rotação de chaves suportada

Uso:
----
    from app.core.credential_encryption import CredentialEncryption
    
    crypto = CredentialEncryption()
    encrypted = crypto.encrypt("minha_senha_secreta")
    decrypted = crypto.decrypt(encrypted)
"""

import os
import base64
import json
import hashlib
import logging
from typing import Dict, Any, Optional, List, Tuple
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)



SENSITIVE_FIELDS_BY_CONNECTION_TYPE: Dict[str, List[str]] = {
    # ══════════════════════════════════════════════════════════════════════════
    # BANCOS DE DADOS RELACIONAIS
    # ══════════════════════════════════════════════════════════════════════════
    "postgresql": ["password"],
    "mysql": ["password"],
    
    # ══════════════════════════════════════════════════════════════════════════
    # DELTA LAKE / OBJECT STORAGE
    # ══════════════════════════════════════════════════════════════════════════
    "deltalake": ["s3a_access_key", "s3a_secret_key"],
    "s3": ["access_key", "secret_key", "session_token"],
    
    # ══════════════════════════════════════════════════════════════════════════
    # CLOUD PROVIDERS
    # ══════════════════════════════════════════════════════════════════════════
    "azure_storage": ["access_key", "client_secret", "sas_token"],
    "gcs": ["service_account_key", "access_token"],
}

# Campos sensíveis dentro do bloco "tunnel" de connection_params.
# Aplicáveis a QUALQUER tipo de conexão que tenha tunnel configurado.
TUNNEL_SENSITIVE_FIELDS: List[str] = [
    "ssh_password",
    "ssh_private_key",
    "ssh_passphrase",
]


class CredentialEncryptionError(Exception):
    """Exceção base para erros de criptografia de credenciais."""
    pass


class CredentialEncryption:
    """
    Serviço de criptografia para credenciais sensíveis.
    
    Utiliza AES-256-GCM que fornece:
    - Confidencialidade (criptografia)
    - Integridade (autenticação)
    - Proteção contra tampering
    
    Attributes:
        ALGORITHM: Algoritmo usado (AES-256-GCM)
        SALT_SIZE: Tamanho do salt em bytes (16)
        NONCE_SIZE: Tamanho do nonce/IV em bytes (12)
        KEY_SIZE: Tamanho da chave em bytes (32 = 256 bits)
        ITERATIONS: Iterações do PBKDF2 (100000)
    """
    
    ALGORITHM = "AES-256-GCM"
    SALT_SIZE = 16  # 128 bits
    NONCE_SIZE = 12  # 96 bits (recomendado para GCM)
    KEY_SIZE = 32   # 256 bits
    ITERATIONS = 100000  # PBKDF2 iterations
    
    # Prefixo para identificar valores criptografados
    ENCRYPTED_PREFIX = "ENC:v1:"
    
    def __init__(self, master_key: Optional[str] = None):
        """
        Inicializa o serviço de criptografia.
        
        Args:
            master_key: Chave mestra para criptografia. Se não fornecida,
                       será lida da variável de ambiente CREDENTIAL_ENCRYPTION_KEY.
        
        Raises:
            CredentialEncryptionError: Se a chave mestra não estiver configurada.
        """
        self._master_key = master_key or os.getenv("CREDENTIAL_ENCRYPTION_KEY")
        
        if not self._master_key:
            logger.warning(
                "CREDENTIAL_ENCRYPTION_KEY não configurada! "
                "As credenciais NÃO serão criptografadas. "
                "Configure esta variável em produção."
            )
            self._encryption_enabled = False
        else:
            self._encryption_enabled = True
            # Valida que a chave tem força suficiente
            if len(self._master_key) < 32:
                logger.warning(
                    "CREDENTIAL_ENCRYPTION_KEY deve ter pelo menos 32 caracteres "
                    "para segurança adequada."
                )
    
    @property
    def is_encryption_enabled(self) -> bool:
        """Retorna True se a criptografia está habilitada."""
        return self._encryption_enabled
    
    def _derive_key(self, salt: bytes) -> bytes:
        """
        Deriva uma chave de criptografia a partir da chave mestra usando PBKDF2.
        
        Args:
            salt: Salt único para esta derivação
            
        Returns:
            Chave derivada de 256 bits
        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=self.KEY_SIZE,
            salt=salt,
            iterations=self.ITERATIONS,
            backend=default_backend()
        )
        return kdf.derive(self._master_key.encode('utf-8'))
    
    def encrypt(self, plaintext: str) -> str:
        """
        Criptografa um valor sensível.
        
        Args:
            plaintext: Texto em claro para criptografar
            
        Returns:
            String criptografada no formato: ENC:v1:base64(salt:nonce:ciphertext:tag)
            
        Raises:
            CredentialEncryptionError: Se a criptografia falhar
        """
        if not self._encryption_enabled:
            logger.debug("Criptografia desabilitada, retornando valor original")
            return plaintext
        
        if not plaintext:
            return plaintext
        
        # Se já está criptografado, retorna como está
        if plaintext.startswith(self.ENCRYPTED_PREFIX):
            return plaintext
        
        try:
            # Gera salt e nonce únicos
            salt = os.urandom(self.SALT_SIZE)
            nonce = os.urandom(self.NONCE_SIZE)
            
            # Deriva a chave
            key = self._derive_key(salt)
            
            # Criptografa usando AES-GCM
            aesgcm = AESGCM(key)
            ciphertext = aesgcm.encrypt(nonce, plaintext.encode('utf-8'), None)
            
            # Concatena salt + nonce + ciphertext e codifica em base64
            encrypted_data = base64.b64encode(salt + nonce + ciphertext).decode('utf-8')
            
            return f"{self.ENCRYPTED_PREFIX}{encrypted_data}"
            
        except Exception as e:
            logger.error(f"Erro ao criptografar credencial: {e}")
            raise CredentialEncryptionError(f"Falha ao criptografar: {e}")
    
    def decrypt(self, ciphertext: str) -> str:
        """
        Descriptografa um valor criptografado.
        
        Args:
            ciphertext: Texto criptografado no formato ENC:v1:base64(...)
            
        Returns:
            Texto em claro original
            
        Raises:
            CredentialEncryptionError: Se a descriptografia falhar
        """
        if not self._encryption_enabled:
            return ciphertext
        
        if not ciphertext:
            return ciphertext
        
        # Se não está criptografado, retorna como está
        if not ciphertext.startswith(self.ENCRYPTED_PREFIX):
            logger.debug("Valor não está criptografado, retornando original")
            return ciphertext
        
        try:
            # Remove o prefixo e decodifica base64
            encrypted_data = base64.b64decode(
                ciphertext[len(self.ENCRYPTED_PREFIX):]
            )
            
            # Extrai salt, nonce e ciphertext
            salt = encrypted_data[:self.SALT_SIZE]
            nonce = encrypted_data[self.SALT_SIZE:self.SALT_SIZE + self.NONCE_SIZE]
            actual_ciphertext = encrypted_data[self.SALT_SIZE + self.NONCE_SIZE:]
            
            # Deriva a chave
            key = self._derive_key(salt)
            
            # Descriptografa
            aesgcm = AESGCM(key)
            plaintext = aesgcm.decrypt(nonce, actual_ciphertext, None)
            
            return plaintext.decode('utf-8')
            
        except Exception as e:
            logger.error(f"Erro ao descriptografar credencial: {e}")
            raise CredentialEncryptionError(f"Falha ao descriptografar: {e}")
    
    def is_encrypted(self, value: str) -> bool:
        """Verifica se um valor está criptografado."""
        return value and isinstance(value, str) and value.startswith(self.ENCRYPTED_PREFIX)
    
    def get_sensitive_fields(self, connection_type: str) -> List[str]:
        """
        Retorna a lista de campos sensíveis para um tipo de conexão.
        
        Os campos são baseados no schema de core.connection_types no banco,
        especificamente aqueles com format: "password" ou "textarea" (para JSON sensível).
        
        Args:
            connection_type: Tipo de conexão (postgresql, mysql, s3, etc.)
            
        Returns:
            Lista de nomes de campos que devem ser criptografados.
            Retorna lista vazia se o tipo de conexão não estiver mapeado.
        """
        conn_type_lower = connection_type.lower().replace("-", "_").replace(" ", "_")
        
        # Retorna apenas os campos específicos do tipo de conexão
        # Não usa fallback genérico - cada tipo deve estar explicitamente mapeado
        return SENSITIVE_FIELDS_BY_CONNECTION_TYPE.get(conn_type_lower, [])
    
    def encrypt_connection_params(
        self, 
        params: Dict[str, Any], 
        connection_type: str
    ) -> Tuple[Dict[str, Any], List[str]]:
        """
        Criptografa campos sensíveis em connection_params.
        
        Apenas os campos definidos em SENSITIVE_FIELDS_BY_CONNECTION_TYPE para
        o tipo de conexão especificado serão criptografados.
        Também criptografa campos sensíveis no bloco ``tunnel`` (se presente).
        
        Args:
            params: Dicionário de parâmetros de conexão
            connection_type: Tipo de conexão (deve estar em SENSITIVE_FIELDS_BY_CONNECTION_TYPE)
            
        Returns:
            Tupla com (params_criptografados, lista_de_campos_criptografados)
        """
        if not params:
            return params, []
        
        sensitive_fields = self.get_sensitive_fields(connection_type)
        
        if not sensitive_fields:
            logger.warning(
                f"Tipo de conexão '{connection_type}' não está mapeado em "
                f"SENSITIVE_FIELDS_BY_CONNECTION_TYPE. Nenhum campo será criptografado."
            )
        
        encrypted_params = params.copy()
        encrypted_fields = []
        
        # 1. Criptografar campos do nível raiz
        sensitive_fields_lower = [f.lower() for f in sensitive_fields]
        
        for key, value in params.items():
            if key == "tunnel":
                continue  # Tratado separadamente abaixo
            if key.lower() in sensitive_fields_lower and value and isinstance(value, str):
                encrypted_params[key] = self.encrypt(value)
                encrypted_fields.append(key)
                logger.debug(f"Campo '{key}' criptografado com sucesso")
        
        # 2. Criptografar campos sensíveis dentro de "tunnel" (se presente)
        tunnel_data = params.get("tunnel")
        if tunnel_data and isinstance(tunnel_data, dict):
            encrypted_tunnel, tunnel_encrypted_fields = self._encrypt_tunnel_params(tunnel_data)
            encrypted_params["tunnel"] = encrypted_tunnel
            encrypted_fields.extend(f"tunnel.{f}" for f in tunnel_encrypted_fields)
        
        if encrypted_fields:
            logger.info(
                f"Criptografados {len(encrypted_fields)} campos sensíveis "
                f"para conexão tipo '{connection_type}': {encrypted_fields}"
            )
        
        return encrypted_params, encrypted_fields
    
    def _encrypt_tunnel_params(
        self, tunnel_params: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], List[str]]:
        """
        Criptografa campos sensíveis dentro do bloco tunnel.
        
        Args:
            tunnel_params: Dicionário do bloco tunnel
            
        Returns:
            Tupla com (tunnel_criptografado, lista_de_campos_criptografados)
        """
        encrypted = tunnel_params.copy()
        encrypted_fields = []
        tunnel_sensitive_lower = [f.lower() for f in TUNNEL_SENSITIVE_FIELDS]
        
        for key, value in tunnel_params.items():
            if key.lower() in tunnel_sensitive_lower and value and isinstance(value, str):
                encrypted[key] = self.encrypt(value)
                encrypted_fields.append(key)
                logger.debug(f"Campo tunnel '{key}' criptografado com sucesso")
        
        return encrypted, encrypted_fields
    
    def decrypt_connection_params(
        self, 
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Descriptografa todos os campos criptografados em connection_params.
        Também descriptografa campos dentro do bloco ``tunnel`` (se presente).
        
        Args:
            params: Dicionário de parâmetros (possivelmente criptografados)
            
        Returns:
            Dicionário com valores descriptografados
        """
        if not params:
            return params
        
        decrypted_params = params.copy()
        
        for key, value in params.items():
            if key == "tunnel":
                continue  # Tratado separadamente abaixo
            if value and isinstance(value, str) and self.is_encrypted(value):
                decrypted_params[key] = self.decrypt(value)
                logger.debug(f"Campo '{key}' descriptografado")
        
        # Descriptografar campos dentro de "tunnel" (se presente)
        tunnel_data = params.get("tunnel")
        if tunnel_data and isinstance(tunnel_data, dict):
            decrypted_tunnel = tunnel_data.copy()
            for key, value in tunnel_data.items():
                if value and isinstance(value, str) and self.is_encrypted(value):
                    decrypted_tunnel[key] = self.decrypt(value)
                    logger.debug(f"Campo tunnel '{key}' descriptografado")
            decrypted_params["tunnel"] = decrypted_tunnel
        
        return decrypted_params
    
    def mask_sensitive_value(self, value: str, show_chars: int = 1) -> str:
        """
        Mascara um valor sensível para logging seguro.
        
        Args:
            value: Valor a mascarar
            show_chars: Número de caracteres a mostrar no início
            
        Returns:
            Valor mascarado (ex: "secr****")
        """
        if not value:
            return "****"
        if len(value) <= show_chars:
            return "****"
        return f"{value[:show_chars]}****"
    
    def mask_connection_params(
        self, 
        params: Dict[str, Any],
        connection_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Mascara campos sensíveis para logging seguro.
        Também mascara campos sensíveis dentro do bloco ``tunnel`` (se presente).
        
        Args:
            params: Dicionário de parâmetros
            connection_type: Tipo de conexão (recomendado fornecer para mascaramento preciso)
            
        Returns:
            Dicionário com valores sensíveis mascarados
        """
        if not params:
            return params
        
        masked_params = {}
        sensitive_fields = self.get_sensitive_fields(connection_type) if connection_type else []
        sensitive_fields_lower = [f.lower() for f in sensitive_fields]
        
        for key, value in params.items():
            if key == "tunnel":
                continue  # Tratado separadamente abaixo
            
            key_lower = key.lower()
            
            # Verifica se é sensível por:
            # 1. Está na lista de campos sensíveis do tipo de conexão
            # 2. Valor já está criptografado (prefixo ENC:v1:)
            is_sensitive = (
                key_lower in sensitive_fields_lower or
                (isinstance(value, str) and self.is_encrypted(value))
            )
            
            if is_sensitive and value:
                masked_params[key] = self.mask_sensitive_value(str(value))
            else:
                masked_params[key] = value
        
        # Mascarar campos dentro de "tunnel" (se presente)
        tunnel_data = params.get("tunnel")
        if tunnel_data and isinstance(tunnel_data, dict):
            tunnel_sensitive_lower = [f.lower() for f in TUNNEL_SENSITIVE_FIELDS]
            masked_tunnel = {}
            for key, value in tunnel_data.items():
                key_lower = key.lower()
                is_sensitive = (
                    key_lower in tunnel_sensitive_lower or
                    (isinstance(value, str) and self.is_encrypted(value))
                )
                if is_sensitive and value:
                    masked_tunnel[key] = self.mask_sensitive_value(str(value))
                else:
                    masked_tunnel[key] = value
            masked_params["tunnel"] = masked_tunnel
        
        return masked_params


# ============================================================================
# INSTÂNCIA SINGLETON
# ============================================================================

_credential_encryption_instance: Optional[CredentialEncryption] = None


def get_credential_encryption() -> CredentialEncryption:
    """
    Retorna a instância singleton do serviço de criptografia.
    
    Returns:
        Instância de CredentialEncryption
    """
    global _credential_encryption_instance
    
    if _credential_encryption_instance is None:
        _credential_encryption_instance = CredentialEncryption()
    
    return _credential_encryption_instance


# ============================================================================
# FUNÇÕES DE CONVENIÊNCIA
# ============================================================================

def encrypt_credential(value: str) -> str:
    """Criptografa um valor de credencial."""
    return get_credential_encryption().encrypt(value)


def decrypt_credential(value: str) -> str:
    """Descriptografa um valor de credencial."""
    return get_credential_encryption().decrypt(value)


def encrypt_params(params: Dict[str, Any], connection_type: str) -> Dict[str, Any]:
    """Criptografa campos sensíveis em parâmetros de conexão."""
    encrypted, _ = get_credential_encryption().encrypt_connection_params(params, connection_type)
    return encrypted


def decrypt_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Descriptografa campos em parâmetros de conexão."""
    return get_credential_encryption().decrypt_connection_params(params)


def mask_params(params: Dict[str, Any], connection_type: Optional[str] = None) -> Dict[str, Any]:
    """Mascara campos sensíveis para logging."""
    return get_credential_encryption().mask_connection_params(params, connection_type)
