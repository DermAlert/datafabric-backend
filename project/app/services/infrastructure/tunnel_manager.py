"""
Serviço de Gerenciamento de Túneis SSH
=======================================

Gerencia túneis SSH para conexões de dados em redes privadas.
Os túneis são criados sob demanda (lazy) e mantidos ativos enquanto necessário.

Integração:
-----------
Este serviço é usado por:
- TrinoManager (ao criar catálogos para bancos atrás de SSH)
- ConnectionManager (ao testar conexões tuneladas)
- StorageBrowserService (ao acessar storage via túnel)

Funcionamento Docker:
---------------------
O túnel faz bind em 0.0.0.0 no container do backend para ser acessível
por outros containers na mesma rede Docker (ex: Trino).

- Trino (outro container): acessa via ``BACKEND_HOSTNAME:porta_local``
- Backend (mesmo container): acessa via ``127.0.0.1:porta_local``

Exemplo:
--------
    from app.services.infrastructure.tunnel_manager import get_tunnel_manager

    manager = get_tunnel_manager()

    # Obter endereço efetivo (cria túnel se necessário)
    host, port = await manager.get_or_create_tunnel(
        connection_id=5,
        tunnel_config={"ssh_host": "bastion.com", "ssh_port": 22, ...},
        remote_host="db-interno.local",
        remote_port=5432,
    )

    # Para Trino (outro container Docker):
    trino_host = manager.get_trino_tunnel_host()  # "dermalert-backend"
"""

import os
import asyncio
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

from ...utils.logger import logger


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class TunnelInfo:
    """Informações de um túnel SSH ativo."""
    connection_id: int
    local_host: str
    local_port: int
    remote_host: str
    remote_port: int
    ssh_host: str
    ssh_port: int
    tunnel_server: Any  # SSHTunnelForwarder
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_health_check: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "active"  # active, error, reconnecting


# =============================================================================
# TUNNEL MANAGER
# =============================================================================

class TunnelManager:
    """
    Gerenciador de túneis SSH para conexões de dados.
    
    Mantém um registro de túneis ativos por connection_id e gerencia
    o ciclo de vida completo: criação, health check e encerramento.
    
    Thread-safe: usa lock para acesso ao registro de túneis.
    """
    
    # Bind address: 0.0.0.0 para ser acessível por outros containers Docker
    LOCAL_BIND_HOST = "0.0.0.0"
    
    # Hostname Docker do container backend (como Trino vai enxergar o túnel)
    BACKEND_DOCKER_HOSTNAME = os.getenv("TUNNEL_BACKEND_HOSTNAME", "dermalert-backend")
    
    # Timeouts e configurações
    SSH_CONNECT_TIMEOUT = int(os.getenv("SSH_CONNECT_TIMEOUT", "10"))
    SSH_KEEPALIVE_INTERVAL = int(os.getenv("SSH_KEEPALIVE_INTERVAL", "30"))
    TUNNEL_MAX_RETRIES = int(os.getenv("TUNNEL_MAX_RETRIES", "1"))
    
    def __init__(self):
        self._active_tunnels: Dict[int, TunnelInfo] = {}
        self._lock = threading.Lock()
    
    # =========================================================================
    # CORE: Criar / Obter túnel
    # =========================================================================
    
    async def get_or_create_tunnel(
        self,
        connection_id: int,
        tunnel_config: Dict[str, Any],
        remote_host: str,
        remote_port: int,
    ) -> Tuple[str, int]:
        """
        Obtém túnel ativo ou cria um novo.
        
        Args:
            connection_id: ID da conexão no banco
            tunnel_config: Configuração do túnel (ssh_host, ssh_port, etc.)
            remote_host: Host de destino (o banco/storage privado)
            remote_port: Porta de destino
            
        Returns:
            Tupla (local_host, local_port) do túnel.
            local_host é sempre ``0.0.0.0`` (bind address).
            
        Raises:
            TunnelError: Se não foi possível criar o túnel
        """
        # Verificar se já existe túnel ativo para esta conexão
        with self._lock:
            existing = self._active_tunnels.get(connection_id)
            if existing and existing.status == "active":
                if self._is_tunnel_alive(existing):
                    logger.debug(
                        f"[TunnelManager] Reusing existing tunnel for connection {connection_id} "
                        f"→ {existing.local_host}:{existing.local_port}"
                    )
                    return existing.local_host, existing.local_port
                else:
                    # Túnel existente mas morto, limpar
                    logger.warning(
                        f"[TunnelManager] Stale tunnel for connection {connection_id}, recreating"
                    )
                    self._stop_tunnel_unsafe(connection_id)
        
        # Criar novo túnel (fora do lock, pois pode demorar)
        tunnel_info = await self._create_tunnel(
            connection_id, tunnel_config, remote_host, remote_port
        )
        
        # Registrar
        with self._lock:
            self._active_tunnels[connection_id] = tunnel_info
        
        logger.info(
            f"[TunnelManager] Tunnel created for connection {connection_id}: "
            f"{tunnel_info.ssh_host}:{tunnel_info.ssh_port} → "
            f"{remote_host}:{remote_port} via local {tunnel_info.local_host}:{tunnel_info.local_port}"
        )
        
        return tunnel_info.local_host, tunnel_info.local_port
    
    def get_trino_tunnel_host(self) -> str:
        """
        Retorna o hostname que o Trino (outro container Docker) deve usar.
        
        O Trino não pode usar 0.0.0.0 nem 127.0.0.1 — precisa do hostname
        Docker do container do backend na shared-network.
        """
        return self.BACKEND_DOCKER_HOSTNAME
    
    def get_tunnel_port(self, connection_id: int) -> Optional[int]:
        """Retorna a porta local do túnel ativo, ou None se não existir."""
        with self._lock:
            tunnel = self._active_tunnels.get(connection_id)
            if tunnel and tunnel.status == "active":
                return tunnel.local_port
        return None
    
    # =========================================================================
    # LIFECYCLE: Parar / Cleanup
    # =========================================================================
    
    async def stop_tunnel(self, connection_id: int) -> None:
        """Para o túnel de uma conexão específica."""
        with self._lock:
            self._stop_tunnel_unsafe(connection_id)
    
    async def shutdown_all(self) -> None:
        """Para todos os túneis ativos. Usar no shutdown do serviço."""
        with self._lock:
            connection_ids = list(self._active_tunnels.keys())
            for cid in connection_ids:
                self._stop_tunnel_unsafe(cid)
            logger.info(
                f"[TunnelManager] Shutdown complete. Stopped {len(connection_ids)} tunnel(s)."
            )
    
    def _stop_tunnel_unsafe(self, connection_id: int) -> None:
        """
        Para um túnel sem lock (deve ser chamado dentro de ``with self._lock``).
        """
        tunnel = self._active_tunnels.pop(connection_id, None)
        if tunnel and tunnel.tunnel_server:
            try:
                tunnel.tunnel_server.stop()
                logger.info(f"[TunnelManager] Tunnel stopped for connection {connection_id}")
            except Exception as e:
                logger.warning(
                    f"[TunnelManager] Error stopping tunnel for connection {connection_id}: {e}"
                )
    
    # =========================================================================
    # HEALTH CHECK
    # =========================================================================
    
    async def health_check(self) -> Dict[int, str]:
        """
        Verifica a saúde de todos os túneis ativos.
        
        Returns:
            Dict com connection_id → status ("active", "error", "reconnected")
        """
        results = {}
        with self._lock:
            for cid, tunnel in list(self._active_tunnels.items()):
                if self._is_tunnel_alive(tunnel):
                    tunnel.last_health_check = datetime.now(timezone.utc)
                    results[cid] = "active"
                else:
                    tunnel.status = "error"
                    results[cid] = "error"
                    logger.warning(f"[TunnelManager] Tunnel unhealthy for connection {cid}")
        return results
    
    def get_active_tunnels_info(self) -> Dict[int, Dict[str, Any]]:
        """Retorna informações sobre túneis ativos (para API de monitoramento)."""
        with self._lock:
            return {
                cid: {
                    "connection_id": t.connection_id,
                    "local_port": t.local_port,
                    "remote_host": t.remote_host,
                    "remote_port": t.remote_port,
                    "ssh_host": t.ssh_host,
                    "status": t.status,
                    "started_at": t.started_at.isoformat(),
                    "last_health_check": t.last_health_check.isoformat(),
                }
                for cid, t in self._active_tunnels.items()
            }
    
    # =========================================================================
    # INTERNAL: Criação de túnel
    # =========================================================================
    
    async def _create_tunnel(
        self,
        connection_id: int,
        tunnel_config: Dict[str, Any],
        remote_host: str,
        remote_port: int,
    ) -> TunnelInfo:
        """
        Cria um túnel SSH usando sshtunnel (executado em thread separada
        porque o sshtunnel é síncrono).
        """
        def _create_sync():
            import paramiko
            # Shim de compatibilidade: sshtunnel referencia paramiko.DSSKey,
            # removido no paramiko 4.0 (DSA deprecated). Criamos um stub para evitar
            # AttributeError durante a inicialização do sshtunnel.
            if not hasattr(paramiko, "DSSKey"):
                paramiko.DSSKey = type("DSSKey", (), {})  # type: ignore[attr-defined]
            
            from sshtunnel import SSHTunnelForwarder
            import io
            
            ssh_host = tunnel_config.get("ssh_host")
            ssh_port = int(tunnel_config.get("ssh_port", 22))
            ssh_username = tunnel_config.get("ssh_username")
            auth_method = tunnel_config.get("auth_method", "password")
            
            if not ssh_host or not ssh_username:
                raise TunnelError(
                    f"Missing required tunnel config: ssh_host={ssh_host}, ssh_username={ssh_username}"
                )
            
            # Configurar autenticação
            kwargs: Dict[str, Any] = {
                "ssh_address_or_host": (ssh_host, ssh_port),
                "ssh_username": ssh_username,
                "remote_bind_address": (remote_host, remote_port),
                "local_bind_address": (self.LOCAL_BIND_HOST, 0),  # Porta automática
                "set_keepalive": self.SSH_KEEPALIVE_INTERVAL,
                "mute_exceptions": False,
            }
            
            if auth_method == "private_key":
                # Autenticação por chave privada
                ssh_private_key = tunnel_config.get("ssh_private_key", "")
                ssh_passphrase = tunnel_config.get("ssh_passphrase")
                
                if not ssh_private_key:
                    raise TunnelError("Private key authentication selected but ssh_private_key is empty")
                
                # Carregar chave privada da string
                pkey = paramiko.RSAKey.from_private_key(
                    io.StringIO(ssh_private_key),
                    password=ssh_passphrase if ssh_passphrase else None,
                )
                kwargs["ssh_pkey"] = pkey
                
            elif auth_method == "password":
                ssh_password = tunnel_config.get("ssh_password", "")
                if not ssh_password:
                    raise TunnelError("Password authentication selected but ssh_password is empty")
                kwargs["ssh_password"] = ssh_password
                
            else:
                raise TunnelError(f"Unsupported auth_method: {auth_method}")
            
            # Criar e iniciar o túnel com timeout controlado
            import socket
            
            # Forçar timeout no socket para evitar hang em credenciais erradas.
            # O sshtunnel/paramiko não expõe timeout diretamente, então usamos
            # socket.setdefaulttimeout para afetar todas as conexões nesta thread.
            old_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(float(self.SSH_CONNECT_TIMEOUT))
            
            try:
                server = SSHTunnelForwarder(**kwargs)
                
                retries = 0
                last_error = None
                while retries < self.TUNNEL_MAX_RETRIES:
                    try:
                        server.start()
                        # Verificar se realmente iniciou
                        if server.local_bind_port:
                            return server
                    except Exception as e:
                        last_error = e
                        retries += 1
                        logger.warning(
                            f"[TunnelManager] Tunnel creation attempt {retries}/{self.TUNNEL_MAX_RETRIES} "
                            f"failed for connection {connection_id}: {e}"
                        )
                        try:
                            server.stop()
                        except Exception:
                            pass
                        
                        if retries < self.TUNNEL_MAX_RETRIES:
                            import time
                            time.sleep(min(2 * retries, 5))
                
                raise TunnelError(
                    f"Failed to create tunnel after {self.TUNNEL_MAX_RETRIES} attempts: {last_error}"
                )
            finally:
                # Restaurar timeout padrão do socket
                socket.setdefaulttimeout(old_timeout)
        
        try:
            # Hard timeout: se _create_sync travar (ex: server.stop() bloqueante),
            # retorna erro em vez de deixar o request HTTP pendurado.
            overall_timeout = self.SSH_CONNECT_TIMEOUT + 10
            server = await asyncio.wait_for(
                asyncio.to_thread(_create_sync),
                timeout=overall_timeout,
            )
        except asyncio.TimeoutError:
            raise TunnelError(
                f"SSH tunnel creation timed out after {overall_timeout}s "
                f"(host={tunnel_config.get('ssh_host')}, user={tunnel_config.get('ssh_username')})"
            )
        except TunnelError:
            raise
        except Exception as e:
            raise TunnelError(f"Unexpected error creating SSH tunnel: {e}")
        
        return TunnelInfo(
            connection_id=connection_id,
            local_host=self.LOCAL_BIND_HOST,
            local_port=server.local_bind_port,
            remote_host=remote_host,
            remote_port=remote_port,
            ssh_host=tunnel_config.get("ssh_host", ""),
            ssh_port=int(tunnel_config.get("ssh_port", 22)),
            tunnel_server=server,
        )
    
    def _is_tunnel_alive(self, tunnel: TunnelInfo) -> bool:
        """Verifica se o túnel SSH ainda está vivo."""
        try:
            server = tunnel.tunnel_server
            if server is None:
                return False
            return server.is_active
        except Exception:
            return False


# =============================================================================
# EXCEÇÕES
# =============================================================================

class TunnelError(Exception):
    """Erro ao criar ou gerenciar túnel SSH."""
    pass


# =============================================================================
# HELPERS: Extração de config de túnel dos connection_params
# =============================================================================

def has_tunnel_config(connection_params: Dict[str, Any]) -> bool:
    """Verifica se os connection_params têm configuração de túnel ativa."""
    tunnel = connection_params.get("tunnel")
    if not tunnel or not isinstance(tunnel, dict):
        return False
    return tunnel.get("enabled", False) is True


def extract_tunnel_config(connection_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extrai a configuração de túnel dos connection_params.
    
    Returns:
        Dict com a config do túnel, ou None se não estiver habilitado.
    """
    if not has_tunnel_config(connection_params):
        return None
    return connection_params["tunnel"]


def get_remote_address_from_params(
    connection_type: str, connection_params: Dict[str, Any]
) -> Tuple[str, int]:
    """
    Extrai o host:porta de destino (remoto) dos connection_params,
    baseado no tipo de conexão.
    
    Para bancos (postgresql, mysql): usa host + port.
    Para storage (s3, deltalake): usa o host do endpoint.
    """
    conn_type = connection_type.lower()
    
    if conn_type in ("postgresql", "postgres"):
        return (
            connection_params.get("host", "localhost"),
            int(connection_params.get("port", 5432)),
        )
    elif conn_type == "mysql":
        return (
            connection_params.get("host", "localhost"),
            int(connection_params.get("port", 3306)),
        )
    elif conn_type in ("s3", "minio", "deltalake", "delta"):
        # Extrair host e porta do endpoint URL
        endpoint = (
            connection_params.get("endpoint_url")
            or connection_params.get("s3a_endpoint")
            or connection_params.get("endpoint")
            or ""
        )
        return _parse_endpoint(endpoint)
    
    # Fallback genérico
    return (
        connection_params.get("host", "localhost"),
        int(connection_params.get("port", 0)),
    )


def _parse_endpoint(endpoint: str) -> Tuple[str, int]:
    """Parseia um endpoint URL em (host, port)."""
    from urllib.parse import urlparse
    
    if not endpoint:
        return ("localhost", 443)
    
    # Adicionar scheme se não tiver para urlparse funcionar
    if "://" not in endpoint:
        endpoint = f"http://{endpoint}"
    
    parsed = urlparse(endpoint)
    host = parsed.hostname or "localhost"
    port = parsed.port
    
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    
    return (host, port)


# =============================================================================
# INSTÂNCIA SINGLETON
# =============================================================================

_tunnel_manager_instance: Optional[TunnelManager] = None


def get_tunnel_manager() -> TunnelManager:
    """Retorna a instância singleton do TunnelManager."""
    global _tunnel_manager_instance
    if _tunnel_manager_instance is None:
        _tunnel_manager_instance = TunnelManager()
    return _tunnel_manager_instance
