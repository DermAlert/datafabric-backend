import logging
from typing import Dict, Any, Tuple, List, Optional
import uuid

from ...utils.logger import logger
from ..infrastructure.trino_manager import TrinoManager
from ..infrastructure.tunnel_manager import (
    get_tunnel_manager,
    has_tunnel_config,
    extract_tunnel_config,
    get_remote_address_from_params,
    TunnelError,
)


async def test_connection(
    connection_type: str, 
    connection_params: Dict[str, Any],
    connection_id: Optional[int] = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Test a connection to verify it works correctly using Trino (async with aiotrino).
    
    Se ``connection_params`` contiver configuração de túnel SSH, um túnel
    temporário será criado para o teste e encerrado ao final.
    
    Args:
        connection_type: Type of connection (postgres, delta, etc.)
        connection_params: Connection parameters (may include tunnel config)
        connection_id: Optional connection ID (used for tunnel management).
            If not provided, a temporary ID is generated for the tunnel.
        
    Returns:
        Tuple of (success, message, details)
    """
    # Para testes, SEMPRE usar um ID temporário negativo para não conflitar
    # com túneis de produção (que usam o connection_id real).
    # Isso garante que cada teste cria um túnel novo, validando as credenciais atuais.
    temp_tunnel_id = -(uuid.uuid4().int % 1_000_000)
    tunnel_started = False
    
    try:
        # Iniciar túnel SSH se configurado
        if has_tunnel_config(connection_params):
            try:
                tunnel_config = extract_tunnel_config(connection_params)
                remote_host, remote_port = get_remote_address_from_params(
                    connection_type, connection_params
                )
                tunnel_manager = get_tunnel_manager()
                
                # Se existe um túnel de produção para esta conexão, NÃO reutilizar.
                # O teste deve sempre criar um túnel novo para validar as credenciais.
                await tunnel_manager.get_or_create_tunnel(
                    connection_id=temp_tunnel_id,
                    tunnel_config=tunnel_config,
                    remote_host=remote_host,
                    remote_port=remote_port,
                )
                tunnel_started = True
                logger.info(f"[ConnectionManager] SSH tunnel started for test (temp_id={temp_tunnel_id})")
            except TunnelError as e:
                return False, f"SSH tunnel creation failed: {str(e)}", {"phase": "tunnel"}
        
        # Use a temporary test name
        test_catalog_base = f"test_conn_{uuid.uuid4().hex[:8]}"
        
        manager = TrinoManager()
        
        # Determinar o connection_id para o tunnel (se houver)
        effective_conn_id = temp_tunnel_id if tunnel_started else None
        
        # Try to create catalog (using async version)
        # Passa temp_tunnel_id para que o TrinoManager encontre o túnel ativo
        success = await manager.ensure_catalog_exists_async(
            test_catalog_base, connection_type, connection_params,
            connection_id=effective_conn_id,
        )
        
        if not success:
             return False, "Failed to create Trino catalog for testing", {}
        
        # O nome real do catálogo pode ter sufixo _id quando connection_id é passado
        if effective_conn_id is not None:
            test_catalog_name = manager.generate_catalog_name(test_catalog_base, effective_conn_id)
        else:
            test_catalog_name = manager._sanitize_identifier(test_catalog_base)
             
        # Try to query schemas to verify connectivity (async)
        conn = await manager.get_connection()
        try:
            cur = await conn.cursor()
            safe_catalog = test_catalog_name
            await cur.execute(f"SHOW SCHEMAS FROM \"{safe_catalog}\"")
            schemas = await cur.fetchall()
            
            # Clean up (async)
            await manager.drop_catalog_async(test_catalog_name)
            
            tunnel_info = " (via SSH tunnel)" if tunnel_started else ""
            return True, f"Connection successful{tunnel_info}. Found {len(schemas)} schemas.", {"schemas": [s[0] for s in schemas]}
            
        except Exception as query_error:
            # Try to clean up
            try:
                await manager.drop_catalog_async(test_catalog_name)
            except:
                pass
            return False, f"Connection created but query failed: {str(query_error)}", {}
        finally:
            await conn.close()
            
    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        return False, f"Connection test failed: {str(e)}", {}
    finally:
        # SEMPRE limpar o túnel de teste (usa ID temporário negativo)
        if tunnel_started:
            try:
                tunnel_manager = get_tunnel_manager()
                await tunnel_manager.stop_tunnel(temp_tunnel_id)
                logger.info(f"[ConnectionManager] Test tunnel cleaned up (temp_id={temp_tunnel_id})")
            except Exception as e:
                logger.warning(f"[ConnectionManager] Error cleaning up test tunnel: {e}")