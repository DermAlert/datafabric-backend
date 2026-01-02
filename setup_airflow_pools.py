#!/usr/bin/env python3
"""
Script para configurar os POOLs do Airflow para controle de concorrência dos microserviços.
Este script deve ser executado após o Airflow estar rodando.
"""

import requests
import json
import base64
import sys
import time
from requests.auth import HTTPBasicAuth

# Configurações do Airflow
AIRFLOW_URL = "http://localhost:8080"
AIRFLOW_USERNAME = "airflow"  # Default username
AIRFLOW_PASSWORD = "airflow"  # Default password

# Configuração dos POOLs
POOLS_CONFIG = [
    {
        "name": "sync_pool",
        "slots": 3,  # Máximo 3 jobs de sync simultâneos
        "description": "Pool para controlar concorrência de jobs de sincronização de metadados"
    },
    {
        "name": "dataset_pool", 
        "slots": 2,  # Máximo 2 jobs de dataset simultâneos (mais pesados)
        "description": "Pool para controlar concorrência de jobs de criação de datasets"
    }
]

# Configuração das conexões HTTP para os microserviços
CONNECTIONS_CONFIG = [
    {
        "connection_id": "sync_service_conn",
        "conn_type": "http",
        "host": "sync-service",  # Nome do serviço no docker-compose
        "port": 8007,
        "description": "Conexão HTTP para o microserviço de sync"
    },
    {
        "connection_id": "dataset_service_conn",
        "conn_type": "http", 
        "host": "dataset-service",  # Nome do serviço no docker-compose
        "port": 8006,
        "description": "Conexão HTTP para o microserviço de dataset"
    }
]

def wait_for_airflow():
    """Aguarda o Airflow estar disponível"""
    print("Aguardando Airflow estar disponível...")
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{AIRFLOW_URL}/health", timeout=5)
            if response.status_code == 200:
                print("✅ Airflow está disponível!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        print(f"Tentativa {attempt + 1}/{max_attempts} - Airflow ainda não disponível")
        time.sleep(10)
    
    print("❌ Timeout aguardando Airflow")
    return False

def setup_pools():
    """Configura os POOLs do Airflow"""
    print("\n🏊 Configurando POOLs do Airflow...")
    
    auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    
    for pool_config in POOLS_CONFIG:
        pool_name = pool_config["name"]
        
        # Verificar se o pool já existe
        try:
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/pools/{pool_name}",
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                print(f"  📝 Atualizando pool existente: {pool_name}")
                # Pool existe, atualizar
                response = requests.patch(
                    f"{AIRFLOW_URL}/api/v1/pools/{pool_name}",
                    auth=auth,
                    headers={"Content-Type": "application/json"},
                    json={
                        "slots": pool_config["slots"],
                        "description": pool_config["description"]
                    }
                )
            else:
                print(f"  ➕ Criando novo pool: {pool_name}")
                # Pool não existe, criar
                response = requests.post(
                    f"{AIRFLOW_URL}/api/v1/pools",
                    auth=auth,
                    headers={"Content-Type": "application/json"},
                    json=pool_config
                )
            
            if response.status_code in [200, 201]:
                print(f"    ✅ Pool {pool_name} configurado com {pool_config['slots']} slots")
            else:
                print(f"    ❌ Erro configurando pool {pool_name}: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Erro de conexão configurando pool {pool_name}: {e}")

def setup_connections():
    """Configura as conexões HTTP para os microserviços"""
    print("\n🔌 Configurando conexões HTTP do Airflow...")
    
    auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    
    for conn_config in CONNECTIONS_CONFIG:
        conn_id = conn_config["connection_id"]
        
        # Preparar dados da conexão
        connection_data = {
            "connection_id": conn_id,
            "conn_type": conn_config["conn_type"],
            "host": conn_config["host"],
            "port": conn_config["port"],
            "description": conn_config["description"]
        }
        
        # Verificar se a conexão já existe
        try:
            response = requests.get(
                f"{AIRFLOW_URL}/api/v1/connections/{conn_id}",
                auth=auth,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                print(f"  📝 Atualizando conexão existente: {conn_id}")
                # Conexão existe, atualizar
                response = requests.patch(
                    f"{AIRFLOW_URL}/api/v1/connections/{conn_id}",
                    auth=auth,
                    headers={"Content-Type": "application/json"},
                    json=connection_data
                )
            else:
                print(f"  ➕ Criando nova conexão: {conn_id}")
                # Conexão não existe, criar
                response = requests.post(
                    f"{AIRFLOW_URL}/api/v1/connections",
                    auth=auth,
                    headers={"Content-Type": "application/json"},
                    json=connection_data
                )
            
            if response.status_code in [200, 201]:
                print(f"    ✅ Conexão {conn_id} configurada para {conn_config['host']}:{conn_config['port']}")
            else:
                print(f"    ❌ Erro configurando conexão {conn_id}: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Erro de conexão configurando {conn_id}: {e}")

def verify_setup():
    """Verifica se a configuração foi aplicada corretamente"""
    print("\n🔍 Verificando configuração...")
    
    auth = HTTPBasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    
    # Verificar pools
    try:
        response = requests.get(f"{AIRFLOW_URL}/api/v1/pools", auth=auth)
        if response.status_code == 200:
            pools = response.json().get("pools", [])
            configured_pools = []
            for p in pools:
                pool_name = p.get("pool") or p.get("name") or p.get("pool_name")
                if pool_name in ["sync_pool", "dataset_pool"]:
                    configured_pools.append(pool_name)
            print(f"  📋 POOLs configurados: {configured_pools}")
        else:
            print(f"  ❌ Erro verificando pools: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Erro verificando pools: {e}")
    
    # Verificar conexões
    try:
        response = requests.get(f"{AIRFLOW_URL}/api/v1/connections", auth=auth)
        if response.status_code == 200:
            connections = response.json().get("connections", [])
            configured_conns = []
            for c in connections:
                conn_id = c.get("connection_id") or c.get("conn_id") or c.get("id")
                if conn_id in ["sync_service_conn", "dataset_service_conn"]:
                    configured_conns.append(conn_id)
            print(f"  🔌 Conexões configuradas: {configured_conns}")
        else:
            print(f"  ❌ Erro verificando conexões: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Erro verificando conexões: {e}")

def main():
    """Função principal"""
    print("🚀 Configurando Airflow para microserviços...")
    
    # Aguardar Airflow estar disponível
    if not wait_for_airflow():
        sys.exit(1)
    
    # Configurar POOLs
    setup_pools()
    
    # Configurar conexões
    setup_connections()
    
    # Verificar configuração
    verify_setup()
    
    print("\n✅ Configuração do Airflow concluída!")
    print("\nPróximos passos:")
    print("1. Acesse http://localhost:8080 para ver o Airflow UI")
    print("2. Verifique se os DAGs 'sync_data_connection' e 'create_unified_dataset' estão disponíveis")
    print("3. Teste os microserviços:")
    print("   - Sync Service: http://localhost:8007/health")
    print("   - Dataset Service: http://localhost:8006/health")

if __name__ == "__main__":
    main()
