# 🚀 Microserviços Data Fabric

Este documento descreve a arquitetura de microserviços implementada para separar as funcionalidades de **Sync de Conexões** e **Criação de Datasets Unificados** do monólito principal.

## 📋 Visão Geral

### Arquitetura Anterior (Monólito)
```
┌─────────────────────────────────────┐
│          FastAPI Backend           │
│  ┌─────────────┐ ┌───────────────┐  │
│  │ Sync Route  │ │ Dataset Route │  │
│  │     +       │ │      +        │  │
│  │ Background  │ │  Heavy Proc.  │  │
│  │   Tasks     │ │   (Gargalo)   │  │
│  └─────────────┘ └───────────────┘  │
└─────────────────────────────────────┘
```

### Nova Arquitetura (Microserviços + Airflow)
```
┌─────────────────┐    ┌─────────────────┐
│   API Gateway   │    │     Airflow     │
│   (Main API)    │────│   Orchestrator  │
└─────────────────┘    └─────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │                   │
            ┌───────▼──────┐    ┌───────▼──────┐
            │ Sync Service │    │Dataset Service│
            │  (Port 8005) │    │  (Port 8006) │
            │              │    │              │
            │ sync_pool    │    │ dataset_pool │
            │ (3 workers)  │    │ (2 workers)  │
            └──────────────┘    └──────────────┘
```

## 🏗️ Componentes

### 1. **Sync Service** (Port 8007)
**Responsabilidade:** Processar sincronização de metadados de conexões de dados.

**Endpoints:**
- `GET /health` - Health check
- `POST /process-sync` - Processar job de sync (chamado pelo Airflow)
- `GET /status/{job_id}` - Status do job

**Pool Airflow:** `sync_pool` (3 workers simultâneos)

### 2. **Dataset Service** (Port 8006)
**Responsabilidade:** Criar datasets unificados com processamento pesado.

**Endpoints:**
- `GET /health` - Health check
- `POST /process-dataset-creation` - Processar criação de dataset (chamado pelo Airflow)
- `POST /preview-dataset-creation` - Preview do dataset
- `GET /status/{job_id}` - Status do job

**Pool Airflow:** `dataset_pool` (2 workers simultâneos)

### 3. **API Principal** (Port 8004)
**Responsabilidade:** Receber requests e disparar DAGs do Airflow.

**Mudanças nos Endpoints:**
- `POST /data-connections/{id}/sync` → Dispara DAG `sync_data_connection`
- `POST /datasets/unified` → Dispara DAG `create_unified_dataset`

### 4. **Airflow DAGs**
**Orquestração:** Gerencia filas e execução dos microserviços.

**DAGs:**
- `sync_data_connection` - Processa sync de conexões
- `create_unified_dataset` - Processa criação de datasets

## 🚀 Como Usar

### 1. **Subir os Serviços**

```bash
# Construir e subir todos os serviços
docker-compose up --build

# Aguardar todos os serviços estarem prontos
# Airflow: http://localhost:8080
# API Principal: http://localhost:8004
# Sync Service: http://localhost:8005
# Dataset Service: http://localhost:8006
```

### 2. **Configurar Airflow**

```bash
# Executar script de configuração dos POOLs
python setup_airflow_pools.py
```

Este script configura:
- **Pools:** `sync_pool` (3 slots) e `dataset_pool` (2 slots)
- **Conexões HTTP:** Para comunicação com os microserviços

### 3. **Testar Health Checks**

```bash
# API Principal
curl http://localhost:8004/

# Sync Service
curl http://localhost:8005/health

# Dataset Service  
curl http://localhost:8006/health

# Airflow
curl http://localhost:8080/health
```

### 4. **Executar Sync de Conexão**

```bash
curl -X POST http://localhost:8004/api/data-connections/1/sync
```

**Resposta:**
```json
{
  "message": "Metadata synchronization for connection 'MyConnection' has been queued",
  "dag_run_id": "sync_1_1734567890",
  "dag_id": "sync_data_connection",
  "airflow_url": "http://localhost:8080/dags/sync_data_connection/grid?dag_run_id=sync_1_1734567890",
  "connection_id": 1,
  "status": "queued"
}
```

### 5. **Executar Criação de Dataset**

```bash
curl -X POST http://localhost:8004/api/datasets/unified \
  -H "Content-Type: application/json" \
  -d '{
    "name": "unified_dataset_test",
    "selection_mode": "tables",
    "selected_tables": [1, 2, 3],
    "auto_include_mapped_columns": true,
    "apply_value_mappings": true,
    "storage_type": "copy_to_minio"
  }'
```

**Resposta:**
```json
{
  "message": "Dataset creation for 'unified_dataset_test' has been queued",
  "dag_run_id": "dataset_unified_dataset_test_1734567890",
  "dag_id": "create_unified_dataset",
  "airflow_url": "http://localhost:8080/dags/create_unified_dataset/grid?dag_run_id=dataset_unified_dataset_test_1734567890",
  "dataset_name": "unified_dataset_test",
  "selection_mode": "tables",
  "status": "queued",
  "estimated_processing_time": "2-10 minutes depending on data volume"
}
```

## 📊 Monitoramento

### 1. **Airflow UI**
- **URL:** http://localhost:8080
- **Login:** airflow / airflow
- **Ver:** DAGs, execuções, logs, POOLs

### 2. **Logs dos Microserviços**

```bash
# Sync Service logs
docker-compose logs -f sync-service

# Dataset Service logs
docker-compose logs -f dataset-service
```

### 3. **Status via API**

```bash
# Status de job de sync
curl http://localhost:8005/status/sync_1_1734567890

# Status de job de dataset
curl http://localhost:8006/status/dataset_test_1734567890
```

## ⚙️ Configuração de Concorrência

### POOLs do Airflow

**Sync Pool:**
- **Nome:** `sync_pool`
- **Slots:** 3
- **Uso:** Jobs de sincronização de metadados
- **Tempo típico:** 1-5 minutos

**Dataset Pool:**
- **Nome:** `dataset_pool`
- **Slots:** 2  
- **Uso:** Criação de datasets (mais pesado)
- **Tempo típico:** 2-10 minutos

### Ajustar Concorrência

Para alterar o número de workers simultâneos:

1. **Via Airflow UI:**
   - Admin → Pools
   - Editar `sync_pool` ou `dataset_pool`
   - Alterar número de slots

2. **Via Script:**
   - Editar `setup_airflow_pools.py`
   - Alterar valores em `POOLS_CONFIG`
   - Executar novamente

## 🔧 Desenvolvimento

### Estrutura dos Microserviços

```
sync-service/
├── main.py          # FastAPI app
├── Dockerfile       # Container config
└── requirements     # Dependencies (shared from main project)

dataset-service/
├── main.py          # FastAPI app  
├── Dockerfile       # Container config
└── requirements     # Dependencies (shared from main project)

dags/
├── sync_connection_dag.py     # DAG para sync
└── create_dataset_dag.py      # DAG para datasets
```

### Adicionar Novo Microserviço

1. **Criar diretório** `new-service/`
2. **Implementar** `main.py` com FastAPI
3. **Criar** `Dockerfile`
4. **Adicionar** ao `docker-compose.yml`
5. **Criar** DAG no Airflow
6. **Configurar** POOL se necessário

### Debug

**Logs detalhados:**
```bash
# Todos os serviços
docker-compose logs -f

# Serviço específico
docker-compose logs -f sync-service
```

**Acessar container:**
```bash
# Sync service
docker-compose exec sync-service bash

# Dataset service
docker-compose exec dataset-service bash
```

## 🚨 Solução de Problemas

### Problema: DAG não encontrado
**Solução:** Verificar se os DAGs estão na pasta `/dags` e se o Airflow reiniciou.

### Problema: Pool não existe
**Solução:** Executar `python setup_airflow_pools.py`

### Problema: Microserviço não responde
**Solução:** 
1. Verificar health check: `curl http://localhost:8005/health`
2. Ver logs: `docker-compose logs sync-service`
3. Reiniciar: `docker-compose restart sync-service`

### Problema: Job fica em pending
**Solução:**
1. Verificar Pool slots disponíveis no Airflow UI
2. Verificar se workers estão rodando
3. Ver logs do DAG para erros

### Problema: Erro de conexão com banco
**Solução:**
1. Verificar se PostgreSQL está rodando
2. Verificar variáveis de ambiente (`.env`)
3. Aguardar serviços dependencies estarem prontos

## 📈 Benefícios da Arquitetura

### ✅ **Vantagens**

1. **Isolamento:** Falhas em um serviço não afetam o outro
2. **Escalabilidade:** Cada serviço pode ser escalado independentemente
3. **Controle de Concorrência:** POOLs do Airflow evitam sobrecarga
4. **Monitoramento:** Visibilidade granular via Airflow UI
5. **Manutenibilidade:** Código separado por responsabilidade
6. **Recuperação:** Jobs podem ser reexecutados individualmente

### 📊 **Métricas de Performance**

**Antes (Monólito):**
- Sync + Dataset simultâneos = Gargalo
- Sem controle de concorrência
- Falha em um afeta tudo

**Depois (Microserviços):**
- Sync: 3 jobs simultâneos
- Dataset: 2 jobs simultâneos  
- Isolamento de falhas
- Fila organizada pelo Airflow

## 🔮 Próximos Passos

1. **Métricas:** Implementar Prometheus + Grafana
2. **Alertas:** Notificações de falha via Slack/Email
3. **Auto-scaling:** Baseado em carga da fila
4. **Circuit Breaker:** Para falhas de dependências
5. **Retry Policy:** Configuração avançada de reexecução
6. **API Gateway:** Rate limiting e autenticação centralizada

---

## 📞 Suporte

Para dúvidas ou problemas:
1. Verificar logs dos serviços
2. Consultar Airflow UI
3. Verificar health checks
4. Revisar esta documentação
