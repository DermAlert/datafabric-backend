# Fix para Paralelismo do Sync Pool

## Problema Identificado
O DAG `sync_data_connection` estava configurado com `max_active_runs=1`, impedindo múltiplas execuções simultâneas mesmo com o `sync_pool` tendo 3 slots disponíveis.

## Alterações Realizadas

### 1. DAG Configuration (`dags/sync_connection_dag.py`)
```python
# ANTES:
max_active_runs=1,

# DEPOIS:
max_active_runs=3,  # Permite até 3 DAG runs simultâneos (igual ao sync_pool)
max_active_tasks=10,  # Permite múltiplas tarefas ativas por DAG run
```

### 2. Pool Usage Optimization
- **validate_params_task**: Removido do pool (validação é rápida)
- **prepare_request_task**: Removido do pool (preparação é rápida) 
- **call_sync_microservice**: Mantido no pool (tarefa pesada que acessa microserviço)
- **log_completion**: Não usa pool (logging é rápido)

### 3. Microserviço Implementado (`sync-service/main.py`)
- ✅ Endpoint `/process-sync` implementado
- ✅ Logging detalhado com emojis
- ✅ Tratamento de erros robusto
- ✅ Tracking de jobs ativos
- ✅ Health check endpoint
- ✅ Porta corrigida para 8007

## Configuração Atual do Pool

```python
# setup_airflow_pools.py
{
    "name": "sync_pool",
    "slots": 3,  # Máximo 3 jobs de sync simultâneos
    "description": "Pool para controlar concorrência de jobs de sincronização de metadados"
}
```

## Como Testar o Paralelismo

### 1. Teste Manual via API
```bash
# Disparar múltiplas sincronizações simultaneamente
curl -X POST "http://localhost:8000/api/data-connections/1/sync" &
curl -X POST "http://localhost:8000/api/data-connections/2/sync" &
curl -X POST "http://localhost:8000/api/data-connections/3/sync" &
curl -X POST "http://localhost:8000/api/data-connections/4/sync" &
wait
```

### 2. Teste Automatizado
```bash
# Usar o script de teste criado
python test_sync_parallelism.py
```

### 3. Verificar no Airflow UI
1. Acesse: http://localhost:8080
2. Vá para: Admin > Pools
3. Verifique se `sync_pool` tem 3 slots
4. Vá para: DAGs > sync_data_connection
5. Dispare múltiplas execuções e observe se rodam simultaneamente

## Resultado Esperado

Após as correções, você deve ver:

1. **No Airflow UI**: Até 3 DAG runs do `sync_data_connection` rodando simultaneamente
2. **Nos Logs**: Múltiplos jobs de sync processando em paralelo
3. **No Pool**: 3 slots sendo utilizados quando há demanda
4. **No Microserviço**: Múltiplas requisições sendo processadas

## Verificações Adicionais

Se ainda houver problemas, verifique:

1. **Configuração do Airflow**: 
   - `parallelism` (configuração global)
   - `dag_concurrency` 
   - `max_active_runs_per_dag`

2. **Recursos do Sistema**:
   - CPU e memória disponíveis
   - Conexões de banco de dados

3. **Logs do Airflow**:
   ```bash
   docker-compose logs -f airflow-webserver
   docker-compose logs -f airflow-scheduler
   ```

## Troubleshooting

### Se apenas 1 job roda por vez:
- Verifique se o DAG foi recarregado no Airflow
- Confirme que `max_active_runs=3` no código
- Verifique se o pool tem 3 slots disponíveis

### Se jobs ficam em "queued":
- Verifique recursos do sistema
- Confirme que o microserviço está rodando
- Verifique logs do scheduler do Airflow

### Se há erros de conexão:
- Confirme que `sync_service_conn` está configurado
- Verifique se o microserviço está acessível em `sync-service:8007`
- Teste a conectividade entre containers
