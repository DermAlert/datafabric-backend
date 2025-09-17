# ⚡ Quick Start - Microserviços Data Fabric

Guia rápido para subir e testar a arquitetura de microserviços.

## 🚀 1. Subir os Serviços

```bash
# Clone o repositório (se necessário)
cd /home/hmsb/datafabric-backend

# Construir e subir todos os serviços
docker-compose up --build -d

# Aguardar todos os serviços estarem prontos (pode levar 2-3 minutos)
```

## ✅ 2. Verificar Status dos Serviços

```bash
# Verificar se todos os containers estão rodando
docker-compose ps

# Testar health checks
curl http://localhost:8004/                    # API Principal
curl http://localhost:8007/health              # Sync Service  
curl http://localhost:8006/health              # Dataset Service
curl http://localhost:8080/health              # Airflow
```

**Esperado:** Todos devem retornar status 200.

## ⚙️ 3. Configurar Airflow

```bash
# Executar script de configuração dos POOLs
python setup_airflow_pools.py
```

**Output esperado:**
```
✅ Airflow está disponível!
📋 POOLs configurados: ['sync_pool', 'dataset_pool']  
🔌 Conexões configuradas: ['sync_service_conn', 'dataset_service_conn']
✅ Configuração do Airflow concluída!
```

## 🔧 4. Verificar Airflow UI

1. Acesse: http://localhost:8080
2. Login: `airflow` / `airflow`
3. Verifique se os DAGs estão disponíveis:
   - `sync_data_connection`
   - `create_unified_dataset`

## 🧪 5. Testar Sync de Conexão

```bash
# Executar sync da conexão ID 1
curl -X POST http://localhost:8004/api/data-connections/1/sync
```

**Resposta esperada:**
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

## 📊 6. Testar Criação de Dataset

```bash
# Criar dataset unificado
curl -X POST http://localhost:8004/api/datasets/unified \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_unified_dataset",
    "selection_mode": "tables",
    "selected_tables": [1, 2],
    "auto_include_mapped_columns": true,
    "apply_value_mappings": true,
    "storage_type": "copy_to_minio"
  }'
```

**Resposta esperada:**
```json
{
  "message": "Dataset creation for 'test_unified_dataset' has been queued",
  "dag_run_id": "dataset_test_unified_dataset_1734567890",
  "dag_id": "create_unified_dataset",
  "status": "queued",
  "estimated_processing_time": "2-10 minutes depending on data volume"
}
```

## 👀 7. Monitorar Execução

### Via Airflow UI:
1. Acesse: http://localhost:8080
2. Clique no DAG executado
3. Veja o progresso das tasks em tempo real

### Via Logs:
```bash
# Logs do sync service
docker-compose logs -f sync-service

# Logs do dataset service
docker-compose logs -f dataset-service
```

## 🎯 8. Verificar Pools

No Airflow UI:
1. Admin → Pools
2. Verifique:
   - `sync_pool`: 3 slots
   - `dataset_pool`: 2 slots

## ❌ Solução de Problemas Rápidos

### Serviço não responde:
```bash
# Reiniciar serviço específico
docker-compose restart sync-service
docker-compose restart dataset-service
```

### DAG não aparece:
```bash
# Reiniciar Airflow
docker-compose restart airflow-scheduler
docker-compose restart airflow-webserver
```

### Erro de conexão com banco:
```bash
# Verificar se PostgreSQL está rodando
docker-compose ps postgres-backend

# Reiniciar se necessário
docker-compose restart postgres-backend
```

## 📈 Status de Sucesso

Quando tudo estiver funcionando, você verá:

1. **Containers rodando:** `docker-compose ps` mostra todos UP
2. **Health checks OK:** Todos os endpoints respondem 200
3. **DAGs visíveis:** Aparecem no Airflow UI
4. **Jobs executando:** Tasks progridem no Airflow
5. **Logs limpos:** Sem erros nos logs dos serviços

## 🔗 URLs Importantes

- **API Principal:** http://localhost:8004
- **Sync Service:** http://localhost:8005
- **Dataset Service:** http://localhost:8006
- **Airflow UI:** http://localhost:8080
- **MinIO UI:** http://localhost:9001

## 📞 Próximos Passos

Após o setup funcionar:
1. Consulte o `README_MICROSERVICES.md` para detalhes completos
2. Teste com dados reais
3. Configure monitoramento adicional
4. Ajuste pools conforme necessário

---

**🎉 Parabéns! Sua arquitetura de microserviços está funcionando!**
