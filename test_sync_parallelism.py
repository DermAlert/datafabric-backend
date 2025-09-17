#!/usr/bin/env python3
"""
Script para testar o paralelismo do sync pool
Dispara múltiplas sincronizações simultaneamente para testar se o pool está funcionando corretamente
"""

import asyncio
import httpx
import time
from datetime import datetime

async def trigger_sync(connection_id: int, session: httpx.AsyncClient):
    """Dispara uma sincronização para uma conexão"""
    try:
        start_time = time.time()
        response = await session.post(
            "http://localhost:8000/api/data-connections/{}/sync".format(connection_id),
            timeout=30.0
        )
        end_time = time.time()
        
        if response.status_code == 202:
            result = response.json()
            print(f"✅ Connection {connection_id}: {result['message']} (triggered in {end_time - start_time:.2f}s)")
            return True, result['dag_run_id']
        else:
            print(f"❌ Connection {connection_id}: HTTP {response.status_code} - {response.text}")
            return False, None
            
    except Exception as e:
        print(f"💥 Connection {connection_id}: Error - {str(e)}")
        return False, None

async def check_dag_status(dag_run_id: str, session: httpx.AsyncClient):
    """Verifica o status de um DAG run via API do Airflow"""
    try:
        # URL da API do Airflow para verificar status do DAG run
        airflow_url = f"http://localhost:8080/api/v1/dags/sync_data_connection/dagRuns/{dag_run_id}"
        
        # Autenticação básica para o Airflow
        auth = ("airflow", "airflow")
        
        response = await session.get(airflow_url, auth=auth, timeout=10.0)
        
        if response.status_code == 200:
            data = response.json()
            state = data.get('state', 'unknown')
            start_date = data.get('start_date', 'unknown')
            end_date = data.get('end_date', 'N/A')
            
            return {
                'dag_run_id': dag_run_id,
                'state': state,
                'start_date': start_date,
                'end_date': end_date
            }
        else:
            return {
                'dag_run_id': dag_run_id,
                'state': 'error',
                'error': f"HTTP {response.status_code}"
            }
            
    except Exception as e:
        return {
            'dag_run_id': dag_run_id,
            'state': 'error',
            'error': str(e)
        }

async def main():
    """Função principal para testar paralelismo"""
    print("🚀 Testando paralelismo do sync_pool")
    print("=" * 50)
    
    # Lista de connection_ids para testar (ajuste conforme suas conexões existentes)
    connection_ids = [1, 2, 3, 4, 5]  # Teste com 5 conexões
    
    async with httpx.AsyncClient() as session:
        print(f"⏰ {datetime.now().strftime('%H:%M:%S')} - Disparando {len(connection_ids)} sincronizações simultaneamente...")
        
        # Dispara todas as sincronizações simultaneamente
        tasks = [trigger_sync(conn_id, session) for conn_id in connection_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Coleta os DAG run IDs dos sucessos
        dag_run_ids = []
        successful_triggers = 0
        
        for i, result in enumerate(results):
            if isinstance(result, tuple) and result[0]:  # Sucesso
                successful_triggers += 1
                if result[1]:  # tem dag_run_id
                    dag_run_ids.append(result[1])
        
        print(f"\n📊 Resultado dos disparos:")
        print(f"  • Sucessos: {successful_triggers}/{len(connection_ids)}")
        print(f"  • DAG runs criados: {len(dag_run_ids)}")
        
        if dag_run_ids:
            print(f"\n⏳ Aguardando 5 segundos para verificar status dos DAGs...")
            await asyncio.sleep(5)
            
            print(f"\n🔍 Verificando status dos DAG runs:")
            print("-" * 80)
            
            # Verifica status de todos os DAG runs
            status_tasks = [check_dag_status(dag_run_id, session) for dag_run_id in dag_run_ids]
            status_results = await asyncio.gather(*status_tasks, return_exceptions=True)
            
            running_count = 0
            queued_count = 0
            completed_count = 0
            failed_count = 0
            
            for status in status_results:
                if isinstance(status, dict):
                    state = status.get('state', 'unknown')
                    dag_run_id = status.get('dag_run_id', 'unknown')
                    
                    if state == 'running':
                        running_count += 1
                        print(f"🏃 {dag_run_id}: RUNNING")
                    elif state == 'queued':
                        queued_count += 1
                        print(f"⏸️  {dag_run_id}: QUEUED")
                    elif state == 'success':
                        completed_count += 1
                        print(f"✅ {dag_run_id}: SUCCESS")
                    elif state == 'failed':
                        failed_count += 1
                        print(f"❌ {dag_run_id}: FAILED")
                    else:
                        print(f"❓ {dag_run_id}: {state}")
            
            print("-" * 80)
            print(f"📈 Resumo do paralelismo:")
            print(f"  • Running: {running_count} (deveria ser até 3)")
            print(f"  • Queued: {queued_count}")
            print(f"  • Completed: {completed_count}")
            print(f"  • Failed: {failed_count}")
            
            if running_count >= 2:
                print(f"✅ PARALELISMO FUNCIONANDO! {running_count} DAGs rodando simultaneamente")
            elif running_count == 1 and queued_count > 0:
                print(f"⚠️  POSSÍVEL PROBLEMA: Apenas {running_count} rodando com {queued_count} na fila")
                print("   Verifique se o sync_pool tem slots suficientes e se max_active_runs=3")
            else:
                print(f"❌ PROBLEMA: Paralelismo não está funcionando corretamente")

if __name__ == "__main__":
    asyncio.run(main())
