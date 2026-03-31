# Relatorio exp1_20260315_v2

## Escopo

- Topologias avaliadas: N=1,2,4,8
- Workloads: bronze_pushdown, bronze_federated, silver_persistent
- Rodadas por cenario: 35
- Warm-ups descartados: 5
- Rodadas medidas por cenario: 30

## Limites fixados de CPU/RAM

- `backend-db-1`: cpu=0.5 mem=1024m
- `convenio_autorizacoes_pg`: cpu=0.5 mem=1024m
- `datafabric-backend-airflow-scheduler-1`: cpu=0.75 mem=1024m
- `datafabric-backend-airflow-triggerer-1`: cpu=0.5 mem=768m
- `datafabric-backend-airflow-webserver-1`: cpu=0.75 mem=1024m
- `datafabric-backend-airflow-worker-1`: cpu=1.0 mem=1536m
- `datafabric-backend-dermalert-backend-1`: cpu=1.5 mem=3g
- `datafabric-backend-minio-1`: cpu=0.5 mem=768m
- `datafabric-backend-postgres-airflow-1`: cpu=0.75 mem=1536m
- `datafabric-backend-postgres-backend-1`: cpu=1.0 mem=2g
- `datafabric-backend-redis-1`: cpu=0.25 mem=256m
- `datafabric-backend-spark-master-1`: cpu=1.0 mem=1536m
- `datafabric-backend-spark-worker-1`: cpu=2.0 mem=4g
- `datafabric-backend-trino-1`: cpu=1.5 mem=3g
- `farmacia_alto_custo_pg`: cpu=0.5 mem=1024m
- `imagem_diagnostica_mysql`: cpu=0.5 mem=1024m
- `laboratorio_privado_pg`: cpu=0.5 mem=1024m
- `pesquisa_clinica_mysql`: cpu=0.5 mem=1024m
- `sus_mysql`: cpu=0.5 mem=1024m
- `telemedicina_mysql`: cpu=0.5 mem=1024m

## Resultados consolidados

| Topologia | Cenario | Sucesso | Falha | Latencia media (s) | P95 (s) | Rows processed | Rows output | Size medio (bytes) | Num files medio | CPU medio (%) | RAM media (bytes) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | bronze_federated | 30 | 0 | 4.5262 | 7.6221 | 500.00 | 500.00 | 51611.97 | 1.00 | 277.51 | 12291493930.31 |
| 1 | bronze_pushdown | 30 | 0 | 5.0544 | 8.0753 | 994.00 | 994.00 | 111771.93 | 1.00 | 315.04 | 11771777856.03 |
| 1 | silver_persistent | 30 | 0 | 7.0944 | 9.5014 | 500.00 | 500.00 | 0.00 | 0.00 | 295.83 | 12276310082.80 |
| 2 | bronze_federated | 30 | 0 | 4.6922 | 7.8506 | 500.00 | 500.00 | 56999.00 | 1.00 | 271.77 | 12379971817.66 |
| 2 | bronze_pushdown | 30 | 0 | 6.4068 | 9.6645 | 994.00 | 994.00 | 117577.97 | 2.00 | 342.62 | 12203119089.04 |
| 2 | silver_persistent | 30 | 0 | 8.9565 | 11.9669 | 500.00 | 500.00 | 0.00 | 0.00 | 317.64 | 12507533494.25 |
| 4 | bronze_federated | 29 | 1 | 9.4457 | 17.8247 | 600.00 | 600.00 | 101133.03 | 1.00 | 315.56 | 12302993180.25 |
| 4 | bronze_pushdown | 30 | 0 | 14.7618 | 23.7717 | 2524.00 | 2524.00 | 239460.90 | 4.00 | 338.50 | 12888711795.85 |
| 4 | silver_persistent | 30 | 0 | 17.4301 | 20.6556 | 1280.00 | 680.00 | 0.00 | 0.00 | 345.57 | 11653737920.09 |
| 8 | bronze_federated | 30 | 0 | 7.2521 | 10.2076 | 630.00 | 630.00 | 159220.80 | 1.00 | 342.75 | 13839011144.03 |
| 8 | bronze_pushdown | 30 | 0 | 32.6495 | 54.4981 | 4523.00 | 4523.00 | 354683.27 | 8.00 | 339.63 | 13654591403.15 |
| 8 | silver_persistent | 30 | 0 | 25.2481 | 28.5264 | 2540.00 | 690.00 | 0.00 | 0.00 | 349.09 | 13877736133.76 |

## Observacoes

- `bronze_federated` com N=1 funciona como baseline de uma unica conexao, nao como federacao real.
- `rows_processed`/`rows_output` no Bronze sao metricas derivadas, conforme mapeamento descrito no README.
- A descoberta automatica de relacionamentos ficou restrita a FK intra-DB; os links cross-database foram criados manualmente por CPF via API.
- O workload Bronze persistente no backend atual escreve em `overwrite` mesmo quando o schema da API menciona outros modos.

## Falhas observadas

- Uma falha registrada nas rodadas medidas: `N=4`, `bronze_federated`, iteracao `27/35`, com `status=failed` e sem `error_message` retornada pela API.
