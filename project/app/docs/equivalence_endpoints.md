# Endpoints de Equivalência de Metadados

Este documento descreve os endpoints criados para gerenciar a equivalência de colunas e valores dos metadados no sistema.

## Estrutura dos Endpoints

### Base URL
Todos os endpoints de equivalência estão sob o prefixo `/equivalence`

## 1. Domínios Semânticos

### Listar domínios semânticos
```
GET /equivalence/semantic-domains
```

### Criar domínio semântico
```
POST /equivalence/semantic-domains
```
**Body:**
```json
{
  "name": "pessoa",
  "description": "Informações relacionadas a pessoas",
  "parent_domain_id": null,
  "domain_rules": {
    "validation": "required",
    "format": "string"
  }
}
```

### Buscar domínio específico
```
GET /equivalence/semantic-domains/{domain_id}
```

### Atualizar domínio
```
PUT /equivalence/semantic-domains/{domain_id}
```

### Remover domínio
```
DELETE /equivalence/semantic-domains/{domain_id}
```

## 2. Dicionário de Dados

### Listar termos do dicionário
```
GET /equivalence/data-dictionary?semantic_domain_id=1&data_type=string
```

### Criar termo no dicionário
```
POST /equivalence/data-dictionary
```
**Body:**
```json
{
  "name": "nome_pessoa",
  "display_name": "Nome da Pessoa",
  "description": "Nome completo de uma pessoa",
  "semantic_domain_id": 1,
  "data_type": "string",
  "validation_rules": {
    "min_length": 2,
    "max_length": 100
  },
  "example_values": {
    "examples": ["João Silva", "Maria Santos"]
  },
  "synonyms": ["nome_completo", "full_name"]
}
```

### Buscar, atualizar e remover termos
```
GET /equivalence/data-dictionary/{term_id}
PUT /equivalence/data-dictionary/{term_id}
DELETE /equivalence/data-dictionary/{term_id}
```

## 3. Grupos de Colunas

### Listar grupos de colunas
```
GET /equivalence/column-groups?semantic_domain_id=1
```

### Criar grupo de colunas
```
POST /equivalence/column-groups
```
**Body:**
```json
{
  "name": "Grupo Nome Pessoa",
  "description": "Agrupa todas as colunas que representam nomes de pessoas",
  "semantic_domain_id": 1,
  "data_dictionary_term_id": 1,
  "properties": {
    "standardization": "upper_case",
    "required": true
  }
}
```

### Buscar grupo com mapeamentos
```
GET /equivalence/column-groups/{group_id}
```
Retorna o grupo junto com todos os mapeamentos de colunas e valores.

### Atualizar e remover grupos
```
PUT /equivalence/column-groups/{group_id}
DELETE /equivalence/column-groups/{group_id}
```

## 4. Mapeamentos de Colunas

### Listar mapeamentos de um grupo
```
GET /equivalence/column-groups/{group_id}/column-mappings
```

### Criar mapeamento de coluna
```
POST /equivalence/column-mappings
```
**Body:**
```json
{
  "group_id": 1,
  "column_id": 15,
  "transformation_rule": "UPPER(TRIM({column_name}))",
  "confidence_score": 0.95,
  "notes": "Mapeamento validado manualmente"
}
```

### Criar múltiplos mapeamentos
```
POST /equivalence/column-mappings/bulk
```
**Body:**
```json
{
  "group_id": 1,
  "mappings": [
    {
      "group_id": 1,
      "column_id": 15,
      "transformation_rule": "UPPER(TRIM({column_name}))",
      "confidence_score": 0.95,
      "notes": "Nome da tabela usuarios"
    },
    {
      "group_id": 1,
      "column_id": 23,
      "transformation_rule": "UPPER(TRIM({column_name}))",
      "confidence_score": 0.90,
      "notes": "Nome da tabela clientes"
    }
  ]
}
```

### Atualizar e remover mapeamentos
```
PUT /equivalence/column-mappings/{mapping_id}
DELETE /equivalence/column-mappings/{mapping_id}
```

## 5. Mapeamentos de Valores

### Listar mapeamentos de valores
```
GET /equivalence/column-groups/{group_id}/value-mappings?source_column_id=15
```

### Criar mapeamento de valor
```
POST /equivalence/value-mappings
```
**Body:**
```json
{
  "group_id": 1,
  "source_column_id": 15,
  "source_value": "M",
  "standard_value": "Masculino",
  "description": "Padronização do gênero masculino"
}
```

### Criar múltiplos mapeamentos de valores
```
POST /equivalence/value-mappings/bulk
```
**Body:**
```json
{
  "group_id": 1,
  "mappings": [
    {
      "group_id": 1,
      "source_column_id": 15,
      "source_value": "M",
      "standard_value": "Masculino",
      "description": "Gênero masculino"
    },
    {
      "group_id": 1,
      "source_column_id": 15,
      "source_value": "F",
      "standard_value": "Feminino",
      "description": "Gênero feminino"
    }
  ]
}
```

## 6. Utilitários e Pesquisa

### Buscar colunas disponíveis para mapeamento
```
GET /equivalence/available-columns?connection_id=1&exclude_mapped=true
```

### Buscar colunas não mapeadas
```
POST /equivalence/search/columns
```
**Body:**
```json
{
  "query": "nome",
  "semantic_domain_id": 1,
  "data_type": "string",
  "confidence_threshold": 0.7
}
```

### Sugestões de colunas similares
```
GET /equivalence/suggestions/similar-columns/{column_id}?limit=10
```

### Estatísticas de cobertura
```
GET /equivalence/statistics/mapping-coverage?connection_id=1
```

### Exportar mapeamentos
```
GET /equivalence/export/mappings?group_id=1&format=json
GET /equivalence/export/mappings?format=csv
```

### Importar mapeamentos
```
POST /equivalence/import/mappings?group_id=1
```
**Body:**
```json
[
  {
    "column_id": 15,
    "transformation_rule": "UPPER(TRIM({column_name}))",
    "confidence_score": 0.95,
    "notes": "Importação em lote"
  }
]
```

## Fluxo de Trabalho Típico

1. **Criar domínios semânticos** para categorizar os tipos de dados
2. **Criar termos no dicionário** para definir conceitos padronizados
3. **Criar grupos de colunas** para agrupar colunas relacionadas
4. **Buscar colunas disponíveis** para ver quais colunas precisam ser mapeadas
5. **Criar mapeamentos de colunas** para associar colunas aos grupos
6. **Criar mapeamentos de valores** para padronizar valores específicos
7. **Usar as estatísticas** para monitorar a cobertura dos mapeamentos

## Códigos de Status HTTP

- `200 OK`: Operação bem-sucedida
- `201 Created`: Recurso criado com sucesso
- `400 Bad Request`: Dados inválidos ou regra de negócio violada
- `404 Not Found`: Recurso não encontrado
- `500 Internal Server Error`: Erro interno do servidor

## Exemplos de Uso Prático

### Cenário: Mapeamento de colunas de nome de pessoas

1. Criar domínio semântico:
```bash
curl -X POST "/equivalence/semantic-domains" \
  -H "Content-Type: application/json" \
  -d '{"name": "pessoa", "description": "Dados pessoais"}'
```

2. Criar termo no dicionário:
```bash
curl -X POST "/equivalence/data-dictionary" \
  -H "Content-Type: application/json" \
  -d '{"name": "nome_pessoa", "display_name": "Nome da Pessoa", "description": "Nome completo", "semantic_domain_id": 1, "data_type": "string"}'
```

3. Criar grupo de colunas:
```bash
curl -X POST "/equivalence/column-groups" \
  -H "Content-Type: application/json" \
  -d '{"name": "Nomes de Pessoas", "description": "Todas as colunas de nomes", "semantic_domain_id": 1, "data_dictionary_term_id": 1}'
```

4. Mapear colunas para o grupo:
```bash
curl -X POST "/equivalence/column-mappings" \
  -H "Content-Type: application/json" \
  -d '{"group_id": 1, "column_id": 15, "transformation_rule": "UPPER(TRIM({column_name}))", "confidence_score": 0.95}'
```
