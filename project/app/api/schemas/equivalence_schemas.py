from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from decimal import Decimal
from datetime import datetime
from app.api.schemas.search import BaseSearchRequest

# Semantic Domain Schemas
class SemanticDomainBase(BaseModel):
    name: str = Field(..., max_length=100, description="Nome do domínio semântico")
    description: Optional[str] = Field(None, description="Descrição do domínio semântico")
    parent_domain_id: Optional[int] = Field(None, description="ID do domínio pai")
    domain_rules: Dict[str, Any] = Field(default_factory=dict, description="Regras do domínio")

class SemanticDomainCreate(SemanticDomainBase):
    pass

class SemanticDomainUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    parent_domain_id: Optional[int] = None
    domain_rules: Optional[Dict[str, Any]] = None

class SemanticDomainResponse(SemanticDomainBase):
    id: int
    
    model_config = {
        "from_attributes": True
    }

# Data Dictionary Schemas
class DataDictionaryBase(BaseModel):
    name: str = Field(..., max_length=100, description="Nome do termo")
    display_name: str = Field(..., max_length=255, description="Nome de exibição")
    description: str = Field(..., description="Descrição do termo")
    semantic_domain_id: Optional[int] = Field(None, description="ID do domínio semântico")
    data_type: str = Field(..., max_length=50, description="Tipo de dados")
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Regras de validação")
    example_values: Dict[str, Any] = Field(default_factory=dict, description="Valores de exemplo")
    synonyms: Optional[List[str]] = Field(None, description="Sinônimos")

class DataDictionaryCreate(DataDictionaryBase):
    pass

class DataDictionaryUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    display_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    semantic_domain_id: Optional[int] = None
    data_type: Optional[str] = Field(None, max_length=50)
    validation_rules: Optional[Dict[str, Any]] = None
    example_values: Optional[Dict[str, Any]] = None
    synonyms: Optional[List[str]] = None

class DataDictionaryResponse(DataDictionaryBase):
    id: int
    
    model_config = {
        "from_attributes": True
    }

# Column Group Schemas
class ColumnGroupBase(BaseModel):
    name: str = Field(..., max_length=255, description="Nome do grupo de colunas")
    description: Optional[str] = Field(None, description="Descrição do grupo")
    semantic_domain_id: Optional[int] = Field(None, description="ID do domínio semântico")
    data_dictionary_term_id: Optional[int] = Field(None, description="ID do termo do dicionário de dados")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Propriedades do grupo")

class ColumnGroupCreate(ColumnGroupBase):
    pass

class ColumnGroupUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    semantic_domain_id: Optional[int] = None
    data_dictionary_term_id: Optional[int] = None
    properties: Optional[Dict[str, Any]] = None

class ColumnGroupResponse(ColumnGroupBase):
    id: int
    
    model_config = {
        "from_attributes": True
    }

# Column Mapping Schemas
class ColumnMappingBase(BaseModel):
    group_id: int = Field(..., description="ID do grupo de colunas")
    column_id: int = Field(..., description="ID da coluna externa")
    transformation_rule: Optional[str] = Field(None, description="Regra de transformação")
    confidence_score: Decimal = Field(default=Decimal('1.0'), description="Pontuação de confiança")
    notes: Optional[str] = Field(None, description="Notas sobre o mapeamento")

class ColumnMappingCreate(ColumnMappingBase):
    pass

class ColumnMappingUpdate(BaseModel):
    transformation_rule: Optional[str] = None
    confidence_score: Optional[Decimal] = None
    notes: Optional[str] = None

class ColumnMappingResponse(ColumnMappingBase):
    id: int
    
    model_config = {
        "from_attributes": True
    }

# Value Mapping Schemas
class ValueMappingBase(BaseModel):
    group_id: int = Field(..., description="ID do grupo de colunas")
    source_column_id: int = Field(..., description="ID da coluna de origem")
    source_value: str = Field(..., description="Valor de origem")
    standard_value: str = Field(..., description="Valor padronizado")
    description: Optional[str] = Field(None, description="Descrição do mapeamento")

class ValueMappingCreate(ValueMappingBase):
    pass

class ValueMappingUpdate(BaseModel):
    source_value: Optional[str] = None
    standard_value: Optional[str] = None
    description: Optional[str] = None

class ValueMappingResponse(ValueMappingBase):
    id: int
    
    model_config = {
        "from_attributes": True
    }

# Complex Response Schemas
class ColumnGroupWithMappingsResponse(ColumnGroupResponse):
    column_mappings: List[ColumnMappingResponse] = []
    value_mappings: List[ValueMappingResponse] = []

class ColumnMappingWithDetailsResponse(ColumnMappingResponse):
    column_name: Optional[str] = None
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    data_type: Optional[str] = None

# Bulk operations
class BulkColumnMappingCreate(BaseModel):
    group_id: int
    mappings: List[ColumnMappingCreate]

class BulkValueMappingCreate(BaseModel):
    group_id: int
    mappings: List[ValueMappingCreate]

# Search and filter schemas
class SearchSemanticDomain(BaseSearchRequest):
    name: Optional[str] = Field(None, description="Filtrar por nome")
    parent_domain_id: Optional[int] = Field(None, description="Filtrar por domínio pai")

class SearchDataDictionary(BaseSearchRequest):
    name: Optional[str] = Field(None, description="Filtrar por nome")
    semantic_domain_id: Optional[int] = Field(None, description="Filtrar por domínio semântico")
    data_type: Optional[str] = Field(None, description="Filtrar por tipo de dados")

class SearchColumnGroup(BaseSearchRequest):
    name: Optional[str] = Field(None, description="Filtrar por nome")
    semantic_domain_id: Optional[int] = Field(None, description="Filtrar por domínio semântico")
    data_dictionary_term_id: Optional[int] = Field(None, description="Filtrar por termo do dicionário")

class SearchColumnMapping(BaseSearchRequest):
    group_id: Optional[int] = Field(None, description="Filtrar por grupo")
    column_id: Optional[int] = Field(None, description="Filtrar por coluna")

class SearchValueMapping(BaseSearchRequest):
    group_id: Optional[int] = Field(None, description="Filtrar por grupo")
    source_column_id: Optional[int] = Field(None, description="Filtrar por coluna de origem")
    source_value: Optional[str] = Field(None, description="Filtrar por valor de origem")

class EquivalenceSearchRequest(BaseSearchRequest):
    query: Optional[str] = Field(None, description="Texto de busca")
    semantic_domain_id: Optional[int] = Field(None, description="Filtrar por domínio semântico")
    data_type: Optional[str] = Field(None, description="Filtrar por tipo de dados")
    confidence_threshold: Optional[Decimal] = Field(None, description="Limite mínimo de confiança")
