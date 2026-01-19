from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from decimal import Decimal
from datetime import datetime
from app.api.schemas.search import BaseSearchRequest


# ==================== SEMANTIC DOMAIN ====================

class SemanticDomainBase(BaseModel):
    name: str = Field(..., max_length=100, description="Nome do domínio semântico")
    description: Optional[str] = Field(None, description="Descrição do domínio semântico")
    parent_domain_id: Optional[int] = Field(None, description="ID do domínio pai")
    color: Optional[str] = Field(None, max_length=7, description="Cor hex do domínio (ex: #3b82f6)")
    domain_rules: Dict[str, Any] = Field(default_factory=dict, description="Regras do domínio")


class SemanticDomainCreate(SemanticDomainBase):
    pass


class SemanticDomainUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    parent_domain_id: Optional[int] = None
    color: Optional[str] = Field(None, max_length=7)
    domain_rules: Optional[Dict[str, Any]] = None


class SemanticDomainResponse(SemanticDomainBase):
    id: int
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True
    }


class SemanticDomainWithCountsResponse(SemanticDomainResponse):
    """Response com contagens para listagem"""
    terms_count: int = 0  # Quantidade de termos no dicionário vinculados


# ==================== DATA DICTIONARY ====================

class DataDictionaryBase(BaseModel):
    name: str = Field(..., max_length=100, description="Nome do termo")
    display_name: str = Field(..., max_length=255, description="Nome de exibição")
    description: str = Field(..., description="Descrição do termo")
    semantic_domain_id: Optional[int] = Field(None, description="ID do domínio semântico")
    data_type: str = Field(..., max_length=50, description="Tipo de dados (STRING, INTEGER, DATE, ENUM, DECIMAL)")
    standard_values: Optional[List[str]] = Field(None, description="Valores padrão permitidos (ex: ['M', 'F', 'O'])")
    validation_rules: Dict[str, Any] = Field(default_factory=dict, description="Regras de validação")
    example_values: Dict[str, Any] = Field(default_factory=dict, description="Valores de exemplo")
    synonyms: Optional[List[str]] = Field(None, description="Sinônimos do termo")


class DataDictionaryCreate(DataDictionaryBase):
    pass


class DataDictionaryUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    display_name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    semantic_domain_id: Optional[int] = None
    data_type: Optional[str] = Field(None, max_length=50)
    standard_values: Optional[List[str]] = None
    validation_rules: Optional[Dict[str, Any]] = None
    example_values: Optional[Dict[str, Any]] = None
    synonyms: Optional[List[str]] = None


class DataDictionaryResponse(DataDictionaryBase):
    id: int
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True
    }


class DataDictionaryWithDetailsResponse(DataDictionaryResponse):
    """Response com detalhes adicionais via JOIN"""
    semantic_domain_name: Optional[str] = None
    semantic_domain_color: Optional[str] = None
    column_groups_count: int = 0


# ==================== COLUMN GROUP ====================

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
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True
    }


class ColumnGroupWithDetailsResponse(ColumnGroupResponse):
    """Response com detalhes adicionais via JOIN"""
    data_dictionary_term_name: Optional[str] = None
    data_dictionary_term_display_name: Optional[str] = None
    standard_values: Optional[List[str]] = None  # Do termo vinculado
    semantic_domain_name: Optional[str] = None
    semantic_domain_color: Optional[str] = None
    columns_count: int = 0
    value_mappings_count: int = 0


# ==================== COLUMN MAPPING ====================

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
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True
    }


class ColumnMappingWithDetailsResponse(ColumnMappingResponse):
    """Response com detalhes da coluna via JOIN"""
    column_name: Optional[str] = None
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    data_type: Optional[str] = None
    data_source_name: Optional[str] = None  # Nome da conexão de dados
    data_source_type: Optional[str] = None  # Tipo da conexão (PostgreSQL, MongoDB, etc.)
    sample_values: Optional[List[Any]] = None  # Valores de amostra da coluna


# ==================== VALUE MAPPING ====================

class ValueMappingBase(BaseModel):
    group_id: int = Field(..., description="ID do grupo de colunas")
    source_column_id: int = Field(..., description="ID da coluna de origem")
    source_value: str = Field(..., description="Valor de origem")
    standard_value: str = Field(..., description="Valor padronizado")
    description: Optional[str] = Field(None, description="Descrição do mapeamento")
    record_count: Optional[int] = Field(None, description="Contagem de registros com esse valor")


class ValueMappingCreate(BaseModel):
    """Schema para criação - não inclui record_count que é calculado"""
    group_id: int = Field(..., description="ID do grupo de colunas")
    source_column_id: int = Field(..., description="ID da coluna de origem")
    source_value: str = Field(..., description="Valor de origem")
    standard_value: str = Field(..., description="Valor padronizado")
    description: Optional[str] = Field(None, description="Descrição do mapeamento")


class ValueMappingUpdate(BaseModel):
    source_value: Optional[str] = None
    standard_value: Optional[str] = None
    description: Optional[str] = None
    record_count: Optional[int] = None


class ValueMappingResponse(ValueMappingBase):
    id: int
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True
    }


class ValueMappingWithDetailsResponse(ValueMappingResponse):
    """Response com detalhes da coluna via JOIN"""
    source_column_name: Optional[str] = None
    table_name: Optional[str] = None
    data_source_name: Optional[str] = None


# ==================== COMPLEX RESPONSES ====================

class ColumnGroupWithMappingsResponse(ColumnGroupWithDetailsResponse):
    """Response completo do grupo com todos os mapeamentos"""
    column_mappings: List[ColumnMappingWithDetailsResponse] = []
    value_mappings: List[ValueMappingWithDetailsResponse] = []


# ==================== BULK OPERATIONS ====================

class BulkColumnMappingCreate(BaseModel):
    group_id: int
    mappings: List[ColumnMappingCreate]


class BulkValueMappingCreate(BaseModel):
    group_id: int
    mappings: List[ValueMappingCreate]


# ==================== SEARCH SCHEMAS ====================

class SearchSemanticDomain(BaseSearchRequest):
    name: Optional[str] = Field(None, description="Filtrar por nome")
    parent_domain_id: Optional[int] = Field(None, description="Filtrar por domínio pai")


class SearchDataDictionary(BaseSearchRequest):
    name: Optional[str] = Field(None, description="Filtrar por nome ou sinônimos")
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
