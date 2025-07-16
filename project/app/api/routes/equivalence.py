from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from sqlalchemy import and_, or_, func
from typing import List, Optional, Dict, Any
from sqlalchemy import join

from ...database.database import get_db
from ...database.equivalence import equivalence
from ...database.metadata import metadata
from ...crud.token import get_current_user
from ..schemas.equivalence_schemas import (
    SemanticDomainCreate,
    SemanticDomainUpdate,
    SemanticDomainResponse,
    DataDictionaryCreate,
    DataDictionaryUpdate,
    DataDictionaryResponse,
    ColumnGroupCreate,
    ColumnGroupUpdate,
    ColumnGroupResponse,
    ColumnGroupWithMappingsResponse,
    ColumnMappingCreate,
    ColumnMappingUpdate,
    ColumnMappingResponse,
    ColumnMappingWithDetailsResponse,
    ValueMappingCreate,
    ValueMappingUpdate,
    ValueMappingResponse,
    BulkColumnMappingCreate,
    BulkValueMappingCreate,
    EquivalenceSearchRequest,
    SearchSemanticDomain,
    SearchDataDictionary,
    SearchColumnGroup,
    SearchColumnMapping,
    SearchValueMapping
)
from ..schemas.search import SearchResult

from ...database.core import core
# from ...services.connectors.postgres_connector import test_postgres_connection
# from ...services.distinct_values_service import DistinctValuesService

router = APIRouter()

# ==================== SEMANTIC DOMAINS ====================

@router.get("/semantic-domains", response_model=List[SemanticDomainResponse])
async def list_semantic_domains(
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Lista todos os domínios semânticos."""
    try:
        result = await db.execute(
            select(equivalence.SemanticDomain)
            .order_by(equivalence.SemanticDomain.name)
        )
        domains = result.scalars().all()
        return domains
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao listar domínios semânticos: {str(e)}"
        )

@router.post("/semantic-domains", response_model=SemanticDomainResponse)
async def create_semantic_domain(
    domain_data: SemanticDomainCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Cria um novo domínio semântico."""
    try:
        # Verificar se já existe um domínio com o mesmo nome
        existing = await db.execute(
            select(equivalence.SemanticDomain)
            .where(equivalence.SemanticDomain.name == domain_data.name)
        )
        if existing.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Domínio semântico com nome '{domain_data.name}' já existe"
            )

        # Verificar se o parent_domain_id existe (se fornecido)
        if domain_data.parent_domain_id:
            parent_result = await db.execute(
                select(equivalence.SemanticDomain)
                .where(equivalence.SemanticDomain.id == domain_data.parent_domain_id)
            )
            if not parent_result.scalars().first():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Domínio pai com ID {domain_data.parent_domain_id} não encontrado"
                )

        domain = equivalence.SemanticDomain(**domain_data.dict())
        db.add(domain)
        await db.commit()
        await db.refresh(domain)
        return domain
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao criar domínio semântico: {str(e)}"
        )

@router.get("/semantic-domains/{domain_id}", response_model=SemanticDomainResponse)
async def get_semantic_domain(
    domain_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Busca um domínio semântico por ID."""
    try:
        result = await db.execute(
            select(equivalence.SemanticDomain)
            .where(equivalence.SemanticDomain.id == domain_id)
        )
        domain = result.scalars().first()
        
        if not domain:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Domínio semântico com ID {domain_id} não encontrado"
            )
        
        return domain
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar domínio semântico: {str(e)}"
        )

@router.put("/semantic-domains/{domain_id}", response_model=SemanticDomainResponse)
async def update_semantic_domain(
    domain_id: int,
    domain_data: SemanticDomainUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Atualiza um domínio semântico."""
    try:
        result = await db.execute(
            select(equivalence.SemanticDomain)
            .where(equivalence.SemanticDomain.id == domain_id)
        )
        domain = result.scalars().first()
        
        if not domain:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Domínio semântico com ID {domain_id} não encontrado"
            )

        # Verificar nome único se estiver sendo alterado
        if domain_data.name and domain_data.name != domain.name:
            existing = await db.execute(
                select(equivalence.SemanticDomain)
                .where(equivalence.SemanticDomain.name == domain_data.name)
            )
            if existing.scalars().first():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Domínio semântico com nome '{domain_data.name}' já existe"
                )

        # Atualizar campos
        update_data = domain_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(domain, field, value)

        await db.commit()
        await db.refresh(domain)
        return domain
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao atualizar domínio semântico: {str(e)}"
        )

@router.delete("/semantic-domains/{domain_id}")
async def delete_semantic_domain(
    domain_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Remove um domínio semântico."""
    try:
        result = await db.execute(
            select(equivalence.SemanticDomain)
            .where(equivalence.SemanticDomain.id == domain_id)
        )
        domain = result.scalars().first()
        
        if not domain:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Domínio semântico com ID {domain_id} não encontrado"
            )

        await db.delete(domain)
        await db.commit()
        return {"message": "Domínio semântico removido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao remover domínio semântico: {str(e)}"
        )

@router.post("/semantic-domains/search", response_model=SearchResult[SemanticDomainResponse])
async def search_semantic_domains(
    search: SearchSemanticDomain,
    db: AsyncSession = Depends(get_db),
):
    """Search semantic domains with pagination and filters"""
    try:
        query = select(equivalence.SemanticDomain)
        
        # Apply filters
        if search.name:
            query = query.where(equivalence.SemanticDomain.name.ilike(f"%{search.name}%"))
        
        if search.parent_domain_id is not None:
            query = query.where(equivalence.SemanticDomain.parent_domain_id == search.parent_domain_id)
        
        # Get total count if requested
        total = 0
        if search.pagination.query_total:
            count_result = await db.execute(
                select(func.count()).select_from(query.subquery())
            )
            total = count_result.scalar()
        
        # Apply pagination
        if search.pagination.skip:
            query = query.offset(search.pagination.skip)
        if search.pagination.limit:
            query = query.limit(search.pagination.limit)
        
        query = query.order_by(equivalence.SemanticDomain.name)
        
        result = await db.execute(query)
        domains = result.scalars().all()
        
        # If query_total is false, set total to the count of returned items
        if not search.pagination.query_total:
            total = len(domains)
        
        return SearchResult(
            total=total,
            items=domains
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar domínios semânticos: {str(e)}"
        )

# ==================== DATA DICTIONARY ====================

@router.get("/data-dictionary", response_model=List[DataDictionaryResponse])
async def list_data_dictionary_terms(
    semantic_domain_id: Optional[int] = Query(None, description="Filtrar por domínio semântico"),
    data_type: Optional[str] = Query(None, description="Filtrar por tipo de dados"),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Lista termos do dicionário de dados."""
    try:
        query = select(equivalence.DataDictionary)
        
        if semantic_domain_id:
            query = query.where(equivalence.DataDictionary.semantic_domain_id == semantic_domain_id)
        if data_type:
            query = query.where(equivalence.DataDictionary.data_type == data_type)
            
        query = query.order_by(equivalence.DataDictionary.name)
        
        result = await db.execute(query)
        terms = result.scalars().all()
        return terms
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao listar termos do dicionário: {str(e)}"
        )

@router.post("/data-dictionary", response_model=DataDictionaryResponse)
async def create_data_dictionary_term(
    term_data: DataDictionaryCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Cria um novo termo no dicionário de dados."""
    try:
        # Verificar se já existe um termo com o mesmo nome
        existing = await db.execute(
            select(equivalence.DataDictionary)
            .where(equivalence.DataDictionary.name == term_data.name)
        )
        if existing.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Termo com nome '{term_data.name}' já existe"
            )

        term = equivalence.DataDictionary(**term_data.dict())
        db.add(term)
        await db.commit()
        await db.refresh(term)
        return term
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao criar termo: {str(e)}"
        )

@router.get("/data-dictionary/{term_id}", response_model=DataDictionaryResponse)
async def get_data_dictionary_term(
    term_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Busca um termo do dicionário por ID."""
    try:
        result = await db.execute(
            select(equivalence.DataDictionary)
            .where(equivalence.DataDictionary.id == term_id)
        )
        term = result.scalars().first()
        
        if not term:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Termo com ID {term_id} não encontrado"
            )
        
        return term
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar termo: {str(e)}"
        )

@router.put("/data-dictionary/{term_id}", response_model=DataDictionaryResponse)
async def update_data_dictionary_term(
    term_id: int,
    term_data: DataDictionaryUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Atualiza um termo do dicionário."""
    try:
        result = await db.execute(
            select(equivalence.DataDictionary)
            .where(equivalence.DataDictionary.id == term_id)
        )
        term = result.scalars().first()
        
        if not term:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Termo com ID {term_id} não encontrado"
            )

        # Verificar nome único se estiver sendo alterado
        if term_data.name and term_data.name != term.name:
            existing = await db.execute(
                select(equivalence.DataDictionary)
                .where(equivalence.DataDictionary.name == term_data.name)
            )
            if existing.scalars().first():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Termo com nome '{term_data.name}' já existe"
                )

        update_data = term_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(term, field, value)

        await db.commit()
        await db.refresh(term)
        return term
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao atualizar termo: {str(e)}"
        )

@router.delete("/data-dictionary/{term_id}")
async def delete_data_dictionary_term(
    term_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Remove um termo do dicionário."""
    try:
        result = await db.execute(
            select(equivalence.DataDictionary)
            .where(equivalence.DataDictionary.id == term_id)
        )
        term = result.scalars().first()
        
        if not term:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Termo com ID {term_id} não encontrado"
            )

        await db.delete(term)
        await db.commit()
        return {"message": "Termo removido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao remover termo: {str(e)}"
        )
    
# ==================== DATA DICTIONARY ====================

@router.post("/data-dictionary/search", response_model=SearchResult[DataDictionaryResponse])
async def search_data_dictionary(
    search: SearchDataDictionary,
    db: AsyncSession = Depends(get_db),
):
    """Search data dictionary terms with pagination and filters"""
    try:
        query = select(equivalence.DataDictionary)
        
        # Apply filters
        if search.name:
            query = query.where(equivalence.DataDictionary.name.ilike(f"%{search.name}%"))
        
        if search.semantic_domain_id is not None:
            query = query.where(equivalence.DataDictionary.semantic_domain_id == search.semantic_domain_id)
        
        if search.data_type:
            query = query.where(equivalence.DataDictionary.data_type == search.data_type)
        
        # Get total count if requested
        total = 0
        if search.pagination.query_total:
            count_result = await db.execute(
                select(func.count()).select_from(query.subquery())
            )
            total = count_result.scalar()
        
        # Apply pagination
        if search.pagination.skip:
            query = query.offset(search.pagination.skip)
        if search.pagination.limit:
            query = query.limit(search.pagination.limit)
        
        query = query.order_by(equivalence.DataDictionary.name)
        
        result = await db.execute(query)
        terms = result.scalars().all()
        
        # If query_total is false, set total to the count of returned items
        if not search.pagination.query_total:
            total = len(terms)
        
        return SearchResult(
            total=total,
            items=terms
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar termos do dicionário: {str(e)}"
        )

# ==================== COLUMN GROUPS ====================

@router.get("/column-groups", response_model=List[ColumnGroupResponse])
async def list_column_groups(
    semantic_domain_id: Optional[int] = Query(None, description="Filtrar por domínio semântico"),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Lista grupos de colunas."""
    try:
        query = select(equivalence.ColumnGroup)
        
        if semantic_domain_id:
            query = query.where(equivalence.ColumnGroup.semantic_domain_id == semantic_domain_id)
            
        query = query.order_by(equivalence.ColumnGroup.name)
        
        result = await db.execute(query)
        groups = result.scalars().all()
        return groups
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao listar grupos de colunas: {str(e)}"
        )

@router.post("/column-groups", response_model=ColumnGroupResponse)
async def create_column_group(
    group_data: ColumnGroupCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Cria um novo grupo de colunas."""
    try:
        group = equivalence.ColumnGroup(**group_data.dict())
        db.add(group)
        await db.commit()
        await db.refresh(group)
        return group
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao criar grupo de colunas: {str(e)}"
        )

@router.get("/column-groups/{group_id}", response_model=ColumnGroupWithMappingsResponse)
async def get_column_group(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Busca um grupo de colunas com seus mapeamentos."""
    try:
        # Buscar o grupo
        group_result = await db.execute(
            select(equivalence.ColumnGroup)
            .where(equivalence.ColumnGroup.id == group_id)
        )
        group = group_result.scalars().first()
        
        if not group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Grupo de colunas com ID {group_id} não encontrado"
            )

        # Buscar mapeamentos de colunas
        column_mappings_result = await db.execute(
            select(equivalence.ColumnMapping)
            .where(equivalence.ColumnMapping.group_id == group_id)
        )
        column_mappings = column_mappings_result.scalars().all()

        # Buscar mapeamentos de valores
        value_mappings_result = await db.execute(
            select(equivalence.ValueMapping)
            .where(equivalence.ValueMapping.group_id == group_id)
        )
        value_mappings = value_mappings_result.scalars().all()

        return ColumnGroupWithMappingsResponse(
            **group.__dict__,
            column_mappings=column_mappings,
            value_mappings=value_mappings
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar grupo de colunas: {str(e)}"
        )

@router.put("/column-groups/{group_id}", response_model=ColumnGroupResponse)
async def update_column_group(
    group_id: int,
    group_data: ColumnGroupUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Atualiza um grupo de colunas."""
    try:
        result = await db.execute(
            select(equivalence.ColumnGroup)
            .where(equivalence.ColumnGroup.id == group_id)
        )
        group = result.scalars().first()
        
        if not group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Grupo de colunas com ID {group_id} não encontrado"
            )

        update_data = group_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(group, field, value)

        await db.commit()
        await db.refresh(group)
        return group
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao atualizar grupo de colunas: {str(e)}"
        )

@router.delete("/column-groups/{group_id}")
async def delete_column_group(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Remove um grupo de colunas."""
    try:
        result = await db.execute(
            select(equivalence.ColumnGroup)
            .where(equivalence.ColumnGroup.id == group_id)
        )
        group = result.scalars().first()
        
        if not group:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Grupo de colunas com ID {group_id} não encontrado"
            )

        await db.delete(group)
        await db.commit()
        return {"message": "Grupo de colunas removido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao remover grupo de colunas: {str(e)}"
        )

@router.post("/column-groups/search", response_model=SearchResult[ColumnGroupResponse])
async def search_column_groups(
    search: SearchColumnGroup,
    db: AsyncSession = Depends(get_db),
):
    """Search column groups with pagination and filters"""
    try:
        query = select(equivalence.ColumnGroup)
        
        # Apply filters
        if search.name:
            query = query.where(equivalence.ColumnGroup.name.ilike(f"%{search.name}%"))
        
        if search.semantic_domain_id is not None:
            query = query.where(equivalence.ColumnGroup.semantic_domain_id == search.semantic_domain_id)
        
        if search.data_dictionary_term_id is not None:
            query = query.where(equivalence.ColumnGroup.data_dictionary_term_id == search.data_dictionary_term_id)
        
        # Get total count if requested
        total = 0
        if search.pagination.query_total:
            count_result = await db.execute(
                select(func.count()).select_from(query.subquery())
            )
            total = count_result.scalar()
        
        # Apply pagination
        if search.pagination.skip:
            query = query.offset(search.pagination.skip)
        if search.pagination.limit:
            query = query.limit(search.pagination.limit)
        
        query = query.order_by(equivalence.ColumnGroup.name)
        
        result = await db.execute(query)
        groups = result.scalars().all()
        
        # If query_total is false, set total to the count of returned items
        if not search.pagination.query_total:
            total = len(groups)
        
        return SearchResult(
            total=total,
            items=groups
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar grupos de colunas: {str(e)}"
        )

# ==================== COLUMN MAPPINGS ====================

@router.get("/column-groups/{group_id}/column-mappings", response_model=List[ColumnMappingWithDetailsResponse])
async def list_column_mappings(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Lista mapeamentos de colunas para um grupo."""
    try:
        result = await db.execute(
            select(
                equivalence.ColumnMapping,
                metadata.ExternalColumn.column_name,
                metadata.ExternalTables.table_name,
                metadata.ExternalSchema.schema_name,
                metadata.ExternalColumn.data_type
            )
            .join(metadata.ExternalColumn, equivalence.ColumnMapping.column_id == metadata.ExternalColumn.id)
            .join(metadata.ExternalTables, metadata.ExternalColumn.table_id == metadata.ExternalTables.id)
            .join(metadata.ExternalSchema, metadata.ExternalTables.schema_id == metadata.ExternalSchema.id)
            .where(equivalence.ColumnMapping.group_id == group_id)
        )
        
        mappings = []
        for row in result.all():
            mapping, column_name, table_name, schema_name, data_type = row
            mappings.append(ColumnMappingWithDetailsResponse(
                **mapping.__dict__,
                column_name=column_name,
                table_name=table_name,
                schema_name=schema_name,
                data_type=data_type
            ))
        
        return mappings
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao listar mapeamentos de colunas: {str(e)}"
        )

@router.post("/column-mappings", response_model=ColumnMappingResponse)
async def create_column_mapping(
    mapping_data: ColumnMappingCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Cria um novo mapeamento de coluna."""
    try:
        # Verificar se o grupo existe
        group_result = await db.execute(
            select(equivalence.ColumnGroup)
            .where(equivalence.ColumnGroup.id == mapping_data.group_id)
        )
        if not group_result.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Grupo de colunas com ID {mapping_data.group_id} não encontrado"
            )

        # Verificar se a coluna existe
        column_result = await db.execute(
            select(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.id == mapping_data.column_id)
        )
        if not column_result.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Coluna com ID {mapping_data.column_id} não encontrada"
            )

        mapping = equivalence.ColumnMapping(**mapping_data.dict())
        db.add(mapping)
        await db.commit()
        await db.refresh(mapping)
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao criar mapeamento de coluna: {str(e)}"
        )

@router.post("/column-mappings/bulk", response_model=List[ColumnMappingResponse])
async def create_bulk_column_mappings(
    bulk_data: BulkColumnMappingCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Cria múltiplos mapeamentos de colunas."""
    try:
        # Verificar se o grupo existe
        group_result = await db.execute(
            select(equivalence.ColumnGroup)
            .where(equivalence.ColumnGroup.id == bulk_data.group_id)
        )
        if not group_result.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Grupo de colunas com ID {bulk_data.group_id} não encontrado"
            )

        mappings = []
        for mapping_data in bulk_data.mappings:
            mapping = equivalence.ColumnMapping(**mapping_data.dict())
            db.add(mapping)
            mappings.append(mapping)

        await db.commit()
        
        # Refresh todos os mappings
        for mapping in mappings:
            await db.refresh(mapping)
            
        return mappings
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao criar mapeamentos em lote: {str(e)}"
        )

@router.put("/column-mappings/{mapping_id}", response_model=ColumnMappingResponse)
async def update_column_mapping(
    mapping_id: int,
    mapping_data: ColumnMappingUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Atualiza um mapeamento de coluna."""
    try:
        result = await db.execute(
            select(equivalence.ColumnMapping)
            .where(equivalence.ColumnMapping.id == mapping_id)
        )
        mapping = result.scalars().first()
        
        if not mapping:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Mapeamento com ID {mapping_id} não encontrado"
            )

        update_data = mapping_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(mapping, field, value)

        await db.commit()
        await db.refresh(mapping)
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao atualizar mapeamento: {str(e)}"
        )

@router.delete("/column-mappings/{mapping_id}")
async def delete_column_mapping(
    mapping_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Remove um mapeamento de coluna."""
    try:
        result = await db.execute(
            select(equivalence.ColumnMapping)
            .where(equivalence.ColumnMapping.id == mapping_id)
        )
        mapping = result.scalars().first()
        
        if not mapping:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Mapeamento com ID {mapping_id} não encontrado"
            )

        await db.delete(mapping)
        await db.commit()
        return {"message": "Mapeamento removido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao remover mapeamento: {str(e)}"
        )

@router.post("/column-mappings/search", response_model=SearchResult[ColumnMappingResponse])
async def search_column_mappings(
    search: SearchColumnMapping,
    db: AsyncSession = Depends(get_db),
):
    """Search column mappings with pagination and filters"""
    try:
        query = select(equivalence.ColumnMapping)
        
        # Apply filters
        if search.group_id is not None:
            query = query.where(equivalence.ColumnMapping.group_id == search.group_id)
        
        if search.column_id is not None:
            query = query.where(equivalence.ColumnMapping.column_id == search.column_id)
        
        # Get total count if requested
        total = 0
        if search.pagination.query_total:
            count_result = await db.execute(
                select(func.count()).select_from(query.subquery())
            )
            total = count_result.scalar()
        
        # Apply pagination
        if search.pagination.skip:
            query = query.offset(search.pagination.skip)
        if search.pagination.limit:
            query = query.limit(search.pagination.limit)
        
        query = query.order_by(equivalence.ColumnMapping.id)
        
        result = await db.execute(query)
        mappings = result.scalars().all()
        
        # If query_total is false, set total to the count of returned items
        if not search.pagination.query_total:
            total = len(mappings)
        
        return SearchResult(
            total=total,
            items=mappings
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar mapeamentos de colunas: {str(e)}"
        )

# ==================== VALUE MAPPINGS ====================

@router.get("/column-groups/{group_id}/value-mappings", response_model=List[ValueMappingResponse])
async def list_value_mappings(
    group_id: int,
    source_column_id: Optional[int] = Query(None, description="Filtrar por coluna de origem"),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Lista mapeamentos de valores para um grupo."""
    try:
        query = select(equivalence.ValueMapping).where(
            equivalence.ValueMapping.group_id == group_id
        )
        
        if source_column_id:
            query = query.where(equivalence.ValueMapping.source_column_id == source_column_id)
            
        result = await db.execute(query)
        mappings = result.scalars().all()
        return mappings
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao listar mapeamentos de valores: {str(e)}"
        )

@router.post("/value-mappings", response_model=ValueMappingResponse)
async def create_value_mapping(
    mapping_data: ValueMappingCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Cria um novo mapeamento de valor."""
    try:
        # Verificar se o grupo existe
        group_result = await db.execute(
            select(equivalence.ColumnGroup)
            .where(equivalence.ColumnGroup.id == mapping_data.group_id)
        )
        if not group_result.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Grupo de colunas com ID {mapping_data.group_id} não encontrado"
            )

        mapping = equivalence.ValueMapping(**mapping_data.dict())
        db.add(mapping)
        await db.commit()
        await db.refresh(mapping)
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao criar mapeamento de valor: {str(e)}"
        )

@router.post("/value-mappings/bulk", response_model=List[ValueMappingResponse])
async def create_bulk_value_mappings(
    bulk_data: BulkValueMappingCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Cria múltiplos mapeamentos de valores."""
    try:
        # Verificar se o grupo existe
        group_result = await db.execute(
            select(equivalence.ColumnGroup)
            .where(equivalence.ColumnGroup.id == bulk_data.group_id)
        )
        if not group_result.scalars().first():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Grupo de colunas com ID {bulk_data.group_id} não encontrado"
            )

        mappings = []
        for mapping_data in bulk_data.mappings:
            mapping = equivalence.ValueMapping(**mapping_data.dict())
            db.add(mapping)
            mappings.append(mapping)

        await db.commit()
        
        # Refresh todos os mappings
        for mapping in mappings:
            await db.refresh(mapping)
            
        return mappings
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao criar mapeamentos de valores em lote: {str(e)}"
        )

@router.put("/value-mappings/{mapping_id}", response_model=ValueMappingResponse)
async def update_value_mapping(
    mapping_id: int,
    mapping_data: ValueMappingUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Atualiza um mapeamento de valor."""
    try:
        result = await db.execute(
            select(equivalence.ValueMapping)
            .where(equivalence.ValueMapping.id == mapping_id)
        )
        mapping = result.scalars().first()
        
        if not mapping:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Mapeamento de valor com ID {mapping_id} não encontrado"
            )

        update_data = mapping_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(mapping, field, value)

        await db.commit()
        await db.refresh(mapping)
        return mapping
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao atualizar mapeamento de valor: {str(e)}"
        )

@router.delete("/value-mappings/{mapping_id}")
async def delete_value_mapping(
    mapping_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Remove um mapeamento de valor."""
    try:
        result = await db.execute(
            select(equivalence.ValueMapping)
            .where(equivalence.ValueMapping.id == mapping_id)
        )
        mapping = result.scalars().first()
        
        if not mapping:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Mapeamento de valor com ID {mapping_id} não encontrado"
            )

        await db.delete(mapping)
        await db.commit()
        return {"message": "Mapeamento de valor removido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao remover mapeamento de valor: {str(e)}"
        )

# ==================== VALUE MAPPINGS ====================

@router.post("/value-mappings/search", response_model=SearchResult[ValueMappingResponse])
async def search_value_mappings(
    search: SearchValueMapping,
    db: AsyncSession = Depends(get_db),
):
    """Search value mappings with pagination and filters"""
    try:
        query = select(equivalence.ValueMapping)
        
        # Apply filters
        if search.group_id is not None:
            query = query.where(equivalence.ValueMapping.group_id == search.group_id)
        
        if search.source_column_id is not None:
            query = query.where(equivalence.ValueMapping.source_column_id == search.source_column_id)
        
        if search.source_value:
            query = query.where(equivalence.ValueMapping.source_value.ilike(f"%{search.source_value}%"))
        
        # Get total count if requested
        total = 0
        if search.pagination.query_total:
            count_result = await db.execute(
                select(func.count()).select_from(query.subquery())
            )
            total = count_result.scalar()
        
        # Apply pagination
        if search.pagination.skip:
            query = query.offset(search.pagination.skip)
        if search.pagination.limit:
            query = query.limit(search.pagination.limit)
        
        query = query.order_by(equivalence.ValueMapping.id)
        
        result = await db.execute(query)
        mappings = result.scalars().all()
        
        # If query_total is false, set total to the count of returned items
        if not search.pagination.query_total:
            total = len(mappings)
        
        return SearchResult(
            total=total,
            items=mappings
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar mapeamentos de valores: {str(e)}"
        )


# ==================== SEARCH AND SUGGESTIONS ====================

@router.post("/search/columns")
async def search_unmapped_columns(
    search_request: EquivalenceSearchRequest,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Busca colunas não mapeadas baseado em critérios."""
    try:
        # Subquery para colunas já mapeadas
        mapped_columns_subquery = select(equivalence.ColumnMapping.column_id)
        
        # Query principal para colunas não mapeadas
        query = select(
            metadata.ExternalColumn,
            metadata.ExternalTables.table_name,
            metadata.ExternalSchema.schema_name
        ).select_from(
            metadata.ExternalColumn
            .join(metadata.ExternalTables, metadata.ExternalColumn.table_id == metadata.ExternalTables.id)
            .join(metadata.ExternalSchema, metadata.ExternalTables.schema_id == metadata.ExternalSchema.id)
        ).where(
            metadata.ExternalColumn.id.notin_(mapped_columns_subquery)
        )
        
        # Aplicar filtros
        if search_request.query:
            search_pattern = f"%{search_request.query}%"
            query = query.where(
                or_(
                    metadata.ExternalColumn.column_name.ilike(search_pattern),
                    metadata.ExternalColumn.description.ilike(search_pattern),
                    metadata.ExternalTables.table_name.ilike(search_pattern)
                )
            )
            
        if search_request.data_type:
            query = query.where(metadata.ExternalColumn.data_type == search_request.data_type)
        
        query = query.limit(100)  # Limitar resultados
        
        result = await db.execute(query)
        rows = result.all()
        
        columns = []
        for row in rows:
            column, table_name, schema_name = row
            columns.append({
                "id": column.id,
                "column_name": column.column_name,
                "data_type": column.data_type,
                "table_name": table_name,
                "schema_name": schema_name,
                "description": column.description,
                "sample_values": column.sample_values[:5] if column.sample_values else []
            })
        
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar colunas: {str(e)}"
        )

# ==================== UTILITY ENDPOINTS ====================

@router.get("/available-columns")
async def get_available_columns_for_mapping(
    connection_id: Optional[int] = Query(None, description="Filtrar por conexão"),
    schema_id: Optional[int] = Query(None, description="Filtrar por schema"),
    table_id: Optional[int] = Query(None, description="Filtrar por tabela"),
    exclude_mapped: bool = Query(True, description="Excluir colunas já mapeadas"),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Lista colunas disponíveis para mapeamento."""
    try:        
        # Create explicit joins
        column_table_join = join(
            metadata.ExternalColumn,
            metadata.ExternalTables,
            metadata.ExternalColumn.table_id == metadata.ExternalTables.id
        )
        
        full_join = join(
            column_table_join,
            metadata.ExternalSchema,
            metadata.ExternalTables.schema_id == metadata.ExternalSchema.id
        )
        
        query = select(
            metadata.ExternalColumn,
            metadata.ExternalTables.table_name,
            metadata.ExternalSchema.schema_name
        ).select_from(full_join)
        
        # Aplicar filtros
        if connection_id:
            query = query.where(metadata.ExternalTables.connection_id == connection_id)
        if schema_id:
            query = query.where(metadata.ExternalTables.schema_id == schema_id)
        if table_id:
            query = query.where(metadata.ExternalColumn.table_id == table_id)
            
        if exclude_mapped:
            # Subquery para colunas já mapeadas
            mapped_columns_subquery = select(equivalence.ColumnMapping.column_id)
            query = query.where(metadata.ExternalColumn.id.notin_(mapped_columns_subquery))
        
        query = query.order_by(
            metadata.ExternalSchema.schema_name,
            metadata.ExternalTables.table_name,
            metadata.ExternalColumn.column_position
        ).limit(1000)
        
        result = await db.execute(query)
        rows = result.all()
        
        columns = []
        for row in rows:
            column, table_name, schema_name = row
            columns.append({
                "id": column.id,
                "column_name": column.column_name,
                "data_type": column.data_type,
                "table_name": table_name,
                "schema_name": schema_name,
                "table_id": column.table_id,
                "description": column.description,
                "is_nullable": column.is_nullable,
                "sample_values": column.sample_values[:3] if column.sample_values else []
            })
        
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar colunas disponíveis: {str(e)}"
        )

@router.get("/statistics/mapping-coverage")
async def get_mapping_coverage_statistics(
    connection_id: Optional[int] = Query(None, description="Filtrar por conexão"),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Retorna estatísticas de cobertura dos mapeamentos."""
    try:
        from sqlalchemy import join, outerjoin
        
        # Query base para contar colunas totais
        total_columns_query = select(func.count(metadata.ExternalColumn.id))
        if connection_id:
            column_table_join = join(
                metadata.ExternalColumn,
                metadata.ExternalTables,
                metadata.ExternalColumn.table_id == metadata.ExternalTables.id
            )
            total_columns_query = total_columns_query.select_from(
                column_table_join
            ).where(metadata.ExternalTables.connection_id == connection_id)
        
        # Query para contar colunas mapeadas
        mapped_columns_query = select(func.count(equivalence.ColumnMapping.id.distinct()))
        if connection_id:
            mapping_column_join = join(
                equivalence.ColumnMapping,
                metadata.ExternalColumn,
                equivalence.ColumnMapping.column_id == metadata.ExternalColumn.id
            )
            mapping_table_join = join(
                mapping_column_join,
                metadata.ExternalTables,
                metadata.ExternalColumn.table_id == metadata.ExternalTables.id
            )
            mapped_columns_query = mapped_columns_query.select_from(
                mapping_table_join
            ).where(metadata.ExternalTables.connection_id == connection_id)
        
        # Executar queries
        total_result = await db.execute(total_columns_query)
        total_columns = total_result.scalar() or 0
        
        mapped_result = await db.execute(mapped_columns_query)
        mapped_columns = mapped_result.scalar() or 0
        
        # Calcular percentuais
        coverage_percentage = (mapped_columns / total_columns * 100) if total_columns > 0 else 0
        
        # Estatísticas por tipo de dados
        column_mapping_outerjoin = outerjoin(
            metadata.ExternalColumn,
            equivalence.ColumnMapping,
            metadata.ExternalColumn.id == equivalence.ColumnMapping.column_id
        )
        
        data_type_stats_query = select(
            metadata.ExternalColumn.data_type,
            func.count(metadata.ExternalColumn.id).label('total'),
            func.count(equivalence.ColumnMapping.id).label('mapped')
        ).select_from(column_mapping_outerjoin)
        
        if connection_id:
            column_table_outerjoin = join(
                metadata.ExternalColumn,
                metadata.ExternalTables,
                metadata.ExternalColumn.table_id == metadata.ExternalTables.id
            )
            full_outerjoin = outerjoin(
                column_table_outerjoin,
                equivalence.ColumnMapping,
                metadata.ExternalColumn.id == equivalence.ColumnMapping.column_id
            )
            data_type_stats_query = select(
                metadata.ExternalColumn.data_type,
                func.count(metadata.ExternalColumn.id).label('total'),
                func.count(equivalence.ColumnMapping.id).label('mapped')
            ).select_from(full_outerjoin).where(
                metadata.ExternalTables.connection_id == connection_id
            )
        
        data_type_stats_query = data_type_stats_query.group_by(metadata.ExternalColumn.data_type)
        
        data_type_result = await db.execute(data_type_stats_query)
        data_type_stats = []
        for row in data_type_result.all():
            data_type, total, mapped = row
            mapped = mapped or 0
            data_type_stats.append({
                "data_type": data_type,
                "total_columns": total,
                "mapped_columns": mapped,
                "coverage_percentage": round((mapped / total * 100), 2) if total > 0 else 0
            })
        
        return {
            "total_columns": total_columns,
            "mapped_columns": mapped_columns,
            "unmapped_columns": total_columns - mapped_columns,
            "coverage_percentage": round(coverage_percentage, 2),
            "data_type_breakdown": data_type_stats
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao calcular estatísticas: {str(e)}"
        )