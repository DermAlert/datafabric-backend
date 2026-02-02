from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc
from sqlalchemy.orm import selectinload, joinedload
from typing import Optional, List, Dict, Any
from fastapi import HTTPException, status
import json
import secrets
from datetime import datetime, timedelta

from ...database.delta_sharing.delta_sharing import (
    Share, ShareSchema, ShareTable, Recipient, 
    RecipientAccessLog, ShareTableVersion, ShareTableFile,
    ShareStatus, TableShareStatus, recipient_shares
)
from ...database.core.core import Dataset, Organization
from ...api.schemas.delta_sharing_schemas import (
    ShareCreate, ShareUpdate, ShareDetail,
    SchemaCreate, SchemaUpdate, SchemaDetail,
    ShareTableCreate, ShareTableUpdate, ShareTableDetail,
    RecipientCreate, RecipientUpdate, RecipientDetail,
    RecipientBasic, ShareBasic,
    SearchShares, SearchSchemas, SearchTables, SearchRecipients
)
from ...api.schemas.search import SearchResult

class ShareService:
    """Service for managing Delta Sharing shares"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_share(self, share_data: ShareCreate, organization_id: int) -> ShareDetail:
        """Create a new share"""
        # Check if share name already exists
        existing = await self.db.execute(
            select(Share).where(Share.name == share_data.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Share with name '{share_data.name}' already exists"
            )
        
        # Create new share
        db_share = Share(
            name=share_data.name,
            description=share_data.description,
            organization_id=organization_id,
            owner_email=share_data.owner_email,
            contact_info=share_data.contact_info,
            terms_of_use=share_data.terms_of_use,
            status=ShareStatus.ACTIVE
        )
        
        self.db.add(db_share)
        await self.db.commit()
        await self.db.refresh(db_share)
        
        return await self._build_share_detail(db_share)
    
    async def get_share(self, share_id: int, organization_id: int) -> ShareDetail:
        """Get share by ID"""
        query = select(Share).where(
            and_(Share.id == share_id, Share.organization_id == organization_id)
        )
        result = await self.db.execute(query)
        db_share = result.scalar_one_or_none()
        
        if not db_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Share not found"
            )
        
        return await self._build_share_detail(db_share)
    
    async def get_share_by_name(self, share_name: str, organization_id: int) -> ShareDetail:
        """Get share by name"""
        query = select(Share).where(
            and_(Share.name == share_name, Share.organization_id == organization_id)
        )
        result = await self.db.execute(query)
        db_share = result.scalar_one_or_none()
        
        if not db_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Share not found"
            )
        
        return await self._build_share_detail(db_share)
    
    async def update_share(self, share_id: int, share_data: ShareUpdate, organization_id: int) -> ShareDetail:
        """Update an existing share"""
        query = select(Share).where(
            and_(Share.id == share_id, Share.organization_id == organization_id)
        )
        result = await self.db.execute(query)
        db_share = result.scalar_one_or_none()
        
        if not db_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Share not found"
            )
        
        # Update fields
        update_data = share_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_share, field, value)
        
        await self.db.commit()
        await self.db.refresh(db_share)
        
        return await self._build_share_detail(db_share)
    
    async def delete_share(self, share_id: int, organization_id: int) -> None:
        """Delete a share"""
        query = select(Share).where(
            and_(Share.id == share_id, Share.organization_id == organization_id)
        )
        result = await self.db.execute(query)
        db_share = result.scalar_one_or_none()
        
        if not db_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Share not found"
            )
        
        await self.db.delete(db_share)
        await self.db.commit()
    
    async def list_shares(self, search_params: SearchShares, organization_id: int) -> SearchResult[ShareDetail]:
        """List shares with pagination and filtering"""
        query = select(Share).where(Share.organization_id == organization_id)
        
        # Apply filters
        if search_params.search:
            search_term = f"%{search_params.search}%"
            query = query.where(
                or_(
                    Share.name.ilike(search_term),
                    Share.description.ilike(search_term)
                )
            )
        
        if search_params.status:
            query = query.where(Share.status == search_params.status)
        
        # Count total
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.db.execute(count_query)
        total = total_result.scalar()
        
        # Apply pagination
        query = query.order_by(desc(Share.data_criacao))
        query = query.offset((search_params.page - 1) * search_params.size)
        query = query.limit(search_params.size)
        
        result = await self.db.execute(query)
        shares = result.scalars().all()
        
        # Build response items
        items = []
        for share in shares:
            items.append(await self._build_share_detail(share))
        
        return SearchResult(
            items=items,
            total=total,
            page=search_params.page,
            size=search_params.size,
            pages=((total - 1) // search_params.size) + 1 if total > 0 else 0
        )
    
    async def _build_share_detail(self, share: Share) -> ShareDetail:
        """Build detailed share information"""
        # Count schemas and tables
        schemas_count_query = select(func.count()).where(ShareSchema.share_id == share.id)
        schemas_result = await self.db.execute(schemas_count_query)
        schemas_count = schemas_result.scalar()
        
        tables_count_query = select(func.count()).select_from(
            ShareTable.__table__.join(ShareSchema.__table__)
        ).where(ShareSchema.share_id == share.id)
        tables_result = await self.db.execute(tables_count_query)
        tables_count = tables_result.scalar()
        
        # Get recipients with access to this share
        recipients_query = select(Recipient).join(
            recipient_shares, Recipient.id == recipient_shares.c.recipient_id
        ).where(recipient_shares.c.share_id == share.id)
        recipients_result = await self.db.execute(recipients_query)
        recipients = recipients_result.scalars().all()
        
        recipients_list = [
            RecipientBasic(
                id=r.id,
                identifier=r.identifier,
                name=r.name,
                email=r.email,
                is_active=r.is_active
            ) for r in recipients
        ]
        
        return ShareDetail(
            id=share.id,
            name=share.name,
            description=share.description,
            organization_id=share.organization_id,
            status=share.status,
            owner_email=share.owner_email,
            contact_info=share.contact_info,
            terms_of_use=share.terms_of_use,
            data_criacao=share.data_criacao,
            data_atualizacao=share.data_atualizacao,
            schemas_count=schemas_count,
            tables_count=tables_count,
            recipients_count=len(recipients_list),
            recipients=recipients_list
        )

class SchemaService:
    """Service for managing Delta Sharing schemas"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_schema(self, share_id: int, schema_data: SchemaCreate, organization_id: int) -> SchemaDetail:
        """Create a new schema in a share"""
        # Verify share exists and belongs to organization
        share_query = select(Share).where(
            and_(Share.id == share_id, Share.organization_id == organization_id)
        )
        share_result = await self.db.execute(share_query)
        db_share = share_result.scalar_one_or_none()
        
        if not db_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Share not found"
            )
        
        # Check if schema name already exists in this share
        existing = await self.db.execute(
            select(ShareSchema).where(
                and_(ShareSchema.share_id == share_id, ShareSchema.name == schema_data.name)
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Schema with name '{schema_data.name}' already exists in this share"
            )
        
        # Create new schema
        db_schema = ShareSchema(
            share_id=share_id,
            name=schema_data.name,
            description=schema_data.description
        )
        
        self.db.add(db_schema)
        await self.db.commit()
        await self.db.refresh(db_schema)
        
        return await self._build_schema_detail(db_schema, db_share)
    
    async def get_schema(self, share_id: int, schema_id: int, organization_id: int) -> SchemaDetail:
        """Get schema by ID"""
        query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        result = await self.db.execute(query)
        schema_share = result.first()
        
        if not schema_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema not found"
            )
        
        db_schema, db_share = schema_share
        return await self._build_schema_detail(db_schema, db_share)
    
    async def get_schema_by_name(self, share_name: str, schema_name: str, organization_id: int) -> SchemaDetail:
        """Get schema by name"""
        query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.name == schema_name,
                Share.name == share_name,
                Share.organization_id == organization_id
            )
        )
        result = await self.db.execute(query)
        schema_share = result.first()
        
        if not schema_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema not found"
            )
        
        db_schema, db_share = schema_share
        return await self._build_schema_detail(db_schema, db_share)
    
    async def update_schema(self, share_id: int, schema_id: int, schema_data: SchemaUpdate, organization_id: int) -> SchemaDetail:
        """Update an existing schema"""
        query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        result = await self.db.execute(query)
        schema_share = result.first()
        
        if not schema_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema not found"
            )
        
        db_schema, db_share = schema_share
        
        # Update fields
        update_data = schema_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_schema, field, value)
        
        await self.db.commit()
        await self.db.refresh(db_schema)
        
        return await self._build_schema_detail(db_schema, db_share)
    
    async def delete_schema(self, share_id: int, schema_id: int, organization_id: int) -> None:
        """Delete a schema"""
        query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        result = await self.db.execute(query)
        schema_share = result.first()
        
        if not schema_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema not found"
            )
        
        db_schema, _ = schema_share
        await self.db.delete(db_schema)
        await self.db.commit()
    
    async def list_schemas(self, share_id: int, search_params: SearchSchemas, organization_id: int) -> SearchResult[SchemaDetail]:
        """List schemas in a share with pagination and filtering"""
        # Verify share exists
        share_query = select(Share).where(
            and_(Share.id == share_id, Share.organization_id == organization_id)
        )
        share_result = await self.db.execute(share_query)
        db_share = share_result.scalar_one_or_none()
        
        if not db_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Share not found"
            )
        
        query = select(ShareSchema).where(ShareSchema.share_id == share_id)
        
        # Apply filters
        if search_params.search:
            search_term = f"%{search_params.search}%"
            query = query.where(
                or_(
                    ShareSchema.name.ilike(search_term),
                    ShareSchema.description.ilike(search_term)
                )
            )
        
        # Count total
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.db.execute(count_query)
        total = total_result.scalar()
        
        # Apply pagination
        query = query.order_by(desc(ShareSchema.data_criacao))
        query = query.offset((search_params.page - 1) * search_params.size)
        query = query.limit(search_params.size)
        
        result = await self.db.execute(query)
        schemas = result.scalars().all()
        
        # Build response items
        items = []
        for schema in schemas:
            items.append(await self._build_schema_detail(schema, db_share))
        
        return SearchResult(
            items=items,
            total=total,
            page=search_params.page,
            size=search_params.size,
            pages=((total - 1) // search_params.size) + 1 if total > 0 else 0
        )
    
    async def _build_schema_detail(self, schema: ShareSchema, share: Share) -> SchemaDetail:
        """Build detailed schema information"""
        # Count tables
        tables_count_query = select(func.count()).where(ShareTable.schema_id == schema.id)
        tables_result = await self.db.execute(tables_count_query)
        tables_count = tables_result.scalar()
        
        return SchemaDetail(
            id=schema.id,
            name=schema.name,
            description=schema.description,
            share_id=schema.share_id,
            share_name=share.name,
            data_criacao=schema.data_criacao,
            data_atualizacao=schema.data_atualizacao,
            tables_count=tables_count
        )
