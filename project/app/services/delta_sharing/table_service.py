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
from ...database.storage.storage import DatasetStorage
from ...api.schemas.delta_sharing_schemas import (
    ShareTableCreate, ShareTableUpdate, ShareTableDetail,
    RecipientCreate, RecipientUpdate, RecipientDetail,
    SearchTables, SearchRecipients
)
from ...api.schemas.search import SearchResult

class TableService:
    """Service for managing Delta Sharing tables"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_table(self, share_id: int, schema_id: int, table_data: ShareTableCreate, organization_id: int) -> ShareTableDetail:
        """Create a new shared table in a schema"""
        # Verify schema exists and belongs to the right share and organization
        schema_query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        schema_result = await self.db.execute(schema_query)
        schema_share = schema_result.first()
        
        if not schema_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema not found"
            )
        
        db_schema, db_share = schema_share
        
        # Verify dataset exists and belongs to organization
        dataset_query = select(Dataset).where(Dataset.id == table_data.dataset_id)
        dataset_result = await self.db.execute(dataset_query)
        db_dataset = dataset_result.scalar_one_or_none()
        
        if not db_dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Dataset not found"
            )
        
        # Get storage location from dataset_storage table
        storage_location = None
        storage_query = select(DatasetStorage).where(DatasetStorage.dataset_id == table_data.dataset_id)
        storage_result = await self.db.execute(storage_query)
        db_storage = storage_result.scalar_one_or_none()
        
        if db_storage:
            # Use the storage location from dataset_storage, append /data/unified_data for Delta Lake
            base_storage = db_storage.storage_location
            # Remove any protocol prefix (s3://, s3a://, minio://) for internal storage
            if base_storage.startswith('s3a://'):
                base_storage = base_storage[6:]
            elif base_storage.startswith('s3://'):
                base_storage = base_storage[5:]
            elif base_storage.startswith('minio://'):
                base_storage = base_storage[8:]
            
            # Add the standard Delta Lake path
            storage_location = f"{base_storage}/data/unified_data"
        
        # Check if table name already exists in this schema
        existing = await self.db.execute(
            select(ShareTable).where(
                and_(ShareTable.schema_id == schema_id, ShareTable.name == table_data.name)
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table with name '{table_data.name}' already exists in this schema"
            )
        
        # Create new shared table with storage location
        db_table = ShareTable(
            schema_id=schema_id,
            name=table_data.name,
            description=table_data.description,
            dataset_id=table_data.dataset_id,
            storage_location=storage_location,
            status=TableShareStatus.ACTIVE,
            share_mode=table_data.share_mode,
            filter_condition=table_data.filter_condition,
            current_version=1,
            table_format="parquet"
        )
        
        self.db.add(db_table)
        await self.db.commit()
        await self.db.refresh(db_table)
        
        # Create initial version
        await self._create_table_version(db_table, "CREATE", "Initial table creation")
        
        return await self._build_table_detail(db_table, db_schema, db_share, db_dataset)
    
    async def get_table(self, share_id: int, schema_id: int, table_id: int, organization_id: int) -> ShareTableDetail:
        """Get table by ID"""
        query = select(ShareTable, ShareSchema, Share, Dataset).join(
            ShareSchema
        ).join(Share).join(Dataset).where(
            and_(
                ShareTable.id == table_id,
                ShareTable.schema_id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        result = await self.db.execute(query)
        table_data = result.first()
        
        if not table_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Table not found"
            )
        
        db_table, db_schema, db_share, db_dataset = table_data
        return await self._build_table_detail(db_table, db_schema, db_share, db_dataset)
    
    async def get_table_by_name(self, share_name: str, schema_name: str, table_name: str, organization_id: int) -> ShareTableDetail:
        """Get table by name"""
        query = select(ShareTable, ShareSchema, Share, Dataset).join(
            ShareSchema
        ).join(Share).join(Dataset).where(
            and_(
                ShareTable.name == table_name,
                ShareSchema.name == schema_name,
                Share.name == share_name,
                Share.organization_id == organization_id
            )
        )
        result = await self.db.execute(query)
        table_data = result.first()
        
        if not table_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Table not found"
            )
        
        db_table, db_schema, db_share, db_dataset = table_data
        return await self._build_table_detail(db_table, db_schema, db_share, db_dataset)
    
    async def update_table(self, share_id: int, schema_id: int, table_id: int, table_data: ShareTableUpdate, organization_id: int) -> ShareTableDetail:
        """Update an existing shared table"""
        query = select(ShareTable, ShareSchema, Share, Dataset).join(
            ShareSchema
        ).join(Share).join(Dataset).where(
            and_(
                ShareTable.id == table_id,
                ShareTable.schema_id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        result = await self.db.execute(query)
        table_result = result.first()
        
        if not table_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Table not found"
            )
        
        db_table, db_schema, db_share, db_dataset = table_result
        
        # Update fields
        update_data = table_data.dict(exclude_unset=True)
        version_updated = False
        dataset_id_changed = False
        
        for field, value in update_data.items():
            old_value = getattr(db_table, field)
            if old_value != value:
                setattr(db_table, field, value)
                if field in ['filter_condition', 'share_mode']:
                    version_updated = True
                if field == 'dataset_id':
                    dataset_id_changed = True
        
        # If dataset_id changed, update storage_location
        if dataset_id_changed:
            storage_query = select(DatasetStorage).where(DatasetStorage.dataset_id == db_table.dataset_id)
            storage_result = await self.db.execute(storage_query)
            db_storage = storage_result.scalar_one_or_none()
            
            if db_storage:
                # Use the storage location from dataset_storage, append /data/unified_data for Delta Lake
                base_storage = db_storage.storage_location
                # Remove any protocol prefix (s3://, s3a://, minio://) for internal storage
                if base_storage.startswith('s3a://'):
                    base_storage = base_storage[6:]
                elif base_storage.startswith('s3://'):
                    base_storage = base_storage[5:]
                elif base_storage.startswith('minio://'):
                    base_storage = base_storage[8:]
                
                # Add the standard Delta Lake path
                db_table.storage_location = f"{base_storage}/data/unified_data"
                version_updated = True
        
        # If configuration changed, increment version
        if version_updated:
            db_table.current_version += 1
            await self._create_table_version(db_table, "UPDATE", "Table configuration updated")
        
        await self.db.commit()
        await self.db.refresh(db_table)
        
        return await self._build_table_detail(db_table, db_schema, db_share, db_dataset)
    
    async def delete_table(self, share_id: int, schema_id: int, table_id: int, organization_id: int) -> None:
        """Delete a shared table"""
        query = select(ShareTable, ShareSchema, Share).join(
            ShareSchema
        ).join(Share).where(
            and_(
                ShareTable.id == table_id,
                ShareTable.schema_id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        result = await self.db.execute(query)
        table_result = result.first()
        
        if not table_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Table not found"
            )
        
        db_table, _, _ = table_result
        await self.db.delete(db_table)
        await self.db.commit()
    
    async def list_tables(self, share_id: int, schema_id: int, search_params: SearchTables, organization_id: int) -> SearchResult[ShareTableDetail]:
        """List tables in a schema with pagination and filtering"""
        # Verify schema exists
        schema_query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        schema_result = await self.db.execute(schema_query)
        schema_share = schema_result.first()
        
        if not schema_share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema not found"
            )
        
        db_schema, db_share = schema_share
        
        query = select(ShareTable, Dataset).join(Dataset).where(ShareTable.schema_id == schema_id)
        
        # Apply filters
        if search_params.search:
            search_term = f"%{search_params.search}%"
            query = query.where(
                or_(
                    ShareTable.name.ilike(search_term),
                    ShareTable.description.ilike(search_term),
                    Dataset.name.ilike(search_term)
                )
            )
        
        if search_params.status:
            query = query.where(ShareTable.status == search_params.status)
        
        # Count total
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.db.execute(count_query)
        total = total_result.scalar()
        
        # Apply pagination
        query = query.order_by(desc(ShareTable.data_criacao))
        query = query.offset((search_params.page - 1) * search_params.size)
        query = query.limit(search_params.size)
        
        result = await self.db.execute(query)
        tables_data = result.all()
        
        # Build response items
        items = []
        for db_table, db_dataset in tables_data:
            items.append(await self._build_table_detail(db_table, db_schema, db_share, db_dataset))
        
        return SearchResult(
            items=items,
            total=total,
            page=search_params.page,
            size=search_params.size,
            pages=((total - 1) // search_params.size) + 1 if total > 0 else 0
        )
    
    async def _create_table_version(self, table: ShareTable, operation: str, description: str) -> None:
        """Create a new version record for a table"""
        version = ShareTableVersion(
            table_id=table.id,
            version=table.current_version,
            operation=operation,
            description=description,
            protocol_version={"minReaderVersion": 1},
            metadata={
                "id": str(table.id),
                "format": {"provider": table.table_format},
                "schemaString": table.schema_string or "{}",
                "partitionColumns": table.partition_columns or []
            }
        )
        
        self.db.add(version)
    
    async def _build_table_detail(self, table: ShareTable, schema: ShareSchema, share: Share, dataset: Dataset) -> ShareTableDetail:
        """Build detailed table information"""
        return ShareTableDetail(
            id=table.id,
            name=table.name,
            description=table.description,
            schema_id=table.schema_id,
            schema_name=schema.name,
            share_id=schema.share_id,
            share_name=share.name,
            dataset_id=table.dataset_id,
            dataset_name=dataset.name,
            status=table.status,
            share_mode=table.share_mode,
            filter_condition=table.filter_condition,
            current_version=table.current_version,
            table_format=table.table_format,
            partition_columns=table.partition_columns,
            storage_location=table.storage_location,
            data_criacao=table.data_criacao,
            data_atualizacao=table.data_atualizacao
        )

class RecipientService:
    """Service for managing Delta Sharing recipients"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def create_recipient(self, recipient_data: RecipientCreate, organization_id: int) -> RecipientDetail:
        """Create a new recipient"""
        # Check if identifier already exists
        existing = await self.db.execute(
            select(Recipient).where(Recipient.identifier == recipient_data.identifier)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Recipient with identifier '{recipient_data.identifier}' already exists"
            )
        
        # Generate bearer token
        bearer_token = self._generate_bearer_token()
        
        # Create new recipient
        db_recipient = Recipient(
            identifier=recipient_data.identifier,
            name=recipient_data.name,
            email=recipient_data.email,
            organization_name=recipient_data.organization_name,
            authentication_type=recipient_data.authentication_type,
            bearer_token=bearer_token,
            token_expiry=datetime.utcnow() + timedelta(days=365),  # 1 year expiry
            is_active=True,
            max_requests_per_hour=recipient_data.max_requests_per_hour,
            max_downloads_per_day=recipient_data.max_downloads_per_day,
            contact_info=recipient_data.contact_info,
            notes=recipient_data.notes
        )
        
        self.db.add(db_recipient)
        await self.db.commit()
        await self.db.refresh(db_recipient)
        
        return await self._build_recipient_detail(db_recipient)
    
    async def get_recipient(self, recipient_id: int) -> RecipientDetail:
        """Get recipient by ID"""
        query = select(Recipient).where(Recipient.id == recipient_id)
        result = await self.db.execute(query)
        db_recipient = result.scalar_one_or_none()
        
        if not db_recipient:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Recipient not found"
            )
        
        return await self._build_recipient_detail(db_recipient)
    
    async def get_recipient_by_identifier(self, identifier: str) -> RecipientDetail:
        """Get recipient by identifier"""
        query = select(Recipient).where(Recipient.identifier == identifier)
        result = await self.db.execute(query)
        db_recipient = result.scalar_one_or_none()
        
        if not db_recipient:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Recipient not found"
            )
        
        return await self._build_recipient_detail(db_recipient)
    
    async def update_recipient(self, recipient_id: int, recipient_data: RecipientUpdate) -> RecipientDetail:
        """Update an existing recipient"""
        query = select(Recipient).where(Recipient.id == recipient_id)
        result = await self.db.execute(query)
        db_recipient = result.scalar_one_or_none()
        
        if not db_recipient:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Recipient not found"
            )
        
        # Update fields
        update_data = recipient_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_recipient, field, value)
        
        await self.db.commit()
        await self.db.refresh(db_recipient)
        
        return await self._build_recipient_detail(db_recipient)
    
    async def delete_recipient(self, recipient_id: int) -> None:
        """Delete a recipient"""
        query = select(Recipient).where(Recipient.id == recipient_id)
        result = await self.db.execute(query)
        db_recipient = result.scalar_one_or_none()
        
        if not db_recipient:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Recipient not found"
            )
        
        await self.db.delete(db_recipient)
        await self.db.commit()
    
    async def list_recipients(self, search_params: SearchRecipients) -> SearchResult[RecipientDetail]:
        """List recipients with pagination and filtering"""
        query = select(Recipient)
        
        # Apply filters
        if search_params.search:
            search_term = f"%{search_params.search}%"
            query = query.where(
                or_(
                    Recipient.identifier.ilike(search_term),
                    Recipient.name.ilike(search_term),
                    Recipient.email.ilike(search_term),
                    Recipient.organization_name.ilike(search_term)
                )
            )
        
        if search_params.is_active is not None:
            query = query.where(Recipient.is_active == search_params.is_active)
        
        # Count total
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.db.execute(count_query)
        total = total_result.scalar()
        
        # Apply pagination
        query = query.order_by(desc(Recipient.data_criacao))
        query = query.offset((search_params.page - 1) * search_params.size)
        query = query.limit(search_params.size)
        
        result = await self.db.execute(query)
        recipients = result.scalars().all()
        
        # Build response items
        items = []
        for recipient in recipients:
            items.append(await self._build_recipient_detail(recipient))
        
        return SearchResult(
            items=items,
            total=total,
            page=search_params.page,
            size=search_params.size,
            pages=((total - 1) // search_params.size) + 1 if total > 0 else 0
        )
    
    async def regenerate_token(self, recipient_id: int) -> str:
        """Regenerate bearer token for a recipient"""
        query = select(Recipient).where(Recipient.id == recipient_id)
        result = await self.db.execute(query)
        db_recipient = result.scalar_one_or_none()
        
        if not db_recipient:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Recipient not found"
            )
        
        # Generate new token
        new_token = self._generate_bearer_token()
        db_recipient.bearer_token = new_token
        db_recipient.token_expiry = datetime.utcnow() + timedelta(days=365)
        
        await self.db.commit()
        
        return new_token
    
    async def assign_shares(self, recipient_id: int, share_ids: List[int], organization_id: int) -> None:
        """Assign shares to a recipient"""
        # Verify recipient exists
        recipient_query = select(Recipient).where(Recipient.id == recipient_id)
        recipient_result = await self.db.execute(recipient_query)
        db_recipient = recipient_result.scalar_one_or_none()
        
        if not db_recipient:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Recipient not found"
            )
        
        # Verify shares exist and belong to organization
        shares_query = select(Share).where(
            and_(Share.id.in_(share_ids), Share.organization_id == organization_id)
        )
        shares_result = await self.db.execute(shares_query)
        db_shares = shares_result.scalars().all()
        
        if len(db_shares) != len(share_ids):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="One or more shares not found or not accessible"
            )
        
        # Clear existing assignments and add new ones
        # First, remove existing assignments
        delete_stmt = recipient_shares.delete().where(
            recipient_shares.c.recipient_id == recipient_id
        )
        await self.db.execute(delete_stmt)
        
        # Add new assignments
        for share in db_shares:
            insert_stmt = recipient_shares.insert().values(
                recipient_id=recipient_id,
                share_id=share.id
            )
            await self.db.execute(insert_stmt)
        
        await self.db.commit()
    
    def _generate_bearer_token(self) -> str:
        """Generate a secure bearer token"""
        return secrets.token_urlsafe(32)
    
    async def _build_recipient_detail(self, recipient: Recipient) -> RecipientDetail:
        """Build detailed recipient information"""
        # Count shares - using the association table directly
        shares_count_query = select(func.count()).select_from(
            recipient_shares
        ).where(recipient_shares.c.recipient_id == recipient.id)
        shares_result = await self.db.execute(shares_count_query)
        shares_count = shares_result.scalar()
        
        # Mask token for security (show only first 8 characters)
        masked_token = None
        if recipient.bearer_token:
            masked_token = recipient.bearer_token[:8] + "..." if len(recipient.bearer_token) > 8 else recipient.bearer_token
        
        return RecipientDetail(
            id=recipient.id,
            identifier=recipient.identifier,
            name=recipient.name,
            email=recipient.email,
            organization_name=recipient.organization_name,
            authentication_type=recipient.authentication_type,
            bearer_token=masked_token,
            token_expiry=recipient.token_expiry,
            is_active=recipient.is_active,
            max_requests_per_hour=recipient.max_requests_per_hour,
            max_downloads_per_day=recipient.max_downloads_per_day,
            access_logged=recipient.access_logged,
            data_usage_agreement_accepted=recipient.data_usage_agreement_accepted,
            agreement_accepted_at=recipient.agreement_accepted_at,
            contact_info=recipient.contact_info,
            notes=recipient.notes,
            data_criacao=recipient.data_criacao,
            data_atualizacao=recipient.data_atualizacao,
            shares_count=shares_count
        )
