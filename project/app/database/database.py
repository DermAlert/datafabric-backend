import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from typing import AsyncGenerator
from sqlalchemy.schema import CreateSchema

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

Base = declarative_base()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session   

async def create_schemas():
    async with engine.begin() as conn:
        try:
            await conn.execute(CreateSchema("core", if_not_exists=True))
        except Exception as e:
            print(f"Error creating core schema: {e}")
        
        try:
            await conn.execute(CreateSchema("workflow", if_not_exists=True))
        except Exception as e:
            print(f"Error creating workflow schema: {e}")