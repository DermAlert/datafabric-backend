from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from app.api.api import api_router

# from app.database.populate_db import populate_db
from app.database import database
from app.database import core
from app.database import workflow


@asynccontextmanager
async def lifespan(app: FastAPI):

    await database.create_schemas()

    print("Database schemas created successfully")

    async with database.engine.begin() as conn:
        await conn.run_sync(core.Base.metadata.create_all)
        await conn.run_sync(workflow.Base.metadata.create_all)

    print("Database tables created successfully")

    yield
    print("Application is shutting down")

# app = FastAPI(lifespan=lifespan)
app = FastAPI(
    lifespan=lifespan,
    title="Data Fabric Backend",
    version="0.1.0"
)

origins = [
    "http://localhost:8081",   
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def healthcheck():
    return {"message": "Project Running Successfully"}

app.include_router(api_router, prefix="/api")



