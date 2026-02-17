from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from app.api.api import api_router
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os

# Configuração do thread pool para asyncio.to_thread()
# Usado principalmente para operações MinIO (sync) - Trino agora usa aiotrino (async nativo)
THREAD_POOL_SIZE = int(os.getenv("THREAD_POOL_SIZE", "20"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configurar thread pool para operações síncronas (MinIO, etc.)
    # Trino agora usa aiotrino (async nativo) - não precisa de threads
    executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    print(f"Thread pool configured with {THREAD_POOL_SIZE} workers (for sync ops like MinIO)")

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
async def healthcheck():
    return {"status": "healthy", "message": "Data Fabric Backend Running"}

app.include_router(api_router, prefix="/api")



