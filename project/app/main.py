from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from app.api.api import api_router
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import sqlalchemy.exc

# Configuração do thread pool para asyncio.to_thread()
# Usado principalmente para operações MinIO (sync) - Trino agora usa aiotrino (async nativo)
THREAD_POOL_SIZE = int(os.getenv("THREAD_POOL_SIZE", "20"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure thread pool for synchronous operations (MinIO, Spark, etc.)
    executor = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)
    loop = asyncio.get_event_loop()
    loop.set_default_executor(executor)
    print(f"Thread pool configured with {THREAD_POOL_SIZE} workers")

    # Start the Spark job queue (limits concurrent Spark jobs to SPARK_POOL_SIZE)
    from app.services.infrastructure.spark_job_queue import get_spark_job_queue
    spark_queue = get_spark_job_queue()
    await spark_queue.start()
    print(
        f"SparkJobQueue started: pool_size={spark_queue.pool_size}"
    )

    yield

    # Graceful shutdown: stop queue workers
    await spark_queue.stop()
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

@app.exception_handler(sqlalchemy.exc.TimeoutError)
async def db_pool_timeout_handler(request: Request, exc: sqlalchemy.exc.TimeoutError):
    """Connection pool exhausted — return 503 so clients can retry.

    Without this handler a QueuePool timeout surfaces as an unhandled
    exception and Uvicorn returns 500.  503 is semantically correct
    (the server is temporarily unable to handle the request) and signals
    load balancers / clients to back off and retry.
    """
    return JSONResponse(
        status_code=503,
        headers={"Retry-After": "5"},
        content={"detail": "Database connection pool exhausted. Please retry in a few seconds."},
    )


@app.exception_handler(sqlalchemy.exc.OperationalError)
async def db_operational_error_handler(request: Request, exc: sqlalchemy.exc.OperationalError):
    """Catch transient DB connectivity errors (e.g. postgres restart, network blip)
    and return 503 instead of 500."""
    return JSONResponse(
        status_code=503,
        headers={"Retry-After": "5"},
        content={"detail": "Database temporarily unavailable. Please retry in a few seconds."},
    )


@app.get("/")
async def healthcheck():
    return {"status": "healthy", "message": "Data Fabric Backend Running"}

app.include_router(api_router, prefix="/api")



