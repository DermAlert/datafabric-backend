"""Database Session Management
==============================

Connection Pool Strategy
------------------------
With asyncpg + asyncio, one SQLAlchemy "connection" = one real PostgreSQL
connection.  The pool_size should reflect MAX CONCURRENT DB OPERATIONS in
flight, not the number of HTTP clients, because:

  • Most handlers complete in < 5 ms and return their connection immediately.
  • Heavy handlers (Spark, Trino, metadata extraction) MUST release the
    connection before the long I/O and reacquire it only to persist results.

3-Phase Pattern for heavy endpoints
------------------------------------
    # Phase 1 — read config; connection is returned to pool on context exit
    async with db_session() as db:
        config = await db.get(Config, config_id)

    # Phase 2 — Spark / Trino (can take minutes): NO connection held
    result = await run_spark_job(config)

    # Phase 3 — persist result; fresh connection acquired automatically
    async with db_session() as db:
        db.add(result_record)
        await db.commit()

For services that receive a session via Depends(get_db) and need to release
mid-execution, call `await release_connection(db)` before the long I/O.
The session stays alive and lazily reacquires a connection on the next query.
"""

import os
import contextlib
import logging
from typing import AsyncGenerator

import sqlalchemy.exc
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import CreateSchema

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

# echo=True logs every SQL query — useful for debugging but a security and
# performance risk in production.  Set SQL_ECHO=true in .env only when
# actively debugging queries.
_sql_echo = os.getenv("SQL_ECHO", "false").lower() == "true"

engine = create_async_engine(
    DATABASE_URL,
    echo=_sql_echo,
    # ── Pool sizing ──────────────────────────────────────────────────────────
    # pool_size: warm (persistent) connections.  Keep low; asyncpg is cheap to
    #   create and fast CRUD handlers release connections in < 5 ms.
    # max_overflow: burst connections created on demand, destroyed on return.
    # pool_timeout: seconds a coroutine waits before raising QueuePool error.
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DB_POOL_MAX_OVERFLOW", "20")),
    pool_timeout=float(os.getenv("DB_POOL_TIMEOUT", "30")),
    # ── Connection health ────────────────────────────────────────────────────
    # pool_pre_ping: sends a lightweight SELECT 1 before lending a connection;
    #   detects stale TCP connections after PostgreSQL idle timeouts or restarts.
    pool_pre_ping=True,
    # Recycle connections older than 30 min to avoid "server closed connection
    # unexpectedly" errors after PostgreSQL's TCP keepalive or idle timeout.
    pool_recycle=1800,
    # ── Reuse strategy ───────────────────────────────────────────────────────
    # pool_use_lifo=True: LIFO (Last-In-First-Out) checkout order.
    #   Reuses the most recently returned connection first, keeping a small set
    #   of connections "hot" and allowing the rest to age out naturally.
    #   Critical for asyncio workloads: reduces the number of simultaneously
    #   active PG connections under burst traffic.
    pool_use_lifo=True,
    # ── asyncpg driver settings ──────────────────────────────────────────────
    connect_args={
        # Connect timeout: how long asyncpg waits for the TCP handshake.
        "timeout": 15,
        # Per-statement timeout (seconds): prevents a single runaway query from
        # occupying a connection indefinitely.
        # Fast CRUD endpoints complete in < 1s.
        # Heavier metadata/report queries can take up to ~30s under load.
        # Spark/Trino calls are NOT routed through this pool (they use their
        # own connections), so this timeout only covers PostgreSQL queries.
        # Override per-query with: await session.execute(stmt, execution_options={"timeout": N})
        "command_timeout": 120,
        "server_settings": {
            # Disable JIT compilation in PostgreSQL — reduces latency variance
            # for the short OLTP queries this service issues.
            "jit": "off",
            "application_name": "datafabric-backend",
        },
    },
)

# async_sessionmaker is the SQLAlchemy 2.0-style factory for AsyncSession.
# Using it (instead of the legacy sessionmaker) avoids a deprecation warning
# and enables 2.0-specific lifecycle improvements.
SessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    # expire_on_commit=False: loaded ORM attributes remain accessible after
    # commit without issuing a new SELECT.  Essential for async patterns where
    # lazy loading would hit a closed event loop.
    expire_on_commit=False,
    # autoflush=False: we control exactly when SQL is flushed to PG.
    autoflush=False,
)

Base = declarative_base()


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI dependency — one session per HTTP request
# ─────────────────────────────────────────────────────────────────────────────

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency — one session per HTTP request.

    Suitable ONLY for fast CRUD endpoints whose entire handler completes in
    milliseconds (no Spark, no Trino, no external long-running I/O).

    For heavy endpoints, use `db_session()` directly or call
    `release_connection(db)` before the slow I/O to avoid holding a pooled
    connection for the duration of a Spark or Trino job.

    On normal exit the transaction is committed automatically.
    On exception the transaction is rolled back before the connection is
    returned to the pool, ensuring no dirty state leaks back.
    """
    async with SessionLocal() as session:
        # Eagerly acquire a connection from the pool BEFORE yielding.
        #
        # Without this, SQLAlchemy acquires the connection lazily on the first
        # SQL statement — which happens inside the route handler's try/except
        # block.  A TimeoutError there is swallowed by the generic
        # `except Exception as e: raise HTTPException(500)` pattern present in
        # most route handlers, resulting in a confusing 500 response.
        #
        # By acquiring here (in the dependency, before yield), the TimeoutError
        # is raised in our own try/except, which converts it to a proper 503
        # before the route handler ever runs.
        try:
            await session.connection()
        except sqlalchemy.exc.TimeoutError:
            raise HTTPException(
                status_code=503,
                headers={"Retry-After": "5"},
                detail="Database connection pool exhausted. Please retry in a few seconds.",
            )

        try:
            yield session
            await session.commit()
        except sqlalchemy.exc.TimeoutError:
            # Guard for re-acquisition after release_connection() + new SQL
            raise HTTPException(
                status_code=503,
                headers={"Retry-After": "5"},
                detail="Database connection pool exhausted. Please retry in a few seconds.",
            )
        except Exception:
            await session.rollback()
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Explicit session context manager — for services with long-running I/O
# ─────────────────────────────────────────────────────────────────────────────

@contextlib.asynccontextmanager
async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """Explicit session context manager for fine-grained connection control.

    The connection is returned to the pool as soon as this context exits.
    Use separate `async with db_session()` blocks for each DB phase to avoid
    holding connections across Spark / Trino waits.

    Typical 3-phase pattern:

        # Phase 1 — read config; connection returned to pool on exit
        async with db_session() as db:
            config = await db.get(Config, config_id)

        # Phase 2 — long I/O: NO connection held
        result = await run_spark_job(config)

        # Phase 3 — persist result; fresh connection acquired
        async with db_session() as db:
            db.add(result_record)
            await db.commit()
    """
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except sqlalchemy.exc.TimeoutError:
            raise HTTPException(
                status_code=503,
                headers={"Retry-After": "5"},
                detail="Database connection pool exhausted. Please retry in a few seconds.",
            )
        except Exception:
            await session.rollback()
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Mid-execution connection release helper
# ─────────────────────────────────────────────────────────────────────────────

def reraise_db_timeout(exc: Exception) -> None:
    """Call at the top of 'except Exception' blocks in route handlers.

    If *exc* is a SQLAlchemy QueuePool timeout, raises HTTPException(503)
    with a Retry-After header so the client can back off gracefully.
    Without this, the generic ``except Exception`` block would log the error
    and re-raise it as HTTPException(500), hiding the real cause.

    Usage::

        except HTTPException:
            raise
        except Exception as e:
            reraise_db_timeout(e)        # ← converts TimeoutError → 503
            await db.rollback()
            logger.error(...)
            raise HTTPException(500, ...)
    """
    if isinstance(exc, sqlalchemy.exc.TimeoutError):
        raise HTTPException(
            status_code=503,
            headers={"Retry-After": "5"},
            detail="Database connection pool exhausted. Please retry in a few seconds.",
        )


async def release_connection(session: AsyncSession) -> None:
    """Return the database connection to the pool immediately.

    The session object remains valid.  SQLAlchemy will transparently
    reacquire a connection from the pool on the next SQL operation.

    Use this in route handlers or services that receive a session via
    `Depends(get_db)` but need to perform long I/O (Spark, Trino) before
    writing results back to the database.

    Example — Bronze / Silver execute endpoint pattern:

        async def execute_endpoint(db: AsyncSession = Depends(get_db)):
            # Phase 1: read config
            config = await db.execute(select(Config).where(...))

            # Commit RUNNING state and release the connection to the pool.
            # Other requests can use this connection while Spark runs.
            await db.commit()
            await release_connection(db)

            # Phase 2: Spark / Trino — no connection held
            result = await service.run_spark(config)

            # Phase 3: persist — session auto-acquires a fresh connection
            db.add(result)
            await db.commit()

    SQLAlchemy 2.0 behaviour: `session.close()` expires all identity-map
    entries and returns the underlying connection to the pool.  Because
    `expire_on_commit=False` the Python objects already loaded remain valid
    in memory; they are simply no longer tracked by this session instance.
    """
    await session.close()


# ─────────────────────────────────────────────────────────────────────────────
# Schema creation
# ─────────────────────────────────────────────────────────────────────────────

async def create_schemas() -> None:
    schemas = [
        "core", "equivalence", "metadata", "storage",
        "workflow", "delta_sharing", "datasets",
    ]
    async with engine.begin() as conn:
        for schema in schemas:
            try:
                await conn.execute(CreateSchema(schema, if_not_exists=True))
            except Exception as exc:
                log.warning("Could not create schema %r: %s", schema, exc)
