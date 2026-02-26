"""
Spark Job Queue — Airflow-style pool for Bronze/Silver Spark executions.

How it works (same pattern as Airflow pools):
  1. POST /execute  → job is enqueued, HTTP returns 202 Accepted + execution_id immediately
  2. Worker coroutines (pool_size slots) pick jobs from the queue and run Spark
  3. Client polls  GET /{bronze,silver}/executions/{id} for status updates

This avoids:
  - HTTP timeouts (endpoints return in <100ms instead of 20-40s)
  - JVM crashes from too many concurrent Spark jobs
  - Py4J "Connection refused" under high concurrency

Migration path to Celery+Redis:
  - Replace SparkJobQueue with Celery tasks
  - Interface is intentionally similar (submit → execution_id → poll)
  - Redis is already in the Docker stack

Configuration (env vars):
  SPARK_POOL_SIZE        Max concurrent Spark jobs (default: 3)
  SPARK_QUEUE_MAX_SIZE   Max jobs waiting in queue, 0=unlimited (default: 0)
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_POOL_SIZE = int(os.getenv("SPARK_POOL_SIZE", "3"))
_QUEUE_MAX_SIZE = int(os.getenv("SPARK_QUEUE_MAX_SIZE", "0"))  # 0 = unlimited


# ---------------------------------------------------------------------------
# Job descriptor
# ---------------------------------------------------------------------------

class JobType(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"


@dataclass
class SparkJob:
    """A unit of work to be executed by a pool worker."""
    job_type: JobType
    config_id: int
    execution_id: int
    # Async callable that performs the actual Spark work.
    # Signature: async () -> None   (updates DB internally)
    coro_factory: Callable[[], Coroutine[Any, Any, Any]]
    submitted_at: datetime = field(default_factory=datetime.now)

    def __repr__(self) -> str:
        return (
            f"SparkJob(type={self.job_type}, config_id={self.config_id}, "
            f"execution_id={self.execution_id})"
        )


# ---------------------------------------------------------------------------
# Queue / Pool
# ---------------------------------------------------------------------------

class SparkJobQueue:
    """
    Singleton async job queue with a fixed-size worker pool.

    Workers are started as asyncio tasks when ``start()`` is called (at app
    startup) and cancelled when ``stop()`` is called (at app shutdown).
    """

    _instance: Optional["SparkJobQueue"] = None

    def __new__(cls) -> "SparkJobQueue":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._queue: asyncio.Queue[SparkJob] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)
        self._workers: list[asyncio.Task] = []
        self._active: int = 0
        self._pool_size = _POOL_SIZE
        self._started = False
        self._initialized = True

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn worker coroutines. Call once at application startup."""
        if self._started:
            return
        self._started = True
        for i in range(self._pool_size):
            task = asyncio.create_task(self._worker(i), name=f"spark-worker-{i}")
            self._workers.append(task)
        logger.info(
            f"SparkJobQueue started: {self._pool_size} worker(s), "
            f"queue max_size={'unlimited' if _QUEUE_MAX_SIZE == 0 else _QUEUE_MAX_SIZE}"
        )

    async def stop(self) -> None:
        """Cancel workers gracefully. Call at application shutdown."""
        for task in self._workers:
            task.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        self._started = False
        logger.info("SparkJobQueue stopped")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def enqueue(self, job: SparkJob) -> None:
        """
        Add a job to the queue. Returns immediately.

        If the queue is full (SPARK_QUEUE_MAX_SIZE > 0) this will block until
        a slot is available — callers should use asyncio.wait_for() with a
        timeout if that behaviour is undesirable.
        """
        await self._queue.put(job)
        logger.info(
            f"Enqueued {job} | queue_size={self._queue.qsize()} "
            f"active_workers={self._active}/{self._pool_size}"
        )

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def active_workers(self) -> int:
        return self._active

    @property
    def pool_size(self) -> int:
        return self._pool_size

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker(self, worker_id: int) -> None:
        """
        Worker coroutine: continuously picks jobs from the queue and executes
        them. One worker = one Spark job at a time → pool_size = max concurrency.
        """
        logger.debug(f"spark-worker-{worker_id} ready")
        while True:
            job: SparkJob = await self._queue.get()
            self._active += 1
            wait_ms = (datetime.now() - job.submitted_at).total_seconds() * 1000
            logger.info(
                f"[worker-{worker_id}] starting {job} "
                f"(waited {wait_ms:.0f}ms in queue)"
            )
            try:
                await job.coro_factory()
            except asyncio.CancelledError:
                logger.warning(f"[worker-{worker_id}] cancelled while running {job}")
                raise
            except Exception as exc:
                # Errors are handled inside the coro_factory (status → FAILED + DB commit).
                # We log here as a safety net and keep the worker alive.
                logger.error(f"[worker-{worker_id}] unhandled error in {job}: {exc}")
            finally:
                self._active -= 1
                self._queue.task_done()
                logger.info(
                    f"[worker-{worker_id}] finished {job} "
                    f"| queue_size={self._queue.qsize()} active={self._active}"
                )


# ---------------------------------------------------------------------------
# Module-level singleton accessor
# ---------------------------------------------------------------------------

_queue_instance: Optional[SparkJobQueue] = None


def get_spark_job_queue() -> SparkJobQueue:
    """Return the application-wide SparkJobQueue singleton."""
    global _queue_instance
    if _queue_instance is None:
        _queue_instance = SparkJobQueue()
    return _queue_instance
