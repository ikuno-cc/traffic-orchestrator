"""
Asyncio-based per-service job queue engine.
Replaces Celery + SQLAlchemy broker entirely.

Each service gets:
  - an asyncio.Queue  (FIFO, in-process, no broker needed)
  - N worker coroutines (N = service.worker_count)

Workers are lightweight coroutines.  Blocking HTTP calls inside the
dispatcher are offloaded to a thread pool via asyncio.to_thread().
This keeps the event loop responsive regardless of service latency.

NOTE: State is in-process only.  A restart will lose in-flight tasks
that are still sitting in the asyncio.Queue (tasks already persisted
to Postgres as 'queued' will be re-enqueued on the next dispatch call
or can be retried via the API).  If you need durable queues, swap this
engine for Redis Streams / RabbitMQ later without touching the rest of
the app.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

JobHandler = Callable[[dict[str, Any]], Awaitable[None]]


# ---------------------------------------------------------------------------
# Per-service worker pool
# ---------------------------------------------------------------------------

class ServiceWorkerPool:
    """A fixed pool of coroutineWorkers draining one asyncio.Queue."""

    def __init__(self, service_id: str, worker_count: int, handler: JobHandler):
        self.service_id = service_id
        self.queue: asyncio.Queue[dict] = asyncio.Queue()
        self._handler = handler
        self._tasks: List[asyncio.Task] = []
        self._worker_count = worker_count

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        self._tasks = [
            asyncio.create_task(
                self._worker_loop(),
                name=f"worker-{self.service_id}-{i}",
            )
            for i in range(max(1, self._worker_count))
        ]
        logger.info(
            "ServiceWorkerPool started: service=%s workers=%d",
            self.service_id,
            self._worker_count,
        )

    def stop(self):
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    # ------------------------------------------------------------------
    # Resize (called when worker_count changes at runtime)
    # ------------------------------------------------------------------

    def resize(self, new_count: int):
        new_count = max(1, new_count)
        diff = new_count - self._worker_count
        self._worker_count = new_count

        if diff > 0:
            for i in range(diff):
                t = asyncio.create_task(
                    self._worker_loop(),
                    name=f"worker-{self.service_id}-resize-{i}",
                )
                self._tasks.append(t)
            logger.info("Scaled UP service %s to %d workers (+%d)", self.service_id, new_count, diff)
        elif diff < 0:
            # Cancel the surplus tasks; they finish their current job first
            # because CancelledError is only raised at the next await point.
            for _ in range(-diff):
                if self._tasks:
                    self._tasks.pop().cancel()
            logger.info("Scaled DOWN service %s to %d workers (%d)", self.service_id, new_count, diff)

    # ------------------------------------------------------------------
    # Enqueueing
    # ------------------------------------------------------------------

    async def enqueue(self, record: dict, delay: float = 0.0):
        if delay > 0:
            asyncio.create_task(self._delayed_put(record, delay))
        else:
            await self.queue.put(record)

    async def _delayed_put(self, record: dict, delay: float):
        await asyncio.sleep(delay)
        await self.queue.put(record)

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker_loop(self):
        svc = self.service_id
        while True:
            try:
                record = await self.queue.get()
                try:
                    await self._handler(record)
                except Exception as exc:
                    logger.exception("Unhandled error in worker for service %s: %s", svc, exc)
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                logger.debug("Worker cancelled for service %s", svc)
                break

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def depth(self) -> int:
        return self.queue.qsize()

    @property
    def worker_count(self) -> int:
        return self._worker_count

    @property
    def active_workers(self) -> int:
        return sum(1 for t in self._tasks if not t.done())


# ---------------------------------------------------------------------------
# Engine (singleton)
# ---------------------------------------------------------------------------

class QueueEngine:
    """
    Manages all per-service worker pools.  This is the single replacement
    for the entire Celery infrastructure.
    """

    def __init__(self):
        self._pools: Dict[str, ServiceWorkerPool] = {}
        self._handler: Optional[JobHandler] = None
        self._running = False

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def set_handler(self, handler: JobHandler) -> None:
        """Register the async function that executes each job."""
        self._handler = handler

    def start(self) -> None:
        self._running = True

    def stop_all(self) -> None:
        self._running = False
        for pool in self._pools.values():
            pool.stop()
        self._pools.clear()

    # ------------------------------------------------------------------
    # Pool management
    # ------------------------------------------------------------------

    def get_or_create_pool(self, service_id: str, worker_count: int = 1) -> ServiceWorkerPool:
        if service_id not in self._pools:
            assert self._handler is not None, "Call set_handler() before enqueueing."
            pool = ServiceWorkerPool(service_id, worker_count, self._handler)
            pool.start()
            self._pools[service_id] = pool
        return self._pools[service_id]

    def set_concurrency(self, service_id: str, new_count: int) -> None:
        """Resize an existing pool's worker count at runtime."""
        pool = self._pools.get(service_id)
        if pool:
            pool.resize(new_count)
        # If the pool doesn't exist yet it will be created with the correct
        # count the next time a job is dispatched for this service.

    def remove_pool(self, service_id: str) -> None:
        pool = self._pools.pop(service_id, None)
        if pool:
            pool.stop()

    # ------------------------------------------------------------------
    # Dispatching
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        service_id: str,
        record: dict,
        worker_count: int = 1,
        delay: float = 0.0,
    ) -> None:
        pool = self.get_or_create_pool(service_id, worker_count)
        await pool.enqueue(record, delay=delay)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def queue_depth(self, service_id: str) -> int:
        pool = self._pools.get(service_id)
        return pool.depth if pool else 0

    def stats(self) -> dict[str, Any]:
        return {
            sid: {
                "queue_depth": p.depth,
                "workers": p.worker_count,
                "active_workers": p.active_workers,
            }
            for sid, p in self._pools.items()
        }


# Module-level singleton — imported by main.py and dispatcher.py
engine = QueueEngine()
