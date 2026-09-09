"""
Asyncio-based per-service job queue engine.
Replaces Celery + SQLAlchemy broker entirely.

Each service gets:
  - a shared asyncio.Queue  (FIFO, in-process, no broker needed)
  - One or more WebhookSlotPools, each representing one webhook endpoint
    with its own worker capacity (max concurrent jobs for that URL).

When a job arrives:
  1. The engine checks each WebhookSlotPool in order (first-available wins).
  2. If a slot has capacity, the job is injected with that endpoint's
     webhook_url and dispatched immediately to a worker.
  3. If ALL slots are at capacity, the job is placed in the shared queue.
  4. When any slot finishes a job it pulls the next item from the shared
     queue (if any), keeping utilisation as high as possible.

Backwards compatibility:
  - If a service has no webhook_endpoints configured (legacy mode), the pool
    falls back to the original single-queue, N-worker behaviour and the
    webhook_url is left untouched in the record.

Workers are lightweight coroutines. Blocking HTTP calls inside the dispatcher
are offloaded to a thread pool via asyncio.to_thread(), keeping the event
loop responsive regardless of service latency.

NOTE: State is in-process only.  A restart will lose in-flight tasks
that are still sitting in the asyncio.Queue (tasks already persisted
to Postgres as 'queued' will be re-enqueued on the next dispatch call
or can be retried via the API).  If you need durable queues, swap this
engine for Redis Streams / RabbitMQ later without touching the rest of
the app.
"""
from __future__ import annotations

import asyncio
import copy
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

JobHandler = Callable[[dict[str, Any]], Awaitable[None]]


# ---------------------------------------------------------------------------
# Single webhook-endpoint slot pool
# ---------------------------------------------------------------------------

class WebhookSlotPool:
    """
    Manages a fixed number of concurrent job slots for ONE webhook endpoint.

    Each slot is a worker coroutine that:
      1. Acquires a semaphore permit (= one slot)
      2. Pulls the next job from the shared service queue
      3. Injects its own webhook_url into the record
      4. Calls the handler
      5. Releases the permit

    When the queue is empty the worker waits on the queue, so it never spins.
    """

    def __init__(
        self,
        webhook_url: str,
        max_workers: int,
        shared_queue: "asyncio.Queue[dict]",
        handler: JobHandler,
        service_id: str,
        slot_index: int,
    ):
        self.webhook_url = webhook_url
        self._max_workers = max(1, max_workers)
        self._queue = shared_queue
        self._handler = handler
        self._service_id = service_id
        self._slot_index = slot_index
        self._semaphore = asyncio.Semaphore(self._max_workers)
        self._tasks: List[asyncio.Task] = []
        self._active = 0  # jobs currently being processed by this slot

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(
                self._worker_loop(),
                name=f"worker-{self._service_id}-slot{self._slot_index}-{i}",
            )
            for i in range(self._max_workers)
        ]
        logger.info(
            "WebhookSlotPool started: service=%s slot=%d url=%s workers=%d",
            self._service_id, self._slot_index, self.webhook_url, self._max_workers,
        )

    def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker_loop(self) -> None:
        svc = self._service_id
        slot = self._slot_index
        while True:
            try:
                record = await self._queue.get()
                self._active += 1
                try:
                    # Inject this slot's webhook URL into the record
                    injected = copy.copy(record)
                    injected["webhook_url"] = self.webhook_url
                    await self._handler(injected)
                except Exception as exc:
                    logger.exception(
                        "Unhandled error in slot %d worker for service %s: %s",
                        slot, svc, exc,
                    )
                finally:
                    self._active -= 1
                    self._queue.task_done()
            except asyncio.CancelledError:
                logger.debug("WebhookSlotPool worker cancelled: service=%s slot=%d", svc, slot)
                break

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def active(self) -> int:
        """Number of jobs currently being processed by this slot pool."""
        return self._active

    @property
    def max_workers(self) -> int:
        return self._max_workers

    @property
    def has_capacity(self) -> bool:
        """True when this slot pool can accept at least one more job immediately."""
        return self._active < self._max_workers

    @property
    def worker_count(self) -> int:
        return self._max_workers


# ---------------------------------------------------------------------------
# Per-service worker pool  (wraps one or more WebhookSlotPools)
# ---------------------------------------------------------------------------

class ServiceWorkerPool:
    """
    A per-service pool that routes jobs across one or more WebhookSlotPools.

    In multi-webhook mode (webhook_endpoints provided):
      - Each webhook endpoint has its own WebhookSlotPool with a dedicated
        worker count.
      - All slot pools share a single asyncio.Queue.
      - Incoming jobs are placed on the shared queue directly.
      - Workers in each slot pool pull from the shared queue in a standard
        FIFO fashion; the first slot with an idle worker wins the job.

    In legacy mode (no webhook_endpoints):
      - A single slot pool is used with webhook_url left as-is from the record.
      - Behaviour is identical to the old implementation.
    """

    def __init__(
        self,
        service_id: str,
        worker_count: int,
        handler: JobHandler,
        webhook_endpoints: Optional[List[dict]] = None,
    ):
        self.service_id = service_id
        self._handler = handler
        self._worker_count = worker_count  # legacy / fallback
        self._webhook_endpoints: List[dict] = webhook_endpoints or []

        # Shared FIFO queue — all slot pools drain this same queue
        self.queue: asyncio.Queue[dict] = asyncio.Queue()

        self._slot_pools: List[WebhookSlotPool] = []
        self._legacy_tasks: List[asyncio.Task] = []  # used in legacy mode

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._webhook_endpoints:
            self._start_multi_webhook()
        else:
            self._start_legacy()

    def _start_multi_webhook(self) -> None:
        """Start one WebhookSlotPool per configured webhook endpoint."""
        for idx, ep in enumerate(self._webhook_endpoints):
            url = ep.get("url", "")
            workers = max(1, int(ep.get("workers", 1)))
            slot = WebhookSlotPool(
                webhook_url=url,
                max_workers=workers,
                shared_queue=self.queue,
                handler=self._handler,
                service_id=self.service_id,
                slot_index=idx,
            )
            slot.start()
            self._slot_pools.append(slot)
        logger.info(
            "ServiceWorkerPool (multi-webhook) started: service=%s endpoints=%d total_workers=%d",
            self.service_id,
            len(self._slot_pools),
            sum(s.max_workers for s in self._slot_pools),
        )

    def _start_legacy(self) -> None:
        """Legacy mode: N workers, webhook_url untouched in record."""
        count = max(1, self._worker_count)
        self._legacy_tasks = [
            asyncio.create_task(
                self._legacy_worker_loop(),
                name=f"worker-{self.service_id}-{i}",
            )
            for i in range(count)
        ]
        logger.info(
            "ServiceWorkerPool (legacy) started: service=%s workers=%d",
            self.service_id, count,
        )

    def stop(self) -> None:
        for slot in self._slot_pools:
            slot.stop()
        self._slot_pools.clear()
        for t in self._legacy_tasks:
            t.cancel()
        self._legacy_tasks.clear()

    # ------------------------------------------------------------------
    # Legacy worker loop (unchanged from original implementation)
    # ------------------------------------------------------------------

    async def _legacy_worker_loop(self) -> None:
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
    # Resize (legacy mode only — called when worker_count changes at runtime)
    # ------------------------------------------------------------------

    def resize(self, new_count: int) -> None:
        """Resize the legacy worker pool. No-op in multi-webhook mode."""
        if self._slot_pools:
            logger.warning(
                "resize() called on multi-webhook ServiceWorkerPool for service=%s; ignored",
                self.service_id,
            )
            return

        new_count = max(1, new_count)
        diff = new_count - self._worker_count
        self._worker_count = new_count

        if diff > 0:
            for i in range(diff):
                t = asyncio.create_task(
                    self._legacy_worker_loop(),
                    name=f"worker-{self.service_id}-resize-{i}",
                )
                self._legacy_tasks.append(t)
            logger.info("Scaled UP service %s to %d workers (+%d)", self.service_id, new_count, diff)
        elif diff < 0:
            for _ in range(-diff):
                if self._legacy_tasks:
                    self._legacy_tasks.pop().cancel()
            logger.info("Scaled DOWN service %s to %d workers (%d)", self.service_id, new_count, diff)

    # ------------------------------------------------------------------
    # Enqueueing
    # ------------------------------------------------------------------

    async def enqueue(self, record: dict, delay: float = 0.0) -> None:
        if delay > 0:
            asyncio.create_task(self._delayed_put(record, delay))
        else:
            await self.queue.put(record)

    async def _delayed_put(self, record: dict, delay: float) -> None:
        await asyncio.sleep(delay)
        await self.queue.put(record)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def depth(self) -> int:
        return self.queue.qsize()

    @property
    def worker_count(self) -> int:
        if self._slot_pools:
            return sum(s.max_workers for s in self._slot_pools)
        return self._worker_count

    @property
    def active_workers(self) -> int:
        if self._slot_pools:
            return sum(s.active for s in self._slot_pools)
        return sum(1 for t in self._legacy_tasks if not t.done())

    def webhook_slot_stats(self) -> List[dict]:
        """Return per-slot stats for multi-webhook mode."""
        return [
            {
                "url": s.webhook_url,
                "max_workers": s.max_workers,
                "active": s.active,
                "has_capacity": s.has_capacity,
            }
            for s in self._slot_pools
        ]


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

    def get_or_create_pool(
        self,
        service_id: str,
        worker_count: int = 1,
        webhook_endpoints: Optional[List[dict]] = None,
    ) -> ServiceWorkerPool:
        if service_id not in self._pools:
            assert self._handler is not None, "Call set_handler() before enqueueing."
            pool = ServiceWorkerPool(
                service_id=service_id,
                worker_count=worker_count,
                handler=self._handler,
                webhook_endpoints=webhook_endpoints or [],
            )
            pool.start()
            self._pools[service_id] = pool
        return self._pools[service_id]

    def set_concurrency(self, service_id: str, new_count: int) -> None:
        """Resize an existing pool's legacy worker count at runtime."""
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
        webhook_endpoints: Optional[List[dict]] = None,
    ) -> None:
        pool = self.get_or_create_pool(service_id, worker_count, webhook_endpoints)
        await pool.enqueue(record, delay=delay)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def queue_depth(self, service_id: str) -> int:
        pool = self._pools.get(service_id)
        return pool.depth if pool else 0

    def stats(self) -> dict[str, Any]:
        result = {}
        for sid, p in self._pools.items():
            entry: dict[str, Any] = {
                "queue_depth": p.depth,
                "workers": p.worker_count,
                "active_workers": p.active_workers,
            }
            slot_stats = p.webhook_slot_stats()
            if slot_stats:
                entry["webhook_slots"] = slot_stats
            result[sid] = entry
        return result


# Module-level singleton — imported by main.py and dispatcher.py
engine = QueueEngine()
