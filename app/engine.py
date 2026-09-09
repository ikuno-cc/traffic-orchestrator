"""
Asyncio-based per-service job queue engine with multi-endpoint routing.
Replaces Celery + SQLAlchemy broker entirely.

Each service gets:
  - a shared asyncio.Queue (FIFO, in-process, no broker needed)
  - a list of backend EndpointSlots in priority order:
      [EndpointSlot(url1, workers1), EndpointSlot(url2, workers2), ...]

Scheduling strategy:
  1. Requests arrive and enter the service's queue.
  2. The scheduler checks endpoints in configured priority order (first URL first).
  3. If Endpoint 1 has free capacity (active < max_workers), the job is dispatched to Endpoint 1.
  4. When Endpoint 1's workers are completely filled, the scheduler moves to Endpoint 2.
  5. When all available endpoints' workers are full, requests wait in the queue.
  6. As soon as ANY worker on ANY endpoint finishes, the slot is freed, and the next
     queued request is immediately assigned to whichever endpoint worker finished first.

Workers are lightweight coroutines. Blocking HTTP calls inside the dispatcher
are offloaded to a thread pool via asyncio.to_thread(), keeping the event
loop responsive regardless of service latency.
"""
from __future__ import annotations

import asyncio
import copy
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

JobHandler = Callable[[dict[str, Any]], Awaitable[None]]


# ---------------------------------------------------------------------------
# Single backend endpoint slot tracking
# ---------------------------------------------------------------------------

class EndpointSlot:
    """Represents a single backend URL with a concurrency worker limit."""

    def __init__(self, url: str, max_workers: int):
        self.url = url
        self.max_workers = max(1, max_workers)
        self.active = 0

    @property
    def has_capacity(self) -> bool:
        return self.active < self.max_workers


# ---------------------------------------------------------------------------
# Per-service worker pool (manages multiple backend endpoints)
# ---------------------------------------------------------------------------

class ServiceWorkerPool:
    """
    Manages job scheduling across one or more backend endpoint URLs for a single service.
    """

    def __init__(
        self,
        service_id: str,
        handler: JobHandler,
        endpoints: Optional[List[dict]] = None,
        worker_count: int = 1,
    ):
        self.service_id = service_id
        self._handler = handler
        self.queue: asyncio.Queue[dict] = asyncio.Queue()
        self._endpoints: List[EndpointSlot] = []
        self._slot_freed_event = asyncio.Event()
        self._scheduler_task: Optional[asyncio.Task] = None
        self._active_job_tasks: set[asyncio.Task] = set()
        self._pending_record: Optional[dict] = None

        self._init_endpoints(endpoints, worker_count)

    def _init_endpoints(self, endpoints: Optional[List[dict]], worker_count: int = 1) -> None:
        self._endpoints.clear()
        if endpoints:
            for ep in endpoints:
                url = str(ep.get("url") or "").strip()
                if url:
                    workers = max(1, int(ep.get("workers") or ep.get("worker_count") or 1))
                    self._endpoints.append(EndpointSlot(url=url, max_workers=workers))

        # Fallback if no explicit endpoints provided
        if not self._endpoints:
            self._endpoints.append(EndpointSlot(url="", max_workers=max(1, worker_count)))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self._scheduler_task is None or self._scheduler_task.done():
            self._slot_freed_event.set()
            self._scheduler_task = asyncio.create_task(
                self._scheduler_loop(),
                name=f"scheduler-{self.service_id}",
            )
            logger.info(
                "ServiceWorkerPool started: service=%s endpoints=%d total_workers=%d",
                self.service_id,
                len(self._endpoints),
                sum(ep.max_workers for ep in self._endpoints),
            )

    def stop(self) -> None:
        if self._scheduler_task:
            self._scheduler_task.cancel()
            self._scheduler_task = None
        for t in list(self._active_job_tasks):
            t.cancel()
        self._active_job_tasks.clear()
        for ep in self._endpoints:
            ep.active = 0
        logger.info("ServiceWorkerPool stopped for service %s", self.service_id)

    # ------------------------------------------------------------------
    # Dynamic update
    # ------------------------------------------------------------------

    def update_endpoints(self, endpoints: Optional[List[dict]], worker_count: int = 1) -> None:
        """Update backend endpoints and worker capacities at runtime."""
        # Preserve active count on matching URLs if possible
        old_active_map = {ep.url: ep.active for ep in self._endpoints}
        self._init_endpoints(endpoints, worker_count)
        for ep in self._endpoints:
            if ep.url in old_active_map:
                ep.active = min(old_active_map[ep.url], ep.max_workers)
        self._slot_freed_event.set()
        logger.info(
            "ServiceWorkerPool updated: service=%s endpoints=%d total_workers=%d",
            self.service_id,
            len(self._endpoints),
            sum(ep.max_workers for ep in self._endpoints),
        )

    # ------------------------------------------------------------------
    # Scheduler Loop
    # ------------------------------------------------------------------

    async def _scheduler_loop(self) -> None:
        svc = self.service_id
        while True:
            try:
                record = await self.queue.get()
                self._pending_record = record

                # Find the first available endpoint (in priority order)
                while True:
                    target_ep: Optional[EndpointSlot] = None
                    for ep in self._endpoints:
                        if ep.has_capacity:
                            target_ep = ep
                            break

                    if target_ep is not None:
                        target_ep.active += 1
                        self._pending_record = None
                        task = asyncio.create_task(
                            self._execute_job(target_ep, record),
                            name=f"worker-{svc}-{record.get('id')}",
                        )
                        self._active_job_tasks.add(task)
                        task.add_done_callback(self._active_job_tasks.discard)
                        self.queue.task_done()
                        break
                    else:
                        # All endpoints are filled to their worker capacity!
                        # Wait until any running worker finishes and frees a slot.
                        self._slot_freed_event.clear()
                        await self._slot_freed_event.wait()
            except asyncio.CancelledError:
                logger.debug("Scheduler loop cancelled for service %s", svc)
                break
            except Exception as exc:
                logger.exception("Unexpected error in scheduler loop for service %s: %s", svc, exc)
                await asyncio.sleep(0.5)

    async def _execute_job(self, endpoint: EndpointSlot, record: dict) -> None:
        try:
            job_record = copy.copy(record)
            if endpoint.url:
                job_record["target_url"] = endpoint.url
            await self._handler(job_record)
        except Exception as exc:
            logger.exception(
                "Unhandled error executing job %s on %s for service %s: %s",
                record.get("id"), endpoint.url, self.service_id, exc
            )
        finally:
            endpoint.active = max(0, endpoint.active - 1)
            # Wake up the scheduler so any queued job can be dispatched to this free slot
            self._slot_freed_event.set()

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
        return self.queue.qsize() + (1 if self._pending_record is not None else 0)

    @property
    def worker_count(self) -> int:
        return sum(ep.max_workers for ep in self._endpoints)

    @property
    def active_workers(self) -> int:
        return sum(ep.active for ep in self._endpoints)

    def endpoint_stats(self) -> List[dict]:
        return [
            {
                "url": ep.url,
                "max_workers": ep.max_workers,
                "active": ep.active,
                "has_capacity": ep.has_capacity,
            }
            for ep in self._endpoints
        ]


# ---------------------------------------------------------------------------
# Engine (singleton)
# ---------------------------------------------------------------------------

class QueueEngine:
    """
    Manages per-service worker pools and dynamic endpoint dispatching.
    """

    def __init__(self):
        self._pools: Dict[str, ServiceWorkerPool] = {}
        self._handler: Optional[JobHandler] = None
        self._running = False

    def set_handler(self, handler: JobHandler) -> None:
        self._handler = handler

    def start(self) -> None:
        self._running = True

    def stop_all(self) -> None:
        self._running = False
        for pool in self._pools.values():
            pool.stop()
        self._pools.clear()

    def get_or_create_pool(
        self,
        service_id: str,
        endpoints: Optional[List[dict]] = None,
        worker_count: int = 1,
    ) -> ServiceWorkerPool:
        if service_id not in self._pools:
            assert self._handler is not None, "Call set_handler() before enqueueing."
            pool = ServiceWorkerPool(
                service_id=service_id,
                handler=self._handler,
                endpoints=endpoints,
                worker_count=worker_count,
            )
            pool.start()
            self._pools[service_id] = pool
        return self._pools[service_id]

    def update_pool(
        self,
        service_id: str,
        endpoints: Optional[List[dict]] = None,
        worker_count: int = 1,
    ) -> None:
        """Update an existing pool's endpoints or create it if not yet existing."""
        pool = self._pools.get(service_id)
        if pool:
            pool.update_endpoints(endpoints, worker_count)
        else:
            self.get_or_create_pool(service_id, endpoints, worker_count)

    def set_concurrency(self, service_id: str, new_count: int) -> None:
        """Legacy resize method."""
        pool = self._pools.get(service_id)
        if pool:
            pool.update_endpoints(None, new_count)

    def remove_pool(self, service_id: str) -> None:
        pool = self._pools.pop(service_id, None)
        if pool:
            pool.stop()

    async def enqueue(
        self,
        service_id: str,
        record: dict,
        delay: float = 0.0,
        endpoints: Optional[List[dict]] = None,
        worker_count: int = 1,
    ) -> None:
        pool = self.get_or_create_pool(service_id, endpoints=endpoints, worker_count=worker_count)
        await pool.enqueue(record, delay=delay)

    def queue_depth(self, service_id: str) -> int:
        pool = self._pools.get(service_id)
        return pool.depth if pool else 0

    def stats(self) -> dict[str, Any]:
        return {
            sid: {
                "queue_depth": p.depth,
                "workers": p.worker_count,
                "active_workers": p.active_workers,
                "endpoints": p.endpoint_stats(),
            }
            for sid, p in self._pools.items()
        }


# Module-level singleton
engine = QueueEngine()
