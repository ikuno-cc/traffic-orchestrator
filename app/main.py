from datetime import datetime
import json
import os
import uuid
import time
from typing import Any, Literal, Optional

import redis
from celery.result import AsyncResult
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.supabase_sync import (
    delete_request_from_supabase,
    delete_service_from_supabase,
    fetch_request_from_supabase,
    fetch_requests_from_supabase,
    fetch_service_from_supabase,
    fetch_services_from_supabase,
    is_supabase_enabled,
    sync_request_to_supabase,
    sync_service_to_supabase,
)
from workers.celery_app import celery_app
from workers.tasks import dispatch_task

app = FastAPI(
    title="Traffic Orchestrator",
    version="1.0.0",
    docs_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_sync():
    _seed_services_from_supabase_if_needed()

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)

EXTERNAL_API_PREFIX = os.getenv("APP_EXTERNAL_API_PREFIX", "").rstrip("/")

SERVICES_KEY = "orchestrator:services"
PAUSED_KEY = "orchestrator:paused_services"
REQUESTS_KEY = "orchestrator:requests"
WORKER_LIMIT_KEY = "orchestrator:worker_limit"
RUNNING_SLOTS_KEY = "orchestrator:running_slots"
SUPABASE_RECONCILE_SECONDS = max(1, int(os.getenv("SUPABASE_RECONCILE_SECONDS", "5")))
_last_service_reconcile_at = 0.0
_last_request_reconcile_at = 0.0

CELERY_STATE_MAP = {
    "PENDING": "queued",
    "STARTED": "running",
    "RETRY": "retrying",
    "SUCCESS": "success",
    "FAILURE": "failed",
    "REVOKED": "cancelled",
}


def _persist_service(service: dict[str, Any]) -> None:
    if is_supabase_enabled():
        sync_service_to_supabase(service)


def _persist_request(record: dict[str, Any]) -> None:
    if is_supabase_enabled():
        sync_request_to_supabase(record)


def _seed_services_from_supabase_if_needed() -> None:
    if not is_supabase_enabled():
        return
    if redis_client.hlen(SERVICES_KEY) > 0:
        return
    for service in fetch_services_from_supabase():
        redis_client.hset(SERVICES_KEY, service["id"], json.dumps(service))


def _seed_requests_from_supabase_if_needed(limit: int = 1000) -> None:
    if not is_supabase_enabled():
        return
    if redis_client.hlen(REQUESTS_KEY) > 0:
        return
    for record in fetch_requests_from_supabase(limit=limit):
        redis_client.hset(REQUESTS_KEY, record["id"], json.dumps(record))


def _reconcile_services_from_supabase(limit: int = 1000) -> None:
    global _last_service_reconcile_at
    if not is_supabase_enabled():
        return
    now = time.time()
    if now - _last_service_reconcile_at < SUPABASE_RECONCILE_SECONDS:
        return
    _last_service_reconcile_at = now
    for service in fetch_services_from_supabase()[:limit]:
        sid = service.get("id")
        if not sid:
            continue
        if not redis_client.hexists(SERVICES_KEY, sid):
            redis_client.hset(SERVICES_KEY, sid, json.dumps(service))


def _reconcile_requests_from_supabase(limit: int = 1000) -> None:
    global _last_request_reconcile_at
    if not is_supabase_enabled():
        return
    now = time.time()
    if now - _last_request_reconcile_at < SUPABASE_RECONCILE_SECONDS:
        return
    _last_request_reconcile_at = now
    for record in fetch_requests_from_supabase(limit=limit):
        rid = record.get("id")
        if not rid:
            continue
        if not redis_client.hexists(REQUESTS_KEY, rid):
            redis_client.hset(REQUESTS_KEY, rid, json.dumps(record))


class ServiceConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    url: str
    type: Literal["comfyui", "n8n", "custom", "omnivoice"]
    description: Optional[str] = ""
    headers: dict = Field(default_factory=dict)
    timeout: int = Field(default=120, ge=1, le=3600)
    delay_seconds: float = Field(default=3, ge=0, le=3600)
    enabled: bool = True
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class DispatchRequest(BaseModel):
    payload: Any
    service_id: str
    metadata: dict = Field(default_factory=dict)
    scene_id: Optional[Any] = None
    priority: int = Field(default=5, ge=1, le=10)
    webhook_url: Optional[str] = None
    delay_seconds: Optional[float] = Field(default=None, ge=0, le=3600)

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        if not value.startswith(("http://", "https://")):
            raise ValueError("webhook_url must start with http:// or https://")
        return value


class WorkerConcurrencyUpdate(BaseModel):
    concurrency: int = Field(ge=1, le=64)


def _sync_celery_status(record: dict) -> dict:
    task_id = record.get("celery_task_id")
    if not task_id:
        return record
    if record.get("status") not in ("queued", "running", "retrying"):
        return record

    result = AsyncResult(task_id, app=celery_app)
    new_status = CELERY_STATE_MAP.get(result.state)
    waiting_for_slot = (
        record.get("status") == "queued"
        and isinstance(record.get("error"), str)
        and record.get("error", "").startswith("Waiting for worker slot")
    )

    if waiting_for_slot and result.state == "STARTED":
        return record

    if new_status and new_status != record.get("status"):
        record["status"] = new_status
        record["updated_at"] = datetime.utcnow().isoformat()
        if result.state == "FAILURE":
            record["error"] = str(result.result)
        redis_client.hset(REQUESTS_KEY, record["id"], json.dumps(record))
        _persist_request(record)

    return record


def _service_exists(service_id: str) -> bool:
    _seed_services_from_supabase_if_needed()
    if not redis_client.hexists(SERVICES_KEY, service_id) and is_supabase_enabled():
        service = fetch_service_from_supabase(service_id)
        if service:
            redis_client.hset(SERVICES_KEY, service_id, json.dumps(service))
    return bool(redis_client.hexists(SERVICES_KEY, service_id))


def _find_duplicate_service(
    url: str, service_type: str, exclude_id: Optional[str] = None
) -> Optional[dict]:
    _seed_services_from_supabase_if_needed()
    all_services = redis_client.hgetall(SERVICES_KEY)
    normalized_url = url.rstrip("/")
    for sid, raw in all_services.items():
        if exclude_id and sid == exclude_id:
            continue
        item = json.loads(raw)
        if item.get("type") == service_type and item.get("url", "").rstrip("/") == normalized_url:
            item["paused"] = redis_client.sismember(PAUSED_KEY, sid)
            return item
    return None


def _worker_snapshot() -> dict:
    inspector = celery_app.control.inspect(timeout=1.0)
    stats = inspector.stats() or {}
    active = inspector.active() or {}

    workers = []
    for name, info in stats.items():
        pool = info.get("pool", {}) or {}
        max_concurrency = pool.get("max-concurrency")
        if max_concurrency is None:
            max_concurrency = len(pool.get("processes") or [])
        worker_concurrency = int(max_concurrency or 0)
        workers.append(
            {
                "name": name,
                "concurrency": worker_concurrency,
                "active_tasks": len(active.get(name, [])),
            }
        )

    total_concurrency = sum(w["concurrency"] for w in workers)
    unique_concurrency = sorted({w["concurrency"] for w in workers if w["concurrency"] > 0})

    configured_limit = redis_client.get(WORKER_LIMIT_KEY)
    try:
        configured_concurrency = int(configured_limit) if configured_limit else None
    except ValueError:
        configured_concurrency = None

    if configured_concurrency is None:
        configured_concurrency = total_concurrency if total_concurrency > 0 else 1

    try:
        running_slots = max(0, int(redis_client.get(RUNNING_SLOTS_KEY) or 0))
    except ValueError:
        running_slots = 0

    return {
        "online_workers": len(workers),
        "total_concurrency": total_concurrency,
        "per_worker_concurrency": unique_concurrency[0] if len(unique_concurrency) == 1 else None,
        "mixed_concurrency": len(unique_concurrency) > 1,
        "configured_concurrency": configured_concurrency,
        "running_slots": running_slots,
        "workers": workers,
    }


@app.get("/services")
def list_services():
    _seed_services_from_supabase_if_needed()
    _reconcile_services_from_supabase(limit=1000)
    raw = redis_client.hgetall(SERVICES_KEY)
    services = [json.loads(v) for v in raw.values()]
    paused = redis_client.smembers(PAUSED_KEY)
    for service in services:
        service["paused"] = service["id"] in paused
    return services


@app.get("/docs", include_in_schema=False)
def custom_swagger_ui_html():
    openapi_url = f"{EXTERNAL_API_PREFIX}/openapi.json" if EXTERNAL_API_PREFIX else "/openapi.json"
    oauth2_redirect = (
        f"{EXTERNAL_API_PREFIX}/docs/oauth2-redirect"
        if EXTERNAL_API_PREFIX
        else "/docs/oauth2-redirect"
    )
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title=f"{app.title} - Swagger UI",
        oauth2_redirect_url=oauth2_redirect,
    )


@app.get("/docs/oauth2-redirect", include_in_schema=False)
def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/workers")
def get_workers():
    return _worker_snapshot()

@app.post("/workers/concurrency")
def set_workers_concurrency(update: WorkerConcurrencyUpdate):
    redis_client.set(WORKER_LIMIT_KEY, update.concurrency)
    updated = _worker_snapshot()
    return {"updated": True, "target_concurrency": update.concurrency, "snapshot": updated}


@app.post("/services", status_code=201)
def create_service(service: ServiceConfig):
    duplicate = _find_duplicate_service(service.url, service.type)
    if duplicate:
        return JSONResponse(status_code=200, content=duplicate)
    payload = service.model_dump()
    redis_client.hset(SERVICES_KEY, service.id, json.dumps(payload))
    _persist_service(payload)
    return service


@app.get("/services/{service_id}")
def get_service(service_id: str):
    _seed_services_from_supabase_if_needed()
    raw = redis_client.hget(SERVICES_KEY, service_id)
    if not raw and is_supabase_enabled():
        service = fetch_service_from_supabase(service_id)
        if service:
            redis_client.hset(SERVICES_KEY, service_id, json.dumps(service))
            raw = redis_client.hget(SERVICES_KEY, service_id)
    if not raw:
        raise HTTPException(404, "Service not found")
    service = json.loads(raw)
    service["paused"] = redis_client.sismember(PAUSED_KEY, service_id)
    return service


@app.put("/services/{service_id}")
def update_service(service_id: str, service: ServiceConfig):
    duplicate = _find_duplicate_service(service.url, service.type, exclude_id=service_id)
    if duplicate:
        raise HTTPException(409, "Service with the same URL and type already exists")

    _seed_services_from_supabase_if_needed()
    raw_existing = redis_client.hget(SERVICES_KEY, service_id)
    if not raw_existing and is_supabase_enabled():
        existing_from_supabase = fetch_service_from_supabase(service_id)
        if existing_from_supabase:
            redis_client.hset(SERVICES_KEY, service_id, json.dumps(existing_from_supabase))
            raw_existing = redis_client.hget(SERVICES_KEY, service_id)
    if not raw_existing:
        raise HTTPException(404, "Service not found")
    existing = json.loads(raw_existing)

    service.id = service_id
    service.created_at = existing.get("created_at", service.created_at)
    payload = service.model_dump()
    redis_client.hset(SERVICES_KEY, service_id, json.dumps(payload))
    _persist_service(payload)
    return service


@app.delete("/services/{service_id}")
def delete_service(service_id: str):
    if not _service_exists(service_id):
        raise HTTPException(404, "Service not found")
    if is_supabase_enabled():
        deleted = delete_service_from_supabase(service_id)
        if not deleted:
            raise HTTPException(502, "Failed to delete service from Supabase")
    redis_client.hdel(SERVICES_KEY, service_id)
    redis_client.srem(PAUSED_KEY, service_id)
    return {"deleted": service_id}


@app.post("/services/{service_id}/pause")
def pause_service(service_id: str):
    if not _service_exists(service_id):
        raise HTTPException(404, "Service not found")
    redis_client.sadd(PAUSED_KEY, service_id)
    return {"status": "paused", "service_id": service_id}


@app.post("/services/{service_id}/resume")
def resume_service(service_id: str):
    if not _service_exists(service_id):
        raise HTTPException(404, "Service not found")

    redis_client.srem(PAUSED_KEY, service_id)
    paused_reqs = _get_paused_requests_for_service(service_id)

    requeued = 0
    for req in paused_reqs:
        _requeue_request(req)
        requeued += 1

    return {"status": "resumed", "service_id": service_id, "requeued": requeued}


@app.post("/dispatch")
def dispatch(req: DispatchRequest, wait_for_result: bool = False, timeout_seconds: Optional[int] = None):
    _seed_services_from_supabase_if_needed()
    _reconcile_services_from_supabase(limit=1000)
    raw = redis_client.hget(SERVICES_KEY, req.service_id)
    if not raw and is_supabase_enabled():
        service_from_supabase = fetch_service_from_supabase(req.service_id)
        if service_from_supabase:
            redis_client.hset(SERVICES_KEY, req.service_id, json.dumps(service_from_supabase))
            raw = redis_client.hget(SERVICES_KEY, req.service_id)
    if not raw:
        raise HTTPException(404, "Service not found")
    service = json.loads(raw)
    if not service.get("enabled", True):
        raise HTTPException(409, "Service is disabled")
    if service.get("type") not in {"comfyui", "n8n", "custom", "omnivoice"}:
        raise HTTPException(409, "Service type is invalid. Update or recreate this service.")

    is_paused = redis_client.sismember(PAUSED_KEY, req.service_id)
    service_delay = float(service.get("delay_seconds", 3))
    effective_delay = req.delay_seconds if req.delay_seconds is not None else service_delay
    request_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    status = "paused" if is_paused else "queued"

    record = {
        "id": request_id,
        "service_id": req.service_id,
        "service_name": service["name"],
        "status": status,
        "created_at": now,
        "updated_at": now,
        "celery_task_id": None,
        "error": None,
        "retry_count": 0,
        "metadata": req.metadata,
        "scene_id": req.scene_id if req.scene_id is not None else req.metadata.get("scene_id"),
        "priority": req.priority,
        "payload": req.payload,
        "webhook_url": req.webhook_url,
        "delay_seconds": effective_delay,
    }

    redis_client.hset(REQUESTS_KEY, request_id, json.dumps(record))
    _persist_request(record)

    if not is_paused:
        task = dispatch_task.apply_async(
            args=[request_id, req.service_id, req.payload, req.webhook_url, effective_delay],
            priority=10 - req.priority,
        )
        record["celery_task_id"] = task.id
        redis_client.hset(REQUESTS_KEY, request_id, json.dumps(record))
        _persist_request(record)

    return JSONResponse(
        status_code=202,
        content={"request_id": request_id, "status": status, "scene_id": record.get("scene_id")},
    )


@app.get("/requests")
def list_requests(service_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100):
    limit = max(1, min(limit, 1000))
    _seed_requests_from_supabase_if_needed(limit=1000)
    _reconcile_requests_from_supabase(limit=1000)
    raw = redis_client.hgetall(REQUESTS_KEY)
    records = [json.loads(v) for v in raw.values()]

    if service_id:
        records = [r for r in records if r.get("service_id") == service_id]
    if status:
        records = [r for r in records if r.get("status") == status]

    records.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    # Only sync Celery status for the page we are about to return to keep polling fast.
    page = records[:limit]
    page = [_sync_celery_status(r) for r in page]
    return page


@app.get("/requests/{request_id}")
def get_request(request_id: str):
    _reconcile_requests_from_supabase(limit=1000)
    raw = redis_client.hget(REQUESTS_KEY, request_id)
    if not raw and is_supabase_enabled():
        record = fetch_request_from_supabase(request_id)
        if not record:
            raise HTTPException(404, "Request not found")
        redis_client.hset(REQUESTS_KEY, request_id, json.dumps(record))
        raw = redis_client.hget(REQUESTS_KEY, request_id)
    if not raw:
        if is_supabase_enabled():
            record = fetch_request_from_supabase(request_id)
            if record:
                redis_client.hset(REQUESTS_KEY, request_id, json.dumps(record))
                return record
        raise HTTPException(404, "Request not found")
    record = json.loads(raw)
    record = _sync_celery_status(record)
    return record


@app.post("/requests/{request_id}/cancel")
def cancel_request(request_id: str):
    raw = redis_client.hget(REQUESTS_KEY, request_id)
    if not raw and is_supabase_enabled():
        record = fetch_request_from_supabase(request_id)
        if record:
            redis_client.hset(REQUESTS_KEY, request_id, json.dumps(record))
            raw = redis_client.hget(REQUESTS_KEY, request_id)
    if not raw:
        raise HTTPException(404, "Request not found")

    record = json.loads(raw)
    if record.get("celery_task_id"):
        celery_app.control.revoke(record["celery_task_id"], terminate=True)

    record["status"] = "cancelled"
    record["updated_at"] = datetime.utcnow().isoformat()
    redis_client.hset(REQUESTS_KEY, request_id, json.dumps(record))
    _persist_request(record)
    return {"cancelled": request_id}


@app.delete("/requests/{request_id}")
def delete_request(request_id: str):
    if is_supabase_enabled():
        deleted = delete_request_from_supabase(request_id)
        if not deleted:
            raise HTTPException(502, "Failed to delete request from Supabase")
    redis_client.hdel(REQUESTS_KEY, request_id)
    return {"deleted": request_id}


@app.get("/stats")
def stats():
    _seed_services_from_supabase_if_needed()
    _seed_requests_from_supabase_if_needed(limit=1000)
    _reconcile_services_from_supabase(limit=1000)
    _reconcile_requests_from_supabase(limit=1000)
    raw = redis_client.hgetall(REQUESTS_KEY)
    records = [json.loads(v) for v in raw.values()]
    services_count = redis_client.hlen(SERVICES_KEY)

    paused_services = list(redis_client.smembers(PAUSED_KEY))
    by_status = {}
    by_service = {}
    for record in records:
        by_status[record["status"]] = by_status.get(record["status"], 0) + 1
        by_service[record["service_name"]] = by_service.get(record["service_name"], 0) + 1

    return {
        "total_requests": len(records),
        "by_status": by_status,
        "by_service": by_service,
        "paused_services": paused_services,
        "active_services": services_count,
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat(),
        "supabase_enabled": is_supabase_enabled(),
    }


@app.post("/sink")
def sink(payload: Any = Body(...)):
    return {
        "status": "accepted",
        "received": payload,
        "time": datetime.utcnow().isoformat(),
    }


def _get_paused_requests_for_service(service_id: str):
    _seed_requests_from_supabase_if_needed(limit=1000)
    raw = redis_client.hgetall(REQUESTS_KEY)
    if not raw and is_supabase_enabled():
        return fetch_requests_from_supabase(service_id=service_id, status="paused", limit=1000)
    result = []
    for value in raw.values():
        record = json.loads(value)
        if record.get("service_id") == service_id and record.get("status") == "paused":
            result.append(record)
    return result


def _requeue_request(record: dict):
    task = dispatch_task.apply_async(
        args=[
            record["id"],
            record["service_id"],
            record.get("payload"),
            record.get("webhook_url"),
            record.get("delay_seconds", 3),
        ],
        priority=10 - record.get("priority", 5),
    )
    record["celery_task_id"] = task.id
    record["status"] = "queued"
    record["updated_at"] = datetime.utcnow().isoformat()
    redis_client.hset(REQUESTS_KEY, record["id"], json.dumps(record))
    _persist_request(record)
