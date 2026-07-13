from datetime import datetime
import asyncio
import os
import uuid
from typing import Any, Literal, Optional

from fastapi import Body, FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.storage import (
    delete_completed_requests_older_than,
    delete_request as store_delete_request,
    delete_service as store_delete_service,
    get_request as store_get_request,
    get_service as store_get_service,
    is_storage_enabled,
    list_requests as store_list_requests,
    list_services as store_list_services,
    storage_backend_name,
    update_request_fields,
    upsert_request,
    upsert_service,
)
from workers.tasks import process_dispatch_request_task

# FastAPI's built-in /docs and /openapi.json — no custom implementation needed.
app = FastAPI(
    title="Traffic Orchestrator",
    version="2.0.0",
    root_path=os.getenv("API_ROOT_PATH", "/api")
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

REQUEST_RETENTION_HOURS = float(os.getenv("REQUEST_RETENTION_HOURS", "5"))
REQUEST_CLEANUP_INTERVAL_SECONDS = int(os.getenv("REQUEST_CLEANUP_INTERVAL_SECONDS", "600"))
WORKER_FALLBACK_QUEUE_ENABLED = os.getenv("WORKER_FALLBACK_QUEUE_ENABLED", "true").strip().lower() not in ("0", "false", "no")
DEFAULT_WORKER_QUEUE = os.getenv("DEFAULT_WORKER_QUEUE", "celery")
_cleanup_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

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
    worker_count: int = Field(default=1, ge=1, le=64)
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_storage() -> None:
    if not is_storage_enabled():
        raise HTTPException(503, "Postgres storage is not configured")


def _find_duplicate_service(url: str, service_type: str, exclude_id: Optional[str] = None) -> Optional[dict]:
    normalized_url = url.rstrip("/")
    for item in store_list_services():
        sid = item.get("id")
        if exclude_id and sid == exclude_id:
            continue
        if item.get("type") == service_type and item.get("url", "").rstrip("/") == normalized_url:
            return item
    return None


def _find_duplicate_service_name(name: str, exclude_id: Optional[str] = None) -> Optional[dict]:
    normalized_name = name.strip().lower()
    for item in store_list_services():
        sid = item.get("id")
        if exclude_id and sid == exclude_id:
            continue
        if str(item.get("name", "")).strip().lower() == normalized_name:
            return item
    return None


def _assert_worker_count_persisted(service_id: str, expected_worker_count: int) -> None:
    actual = store_get_service(service_id)
    if not actual:
        raise HTTPException(502, "Service saved but failed to read it back from Postgres")
    actual_wc = int(actual.get("worker_count") or 1)
    if actual_wc != int(expected_worker_count):
        raise HTTPException(
            409,
            "worker_count was not persisted. Add `worker_count` column to table `orch_services`.",
        )


def _strip_heavy_payload(record: dict[str, Any]) -> dict[str, Any]:
    out = dict(record)
    payload = out.get("payload")
    if isinstance(payload, dict):
        payload_copy = dict(payload)
        mp = payload_copy.get("multipart")
        if isinstance(mp, dict) and "file_base64" in mp:
            mp_copy = dict(mp)
            mp_copy["file_base64"] = "__omitted__"
            payload_copy["multipart"] = mp_copy
        out["payload"] = payload_copy
    return out


def _service_queue_name(service_id: str) -> str:
    return f"svc.{service_id}"


def _ensure_worker_consumes_queue(queue_name: str) -> bool:
    try:
        replies = process_dispatch_request_task.app.control.add_consumer(
            queue_name,
            reply=True,
            timeout=2.0,
        )
        confirmed = bool(replies)
        if confirmed:
            print(f"[QUEUE] Worker confirmed consuming {queue_name} (replies={len(replies)})")
        else:
            print(f"[QUEUE] No worker replied to add_consumer({queue_name})")
        return confirmed
    except Exception as exc:
        print(f"[QUEUE] add_consumer({queue_name}) raised: {exc}")
        return False


def _resolve_dispatch_queue(service_id: str) -> str:
    queue_name = _service_queue_name(service_id)
    confirmed = _ensure_worker_consumes_queue(queue_name)
    if not confirmed and WORKER_FALLBACK_QUEUE_ENABLED:
        return DEFAULT_WORKER_QUEUE
    return queue_name


async def _request_cleanup_loop() -> None:
    while True:
        try:
            removed = delete_completed_requests_older_than(REQUEST_RETENTION_HOURS)
            if removed:
                print(f"[CLEANUP] Deleted {removed} completed requests older than {REQUEST_RETENTION_HOURS}h")
        except Exception as exc:
            print(f"[CLEANUP] Failed: {exc}")
        await asyncio.sleep(max(30, REQUEST_CLEANUP_INTERVAL_SECONDS))


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------

@app.get("/services")
def api_list_services():
    _require_storage()
    return store_list_services()


@app.get("/workers")
def get_workers():
    _require_storage()
    services = store_list_services()
    rows = []
    total = 0
    for s in services:
        wc = int(s.get("worker_count") or 1)
        total += wc
        rows.append({"service_id": s.get("id"), "service_name": s.get("name"), "workers": wc, "enabled": bool(s.get("enabled", True))})
    return {
        "mode": "celery",
        "online_workers": None,
        "total_concurrency": total,
        "configured_concurrency": total,
        "workers": rows,
    }


@app.post("/workers/concurrency")
def set_workers_concurrency(update: WorkerConcurrencyUpdate):
    _require_storage()
    services = store_list_services()
    for s in services:
        s["worker_count"] = update.concurrency
        upsert_service(s)
        _assert_worker_count_persisted(str(s.get("id")), update.concurrency)
    return {"updated": True, "target_concurrency": update.concurrency}


@app.post("/services", status_code=201)
def create_service(service: ServiceConfig):
    _require_storage()
    existing_by_id = store_get_service(service.id)
    if existing_by_id:
        return JSONResponse(status_code=200, content=jsonable_encoder(existing_by_id))
    duplicate_name = _find_duplicate_service_name(service.name)
    if duplicate_name:
        return JSONResponse(status_code=200, content=jsonable_encoder(duplicate_name))
    duplicate = _find_duplicate_service(service.url, service.type)
    if duplicate:
        return JSONResponse(status_code=200, content=jsonable_encoder(duplicate))
    payload = service.model_dump()
    upsert_service(payload)
    _assert_worker_count_persisted(service.id, service.worker_count)
    return payload


@app.get("/services/{service_id}")
def api_get_service(service_id: str):
    _require_storage()
    service = store_get_service(service_id)
    if not service:
        raise HTTPException(404, "Service not found")
    return service


@app.put("/services/{service_id}")
def update_service(service_id: str, service: ServiceConfig):
    _require_storage()
    existing = store_get_service(service_id)
    if not existing:
        raise HTTPException(404, "Service not found")
    duplicate_name = _find_duplicate_service_name(service.name, exclude_id=service_id)
    if duplicate_name:
        return JSONResponse(status_code=200, content=jsonable_encoder(duplicate_name))
    if _find_duplicate_service(service.url, service.type, exclude_id=service_id):
        raise HTTPException(409, "Service with the same URL and type already exists")
    service.id = service_id
    service.created_at = existing.get("created_at", service.created_at)
    payload = service.model_dump()
    upsert_service(payload)
    _assert_worker_count_persisted(service_id, service.worker_count)
    return payload


@app.delete("/services/{service_id}")
def api_delete_service(service_id: str):
    _require_storage()
    if not store_get_service(service_id):
        raise HTTPException(404, "Service not found")
    if not store_delete_service(service_id):
        raise HTTPException(502, "Failed to delete service from Postgres")
    return {"deleted": service_id}


@app.post("/services/{service_id}/pause")
def pause_service(service_id: str):
    _require_storage()
    service = store_get_service(service_id)
    if not service:
        raise HTTPException(404, "Service not found")
    service["enabled"] = False
    upsert_service(service)
    return {"status": "paused", "service_id": service_id}


@app.post("/services/{service_id}/resume")
def resume_service(service_id: str):
    _require_storage()
    service = store_get_service(service_id)
    if not service:
        raise HTTPException(404, "Service not found")
    service["enabled"] = True
    upsert_service(service)
    paused_reqs = store_list_requests(service_id=service_id, status="paused", limit=1000)
    failed_reqs = store_list_requests(service_id=service_id, status="failed", limit=1000)
    requeued = 0
    for req in paused_reqs + failed_reqs:
        update_request_fields(
            req["id"],
            {"status": "queued", "error": None, "retry_count": 0, "updated_at": datetime.utcnow().isoformat()},
        )
        requeued += 1
    return {"status": "resumed", "service_id": service_id, "requeued": requeued}


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

@app.post("/dispatch")
def dispatch(req: DispatchRequest, wait_for_result: bool = False, timeout_seconds: Optional[int] = None):
    _require_storage()
    service = store_get_service(req.service_id)
    if not service:
        raise HTTPException(404, "Service not found")
    if not service.get("enabled", True):
        raise HTTPException(409, "Service is disabled")

    request_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    service_delay = float(service.get("delay_seconds", 3))
    effective_delay = req.delay_seconds if req.delay_seconds is not None else service_delay

    record = {
        "id": request_id,
        "service_id": req.service_id,
        "service_name": service["name"],
        "status": "queued",
        "created_at": now,
        "updated_at": now,
        "error": None,
        "retry_count": 0,
        "metadata": req.metadata,
        "scene_id": req.scene_id if req.scene_id is not None else req.metadata.get("scene_id"),
        "priority": req.priority,
        "payload": req.payload,
        "webhook_url": req.webhook_url,
        "delay_seconds": effective_delay,
    }
    upsert_request(record)
    queue_name = _resolve_dispatch_queue(req.service_id)
    task_record = dict(record)
    task_record["delay_seconds"] = 0
    task = process_dispatch_request_task.apply_async(
        args=[task_record],
        priority=max(0, 10 - int(req.priority)),
        countdown=max(0.0, float(effective_delay)),
        queue=queue_name,
    )
    update_request_fields(request_id, {"celery_task_id": task.id, "queue": queue_name})
    return JSONResponse(status_code=202, content={"request_id": request_id, "status": "queued", "scene_id": record.get("scene_id")})


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

@app.get("/requests")
def api_list_requests(service_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100, include_payload: bool = False):
    _require_storage()
    records = store_list_requests(service_id=service_id, status=status, limit=limit)
    if not include_payload:
        records = [_strip_heavy_payload(r) for r in records]
    return records


@app.get("/requests/{request_id}")
def api_get_request(request_id: str):
    _require_storage()
    record = store_get_request(request_id)
    if not record:
        raise HTTPException(404, "Request not found")
    return record


@app.post("/requests/{request_id}/cancel")
def cancel_request(request_id: str):
    _require_storage()
    record = store_get_request(request_id)
    if not record:
        raise HTTPException(404, "Request not found")
    if record.get("status") in {"success", "failed", "cancelled"}:
        return {"cancelled": request_id}
    update_request_fields(request_id, {"status": "cancelled", "updated_at": datetime.utcnow().isoformat()})
    return {"cancelled": request_id}


@app.post("/requests/{request_id}/retry")
def retry_request(request_id: str):
    _require_storage()
    record = store_get_request(request_id)
    if not record:
        raise HTTPException(404, "Request not found")
    if record.get("status") in {"running", "queued"}:
        return {"request_id": request_id, "status": record.get("status"), "updated": False}
    if record.get("status") == "cancelled":
        raise HTTPException(409, "Cancelled requests cannot be retried")

    update_request_fields(
        request_id,
        {"status": "queued", "error": None, "retry_count": 0, "updated_at": datetime.utcnow().isoformat()},
    )
    refreshed = store_get_request(request_id)
    if refreshed:
        service = store_get_service(str(refreshed.get("service_id")))
        countdown = float((refreshed.get("delay_seconds") if refreshed.get("delay_seconds") is not None else (service or {}).get("delay_seconds", 3)) or 0)
        queue_name = _resolve_dispatch_queue(str(refreshed.get("service_id")))
        task_record = dict(refreshed)
        task_record["delay_seconds"] = 0
        task = process_dispatch_request_task.apply_async(
            args=[task_record],
            priority=max(0, 10 - int(refreshed.get("priority", 5))),
            countdown=max(0.0, countdown),
            queue=queue_name,
        )
        update_request_fields(request_id, {"celery_task_id": task.id, "queue": queue_name})
    return {"request_id": request_id, "status": "queued", "updated": True}


@app.delete("/requests/{request_id}")
def api_delete_request(request_id: str):
    _require_storage()
    if not store_delete_request(request_id):
        raise HTTPException(502, "Failed to delete request from Postgres")
    return {"deleted": request_id}


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@app.get("/stats")
def stats():
    _require_storage()
    records = store_list_requests(limit=1000)
    services = store_list_services()
    by_status: dict = {}
    by_service: dict = {}
    for record in records:
        by_status[record.get("status", "unknown")] = by_status.get(record.get("status", "unknown"), 0) + 1
        sname = str(record.get("service_name") or record.get("service_id") or "unknown")
        by_service[sname] = by_service.get(sname, 0) + 1
    paused_services = [s.get("id") for s in services if not bool(s.get("enabled", True))]
    return {
        "total_requests": len(records),
        "by_status": by_status,
        "by_service": by_service,
        "paused_services": paused_services,
        "active_services": len(services),
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat(),
        "storage_backend": storage_backend_name(),
        "storage_enabled": is_storage_enabled(),
    }


@app.post("/sink")
def sink(payload: Any = Body(...)):
    return {"status": "accepted", "received": payload, "time": datetime.utcnow().isoformat()}


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def _on_startup() -> None:
    global _cleanup_task
    try:
        for svc in store_list_services():
            sid = str(svc.get("id") or "")
            if sid:
                _ensure_worker_consumes_queue(_service_queue_name(sid))
    except Exception as exc:
        print(f"[QUEUE] Startup queue sync failed: {exc}")
    _cleanup_task = asyncio.create_task(_request_cleanup_loop())


@app.on_event("shutdown")
async def _on_shutdown() -> None:
    global _cleanup_task
    if _cleanup_task:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
