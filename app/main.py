from datetime import datetime
import uuid
from typing import Any, Literal, Optional

from fastapi import Body, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from fastapi.responses import JSONResponse
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
    update_request_fields,
)

app = FastAPI(title="Traffic Orchestrator", version="2.0.0", docs_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

EXTERNAL_API_PREFIX = ""


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


def _require_supabase() -> None:
    if not is_supabase_enabled():
        raise HTTPException(503, "Supabase is not configured")


def _find_duplicate_service(url: str, service_type: str, exclude_id: Optional[str] = None) -> Optional[dict]:
    normalized_url = url.rstrip("/")
    for item in fetch_services_from_supabase():
        sid = item.get("id")
        if exclude_id and sid == exclude_id:
            continue
        if item.get("type") == service_type and item.get("url", "").rstrip("/") == normalized_url:
            return item
    return None


def _find_duplicate_service_name(name: str, exclude_id: Optional[str] = None) -> Optional[dict]:
    normalized_name = name.strip().lower()
    for item in fetch_services_from_supabase():
        sid = item.get("id")
        if exclude_id and sid == exclude_id:
            continue
        item_name = str(item.get("name", "")).strip().lower()
        if item_name and item_name == normalized_name:
            return item
    return None


@app.get("/services")
def list_services():
    _require_supabase()
    return fetch_services_from_supabase()


@app.get("/docs", include_in_schema=False)
def custom_swagger_ui_html():
    openapi_url = f"{EXTERNAL_API_PREFIX}/openapi.json" if EXTERNAL_API_PREFIX else "/openapi.json"
    oauth2_redirect = f"{EXTERNAL_API_PREFIX}/docs/oauth2-redirect" if EXTERNAL_API_PREFIX else "/docs/oauth2-redirect"
    return get_swagger_ui_html(openapi_url=openapi_url, title=f"{app.title} - Swagger UI", oauth2_redirect_url=oauth2_redirect)


@app.get("/docs/oauth2-redirect", include_in_schema=False)
def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/workers")
def get_workers():
    _require_supabase()
    services = fetch_services_from_supabase()
    rows = []
    total = 0
    for s in services:
        wc = int(s.get("worker_count") or 1)
        total += wc
        rows.append({"service_id": s.get("id"), "service_name": s.get("name"), "workers": wc, "enabled": bool(s.get("enabled", True))})
    return {
        "mode": "supabase-queues",
        "online_workers": total,
        "total_concurrency": total,
        "configured_concurrency": total,
        "workers": rows,
    }


@app.post("/workers/concurrency")
def set_workers_concurrency(update: WorkerConcurrencyUpdate):
    _require_supabase()
    services = fetch_services_from_supabase()
    for s in services:
        s["worker_count"] = update.concurrency
        sync_service_to_supabase(s)
    return {"updated": True, "target_concurrency": update.concurrency}


@app.post("/services", status_code=201)
def create_service(service: ServiceConfig):
    _require_supabase()
    duplicate_name = _find_duplicate_service_name(service.name)
    if duplicate_name:
        return JSONResponse(status_code=200, content=duplicate_name)
    duplicate = _find_duplicate_service(service.url, service.type)
    if duplicate:
        return JSONResponse(status_code=200, content=duplicate)
    payload = service.model_dump()
    sync_service_to_supabase(payload)
    return payload


@app.get("/services/{service_id}")
def get_service(service_id: str):
    _require_supabase()
    service = fetch_service_from_supabase(service_id)
    if not service:
        raise HTTPException(404, "Service not found")
    return service


@app.put("/services/{service_id}")
def update_service(service_id: str, service: ServiceConfig):
    _require_supabase()
    existing = fetch_service_from_supabase(service_id)
    if not existing:
        raise HTTPException(404, "Service not found")
    duplicate_name = _find_duplicate_service_name(service.name, exclude_id=service_id)
    if duplicate_name:
        return JSONResponse(status_code=200, content=duplicate_name)
    if _find_duplicate_service(service.url, service.type, exclude_id=service_id):
        raise HTTPException(409, "Service with the same URL and type already exists")

    service.id = service_id
    service.created_at = existing.get("created_at", service.created_at)
    payload = service.model_dump()
    sync_service_to_supabase(payload)
    return payload


@app.delete("/services/{service_id}")
def delete_service(service_id: str):
    _require_supabase()
    if not fetch_service_from_supabase(service_id):
        raise HTTPException(404, "Service not found")
    if not delete_service_from_supabase(service_id):
        raise HTTPException(502, "Failed to delete service from Supabase")
    return {"deleted": service_id}


@app.post("/services/{service_id}/pause")
def pause_service(service_id: str):
    _require_supabase()
    service = fetch_service_from_supabase(service_id)
    if not service:
        raise HTTPException(404, "Service not found")
    service["enabled"] = False
    sync_service_to_supabase(service)
    return {"status": "paused", "service_id": service_id}


@app.post("/services/{service_id}/resume")
def resume_service(service_id: str):
    _require_supabase()
    service = fetch_service_from_supabase(service_id)
    if not service:
        raise HTTPException(404, "Service not found")
    service["enabled"] = True
    sync_service_to_supabase(service)

    paused_reqs = fetch_requests_from_supabase(service_id=service_id, status="paused", limit=1000)
    requeued = 0
    for req in paused_reqs:
        update_request_fields(req["id"], {"status": "queued", "error": None, "updated_at": datetime.utcnow().isoformat()})
        requeued += 1
    return {"status": "resumed", "service_id": service_id, "requeued": requeued}


@app.post("/dispatch")
def dispatch(req: DispatchRequest, wait_for_result: bool = False, timeout_seconds: Optional[int] = None):
    _require_supabase()
    service = fetch_service_from_supabase(req.service_id)
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
    sync_request_to_supabase(record)
    return JSONResponse(status_code=202, content={"request_id": request_id, "status": "queued", "scene_id": record.get("scene_id")})


@app.get("/requests")
def list_requests(service_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100, include_payload: bool = False):
    _require_supabase()
    records = fetch_requests_from_supabase(service_id=service_id, status=status, limit=limit)
    if not include_payload:
        records = [_strip_heavy_payload(r) for r in records]
    return records


@app.get("/requests/{request_id}")
def get_request(request_id: str):
    _require_supabase()
    record = fetch_request_from_supabase(request_id)
    if not record:
        raise HTTPException(404, "Request not found")
    return record


@app.post("/requests/{request_id}/cancel")
def cancel_request(request_id: str):
    _require_supabase()
    record = fetch_request_from_supabase(request_id)
    if not record:
        raise HTTPException(404, "Request not found")
    if record.get("status") in {"success", "failed", "cancelled"}:
        return {"cancelled": request_id}
    update_request_fields(request_id, {"status": "cancelled", "updated_at": datetime.utcnow().isoformat()})
    return {"cancelled": request_id}


@app.delete("/requests/{request_id}")
def delete_request(request_id: str):
    _require_supabase()
    if not delete_request_from_supabase(request_id):
        raise HTTPException(502, "Failed to delete request from Supabase")
    return {"deleted": request_id}


@app.get("/stats")
def stats():
    _require_supabase()
    records = fetch_requests_from_supabase(limit=1000)
    services = fetch_services_from_supabase()

    by_status = {}
    by_service = {}
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
    return {"status": "ok", "time": datetime.utcnow().isoformat(), "supabase_enabled": is_supabase_enabled()}


@app.post("/sink")
def sink(payload: Any = Body(...)):
    return {"status": "accepted", "received": payload, "time": datetime.utcnow().isoformat()}


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
