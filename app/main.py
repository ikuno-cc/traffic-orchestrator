"""
Traffic Orchestrator — single-process FastAPI app.
Celery has been replaced with an asyncio queue engine (app/engine.py).
All blocking HTTP calls run in a thread-pool via asyncio.to_thread().

One container. No broker. No worker process. Just Python + Postgres.
"""
from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Literal, Optional

from fastapi import Body, FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.dispatcher import handle_job
from app.engine import engine
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

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

EXTERNAL_API_PREFIX = os.getenv("EXTERNAL_API_PREFIX", "") or os.getenv("APP_EXTERNAL_API_PREFIX", "")
REQUEST_RETENTION_HOURS = float(os.getenv("REQUEST_RETENTION_HOURS", "5"))
REQUEST_CLEANUP_INTERVAL_SECONDS = int(os.getenv("REQUEST_CLEANUP_INTERVAL_SECONDS", "600"))

_cleanup_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------------------
# Lifespan (replaces deprecated @app.on_event)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cleanup_task

    # 1. Register job handler and start the engine
    engine.set_handler(handle_job)
    engine.start()

    # 2. Pre-create worker pools for every existing service so requests
    #    that come in immediately after startup are routed correctly.
    try:
        for svc in store_list_services():
            sid = str(svc.get("id") or "")
            wc = int(svc.get("worker_count") or 1)
            if sid:
                engine.get_or_create_pool(sid, wc)
    except Exception as exc:
        print(f"[STARTUP] Could not pre-create worker pools: {exc}")

    # 3. Start periodic request cleanup
    _cleanup_task = asyncio.create_task(_request_cleanup_loop())

    yield  # ← app runs here

    # Shutdown
    if _cleanup_task:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
    engine.stop_all()


app = FastAPI(
    title="Traffic Orchestrator",
    version="3.0.0",
    docs_url=None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
# Docs
# ---------------------------------------------------------------------------

@app.get("/docs", include_in_schema=False)
def custom_swagger_ui_html():
    openapi_url = f"{EXTERNAL_API_PREFIX}/openapi.json" if EXTERNAL_API_PREFIX else "/openapi.json"
    oauth2_redirect = f"{EXTERNAL_API_PREFIX}/docs/oauth2-redirect" if EXTERNAL_API_PREFIX else "/docs/oauth2-redirect"
    return get_swagger_ui_html(openapi_url=openapi_url, title=f"{app.title} - Swagger UI", oauth2_redirect_url=oauth2_redirect)


@app.get("/docs/oauth2-redirect", include_in_schema=False)
def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat(),
        "storage_backend": storage_backend_name(),
        "storage_enabled": is_storage_enabled(),
        "engine": engine.stats(),
    }


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------

@app.get("/services")
def api_list_services():
    _require_storage()
    return store_list_services()


@app.post("/services", status_code=201)
def create_service(service: ServiceConfig):
    _require_storage()
    if store_get_service(service.id):
        return JSONResponse(status_code=200, content=jsonable_encoder(store_get_service(service.id)))
    if (dup := _find_duplicate_service_name(service.name)):
        return JSONResponse(status_code=200, content=jsonable_encoder(dup))
    if (dup := _find_duplicate_service(service.url, service.type)):
        return JSONResponse(status_code=200, content=jsonable_encoder(dup))
    payload = service.model_dump()
    upsert_service(payload)
    # Pre-create worker pool for the new service
    engine.get_or_create_pool(service.id, service.worker_count)
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
    if (dup := _find_duplicate_service_name(service.name, exclude_id=service_id)):
        return JSONResponse(status_code=200, content=jsonable_encoder(dup))
    if _find_duplicate_service(service.url, service.type, exclude_id=service_id):
        raise HTTPException(409, "Service with the same URL and type already exists")
    service.id = service_id
    service.created_at = existing.get("created_at", service.created_at)
    payload = service.model_dump()
    upsert_service(payload)
    # Update the engine pool concurrency if worker_count changed
    engine.set_concurrency(service_id, service.worker_count)
    return payload


@app.delete("/services/{service_id}")
def api_delete_service(service_id: str):
    _require_storage()
    if not store_get_service(service_id):
        raise HTTPException(404, "Service not found")
    if not store_delete_service(service_id):
        raise HTTPException(502, "Failed to delete service from Postgres")
    engine.remove_pool(service_id)
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
async def dispatch(req: DispatchRequest):
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
    worker_count = int(service.get("worker_count") or 1)

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

    # Enqueue — delay is handled inside the engine
    task_record = dict(record)
    task_record["delay_seconds"] = 0  # engine handles the delay, not the dispatcher
    await engine.enqueue(
        service_id=req.service_id,
        record=task_record,
        worker_count=worker_count,
        delay=max(0.0, float(effective_delay)),
    )

    return JSONResponse(
        status_code=202,
        content={
            "request_id": request_id,
            "status": "queued",
            "scene_id": record.get("scene_id"),
            "queue_depth": engine.queue_depth(req.service_id),
        },
    )


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

@app.get("/requests")
def api_list_requests(
    service_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
    include_payload: bool = False,
):
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
async def retry_request(request_id: str):
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
        countdown = float(
            (refreshed.get("delay_seconds") if refreshed.get("delay_seconds") is not None
             else (service or {}).get("delay_seconds", 3)) or 0
        )
        worker_count = int((service or {}).get("worker_count") or 1)
        task_record = dict(refreshed)
        task_record["delay_seconds"] = 0
        await engine.enqueue(
            service_id=str(refreshed.get("service_id")),
            record=task_record,
            worker_count=worker_count,
            delay=max(0.0, countdown),
        )
    return {"request_id": request_id, "status": "queued", "updated": True}


@app.delete("/requests/{request_id}")
def api_delete_request(request_id: str):
    _require_storage()
    if not store_delete_request(request_id):
        raise HTTPException(502, "Failed to delete request from Postgres")
    return {"deleted": request_id}


# ---------------------------------------------------------------------------
# Workers / concurrency
# ---------------------------------------------------------------------------

@app.get("/workers")
def get_workers():
    _require_storage()
    services = store_list_services()
    rows = []
    total = 0
    for s in services:
        wc = int(s.get("worker_count") or 1)
        total += wc
        rows.append({
            "service_id": s.get("id"),
            "service_name": s.get("name"),
            "workers": wc,
            "enabled": bool(s.get("enabled", True)),
            "queue_depth": engine.queue_depth(str(s.get("id"))),
        })
    return {
        "mode": "asyncio",
        "total_workers": total,
        "engine_stats": engine.stats(),
        "services": rows,
    }


@app.post("/workers/concurrency")
def set_workers_concurrency(update: WorkerConcurrencyUpdate):
    _require_storage()
    services = store_list_services()
    for s in services:
        s["worker_count"] = update.concurrency
        upsert_service(s)
        engine.set_concurrency(str(s.get("id")), update.concurrency)
    return {"updated": True, "target_concurrency": update.concurrency}


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@app.get("/stats")
def stats():
    _require_storage()
    records = store_list_requests(limit=1000)
    services = store_list_services()
    by_status: dict[str, int] = {}
    by_service: dict[str, int] = {}
    for record in records:
        s = record.get("status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1
        sname = str(record.get("service_name") or record.get("service_id") or "unknown")
        by_service[sname] = by_service.get(sname, 0) + 1
    paused_services = [s.get("id") for s in services if not bool(s.get("enabled", True))]
    return {
        "total_requests": len(records),
        "by_status": by_status,
        "by_service": by_service,
        "paused_services": paused_services,
        "active_services": len(services),
        "engine": engine.stats(),
    }


# ---------------------------------------------------------------------------
# Sink (webhook test endpoint)
# ---------------------------------------------------------------------------

@app.post("/sink")
def sink(payload: Any = Body(...)):
    return {"status": "accepted", "received": payload, "time": datetime.utcnow().isoformat()}


# ---------------------------------------------------------------------------
# Inline dashboard UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    return HTMLResponse(_DASHBOARD_HTML)


_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Traffic Orchestrator</title>
<style>
  :root {
    --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3a;
    --accent: #6366f1; --accent2: #8b5cf6; --text: #e2e8f0;
    --muted: #64748b; --success: #10b981; --warning: #f59e0b;
    --danger: #ef4444; --info: #3b82f6;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, sans-serif; min-height: 100vh; }
  header { background: var(--surface); border-bottom: 1px solid var(--border); padding: 1rem 2rem;
           display: flex; align-items: center; gap: 1rem; position: sticky; top: 0; z-index: 10; }
  header h1 { font-size: 1.2rem; font-weight: 700; background: linear-gradient(135deg, var(--accent), var(--accent2));
              -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
  .badge { font-size: .7rem; padding: .2rem .5rem; border-radius: 9999px; font-weight: 600; }
  .badge-ok { background: #10b98122; color: var(--success); border: 1px solid #10b98144; }
  .badge-err { background: #ef444422; color: var(--danger); border: 1px solid #ef444444; }
  .pill { font-size:.7rem; padding:.15rem .45rem; border-radius:4px; font-weight:600; }
  .pill-success { background:#10b98120; color:var(--success); }
  .pill-failed  { background:#ef444420; color:var(--danger); }
  .pill-running { background:#3b82f620; color:var(--info); }
  .pill-queued  { background:#f59e0b20; color:var(--warning); }
  .pill-paused  { background:#64748b20; color:var(--muted); }
  .pill-cancelled { background:#64748b20; color:var(--muted); }
  main { max-width: 1200px; margin: 0 auto; padding: 2rem; display: flex; flex-direction: column; gap: 1.5rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 1rem; }
  .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 1.25rem; }
  .card-title { font-size: .75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; margin-bottom: .5rem; }
  .card-value { font-size: 2rem; font-weight: 700; }
  .card-value.accent { background: linear-gradient(135deg, var(--accent), var(--accent2));
                       -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
  section h2 { font-size: 1rem; font-weight: 600; margin-bottom: .75rem; color: var(--text); }
  table { width: 100%; border-collapse: collapse; font-size: .85rem; }
  th { text-align: left; padding: .6rem 1rem; color: var(--muted); font-weight: 500;
       font-size: .75rem; text-transform: uppercase; letter-spacing: .05em;
       border-bottom: 1px solid var(--border); }
  td { padding: .65rem 1rem; border-bottom: 1px solid var(--border)20; }
  tr:hover td { background: var(--border)30; }
  .mono { font-family: monospace; font-size: .8rem; color: var(--muted); }
  .refresh { margin-left: auto; font-size: .8rem; color: var(--muted); cursor: pointer;
             background: var(--border); border: none; color: var(--muted); padding: .4rem .8rem;
             border-radius: 6px; cursor: pointer; transition: all .2s; }
  .refresh:hover { background: var(--accent); color: white; }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
  .dot-green { background: var(--success); box-shadow: 0 0 6px var(--success); }
  .dot-red   { background: var(--danger); }
  .err { color: var(--danger); font-size: .8rem; }
  .empty { color: var(--muted); text-align: center; padding: 2rem; font-size: .9rem; }
</style>
</head>
<body>
<header>
  <h1>⚡ Traffic Orchestrator</h1>
  <span id="health-badge" class="badge">...</span>
  <button class="refresh" onclick="loadAll()">↻ Refresh</button>
</header>
<main>
  <div class="grid" id="stats-grid">
    <div class="card"><div class="card-title">Total Requests</div><div class="card-value accent" id="s-total">—</div></div>
    <div class="card"><div class="card-title">Queued</div><div class="card-value" id="s-queued" style="color:var(--warning)">—</div></div>
    <div class="card"><div class="card-title">Running</div><div class="card-value" id="s-running" style="color:var(--info)">—</div></div>
    <div class="card"><div class="card-title">Success</div><div class="card-value" id="s-success" style="color:var(--success)">—</div></div>
    <div class="card"><div class="card-title">Failed</div><div class="card-value" id="s-failed" style="color:var(--danger)">—</div></div>
    <div class="card"><div class="card-title">Services</div><div class="card-value accent" id="s-services">—</div></div>
  </div>

  <section>
    <h2>Services</h2>
    <div class="card" style="padding:0;overflow:auto">
      <table id="services-table">
        <thead><tr><th>Name</th><th>Type</th><th>Workers</th><th>Queue Depth</th><th>Status</th><th>URL</th></tr></thead>
        <tbody id="services-body"><tr><td colspan="6" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>
  </section>

  <section>
    <h2>Recent Requests (last 50)</h2>
    <div class="card" style="padding:0;overflow:auto">
      <table id="requests-table">
        <thead><tr><th>ID</th><th>Service</th><th>Status</th><th>Scene</th><th>Created</th><th>Error</th></tr></thead>
        <tbody id="requests-body"><tr><td colspan="6" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>
  </section>
</main>
<script>
const BASE = '';
async function get(path) {
  const r = await fetch(BASE + path);
  if (!r.ok) throw new Error(r.status);
  return r.json();
}
function pill(status) {
  return `<span class="pill pill-${status||'queued'}">${status||'?'}</span>`;
}
function fmt(iso) {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleString(); } catch { return iso; }
}
function short(s, n=24) { return s ? (s.length > n ? s.slice(0,n)+'…' : s) : '—'; }

async function loadAll() {
  // Health
  try {
    const h = await get('/health');
    const ok = h.status === 'ok';
    const badge = document.getElementById('health-badge');
    badge.textContent = ok ? '● Healthy' : '● Degraded';
    badge.className = 'badge ' + (ok ? 'badge-ok' : 'badge-err');
  } catch { document.getElementById('health-badge').textContent = '● Offline'; }

  // Stats
  try {
    const s = await get('/stats');
    document.getElementById('s-total').textContent    = s.total_requests ?? '—';
    document.getElementById('s-queued').textContent   = (s.by_status||{}).queued  ?? 0;
    document.getElementById('s-running').textContent  = (s.by_status||{}).running ?? 0;
    document.getElementById('s-success').textContent  = (s.by_status||{}).success ?? 0;
    document.getElementById('s-failed').textContent   = (s.by_status||{}).failed  ?? 0;
    document.getElementById('s-services').textContent = s.active_services ?? '—';
  } catch {}

  // Workers / services
  try {
    const w = await get('/workers');
    const tbody = document.getElementById('services-body');
    const svcs = w.services || [];
    if (!svcs.length) { tbody.innerHTML = '<tr><td colspan="6" class="empty">No services configured</td></tr>'; }
    else {
      tbody.innerHTML = svcs.map(s => `
        <tr>
          <td><strong>${s.service_name||'—'}</strong></td>
          <td class="mono">${s.service_name ? '' : ''}—</td>
          <td>${s.workers}</td>
          <td>${s.queue_depth ?? 0}</td>
          <td>${s.enabled ? '<span class="dot dot-green"></span>Active' : '<span class="dot dot-red"></span>Paused'}</td>
          <td class="mono">${short((s.service_id||'').toString())}</td>
        </tr>`).join('');
    }
  } catch(e) {
    document.getElementById('services-body').innerHTML = `<tr><td colspan="6" class="err">Error: ${e.message}</td></tr>`;
  }

  // Requests
  try {
    const reqs = await get('/requests?limit=50');
    const tbody = document.getElementById('requests-body');
    if (!reqs.length) { tbody.innerHTML = '<tr><td colspan="6" class="empty">No requests yet</td></tr>'; }
    else {
      tbody.innerHTML = reqs.map(r => `
        <tr>
          <td class="mono">${short(r.id, 16)}</td>
          <td>${r.service_name || short(r.service_id)}</td>
          <td>${pill(r.status)}</td>
          <td class="mono">${short(String(r.scene_id||''), 14)}</td>
          <td class="mono" style="white-space:nowrap">${fmt(r.created_at)}</td>
          <td class="err">${short(r.error||'', 40)}</td>
        </tr>`).join('');
    }
  } catch(e) {
    document.getElementById('requests-body').innerHTML = `<tr><td colspan="6" class="err">Error: ${e.message}</td></tr>`;
  }
}

loadAll();
setInterval(loadAll, 10000); // auto-refresh every 10s
</script>
</body>
</html>"""
