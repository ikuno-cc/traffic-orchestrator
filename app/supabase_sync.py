import json
import os
import time
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote_plus

import requests

PROJECT_REF = os.getenv("SUPABASE_PROJECT_REF", "rwvoskxjwobvukmujpcr")
BASE_URL = os.getenv("SUPABASE_URL", f"https://{PROJECT_REF}.supabase.co").rstrip("/")
API_KEY = os.getenv("SUPABASE_SECRET_KEY")
SERVICES_TABLE = os.getenv("SUPABASE_SERVICES_TABLE", "orch_services")
REQUESTS_TABLE = os.getenv("SUPABASE_REQUESTS_TABLE", "orch_requests")


def is_supabase_enabled() -> bool:
    return bool(API_KEY and BASE_URL)


def _headers(write: bool = False) -> dict[str, str]:
    headers = {
        "apikey": API_KEY or "",
        "Authorization": f"Bearer {API_KEY or ''}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if write:
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
    return headers


def _request(method: str, path: str, allow_error: bool = False, **kwargs):
    if not is_supabase_enabled():
        return None
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.request(method, f"{BASE_URL}/rest/v1/{path}", timeout=15, **kwargs)
            if resp.status_code >= 400:
                print(f"[SUPABASE] {method} {path} failed {resp.status_code}: {resp.text[:500]}")
                if resp.status_code >= 500 and attempt < attempts:
                    time.sleep(0.4 * attempt)
                    continue
                if not allow_error:
                    return None
            return resp
        except Exception as exc:
            if attempt < attempts:
                time.sleep(0.4 * attempt)
                continue
            print(f"[SUPABASE] Operation failed: {exc}")
            return None


def _request_row_to_record(row: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {}
    raw_info = row.get("info")
    if isinstance(raw_info, str) and raw_info.strip():
        try:
            parsed = json.loads(raw_info)
            if isinstance(parsed, dict):
                record.update(parsed)
        except Exception:
            record["info"] = raw_info

    record["id"] = row.get("id")
    record["service_id"] = row.get("service_id")
    record["status"] = row.get("status")
    record["priority"] = row.get("priority", 5)
    if row.get("created_at"):
        record["created_at"] = row["created_at"]
    return record


def fetch_services_from_supabase() -> list[dict[str, Any]]:
    resp = _request(
        "GET",
        f"{SERVICES_TABLE}?select=id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,created_at&order=created_at.desc",
        headers=_headers(),
    )
    if resp is None:
        return []

    rows = resp.json()
    services: list[dict[str, Any]] = []
    for row in rows:
        services.append(
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "type": row.get("type"),
                "url": row.get("endpoint"),
                "description": row.get("description") or "",
                "timeout": int(row.get("timeout") or 120),
                "enabled": bool(row.get("enabled", True)),
                "headers": row.get("custom_header") or {},
                "delay_seconds": float(row.get("delay_seconds", 3) or 3),
                "created_at": row.get("created_at"),
            }
        )
    return services


def fetch_service_from_supabase(service_id: str) -> Optional[dict[str, Any]]:
    sid = quote_plus(service_id)
    resp = _request(
        "GET",
        f"{SERVICES_TABLE}?select=id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,created_at&id=eq.{sid}&limit=1",
        headers=_headers(),
    )
    if resp is None:
        return None
    rows = resp.json()
    if not rows:
        return None
    row = rows[0]
    return {
        "id": row.get("id"),
        "name": row.get("name"),
        "type": row.get("type"),
        "url": row.get("endpoint"),
        "description": row.get("description") or "",
        "timeout": int(row.get("timeout") or 120),
        "enabled": bool(row.get("enabled", True)),
        "headers": row.get("custom_header") or {},
        "delay_seconds": float(row.get("delay_seconds", 3) or 3),
        "created_at": row.get("created_at"),
    }


def sync_service_to_supabase(service: dict[str, Any]) -> None:
    payload = {
        "id": service.get("id"),
        "name": service.get("name"),
        "type": service.get("type"),
        "endpoint": service.get("url"),
        "description": service.get("description") or "",
        "timeout": int(service.get("timeout", 120)),
        "delay_seconds": float(service.get("delay_seconds", 3) or 3),
        "enabled": bool(service.get("enabled", True)),
        "custom_header": service.get("headers", {}),
        "created_at": service.get("created_at"),
    }
    resp = _request(
        "POST",
        f"{SERVICES_TABLE}",
        headers=_headers(write=True),
        data=json.dumps([payload]),
        allow_error=True,
    )
    if resp is not None and resp.status_code == 400 and "delay_seconds" in (resp.text or ""):
        # Backward-compatible fallback for older schemas that do not yet have delay_seconds.
        payload_compat = dict(payload)
        payload_compat.pop("delay_seconds", None)
        resp = _request(
            "POST",
            f"{SERVICES_TABLE}",
            headers=_headers(write=True),
            data=json.dumps([payload_compat]),
            allow_error=True,
        )

    if resp is not None and resp.status_code == 409:
        sid = quote_plus(str(service.get("id")))
        _request(
            "PATCH",
            f"{SERVICES_TABLE}?id=eq.{sid}",
            headers=_headers(write=True),
            data=json.dumps(payload),
            allow_error=True,
        )


def delete_service_from_supabase(service_id: str) -> bool:
    sid = quote_plus(service_id)
    resp = _request("DELETE", f"{SERVICES_TABLE}?id=eq.{sid}", headers=_headers(), allow_error=True)
    return resp is not None and resp.status_code < 400


def sync_request_to_supabase(record: dict[str, Any]) -> None:
    created_at = record.get("created_at") or record.get("updated_at") or datetime.utcnow().isoformat()
    payload = {
        "id": record.get("id"),
        "service_id": record.get("service_id"),
        "status": record.get("status"),
        "info": json.dumps(record, default=str),
        "priority": int(record.get("priority", 5)),
        "duration": record.get("duration"),
        "created_at": created_at,
    }
    resp = _request(
        "POST",
        f"{REQUESTS_TABLE}",
        headers=_headers(write=True),
        data=json.dumps([payload]),
        allow_error=True,
    )
    if resp is not None and resp.status_code == 409:
        rid = quote_plus(str(record.get("id")))
        _request(
            "PATCH",
            f"{REQUESTS_TABLE}?id=eq.{rid}",
            headers=_headers(write=True),
            data=json.dumps(payload),
            allow_error=True,
        )


def fetch_request_from_supabase(request_id: str) -> Optional[dict[str, Any]]:
    rid = quote_plus(request_id)
    resp = _request(
        "GET",
        f"{REQUESTS_TABLE}?select=id,service_id,status,info,priority,duration,created_at&id=eq."
        + rid
        + "&limit=1",
        headers=_headers(),
    )
    if resp is None:
        return None
    rows = resp.json()
    if not rows:
        return None
    return _request_row_to_record(rows[0])


def fetch_requests_from_supabase(
    service_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100
) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 1000))
    query = f"{REQUESTS_TABLE}?select=id,service_id,status,info,priority,duration,created_at"
    if service_id:
        query += f"&service_id=eq.{quote_plus(service_id)}"
    if status:
        query += f"&status=eq.{quote_plus(status)}"
    query += f"&order=created_at.desc&limit={limit}"
    resp = _request("GET", query, headers=_headers())
    if resp is None:
        return []
    return [_request_row_to_record(row) for row in resp.json()]


def delete_request_from_supabase(request_id: str) -> bool:
    rid = quote_plus(request_id)
    resp = _request("DELETE", f"{REQUESTS_TABLE}?id=eq.{rid}", headers=_headers(), allow_error=True)
    return resp is not None and resp.status_code < 400
