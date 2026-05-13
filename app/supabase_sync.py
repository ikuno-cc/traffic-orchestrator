import json
import os
import time
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote, quote_plus, urlsplit, urlunsplit

import requests

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

PROJECT_REF = os.getenv("SUPABASE_PROJECT_REF", "rwvoskxjwobvukmujpcr")
BASE_URL = os.getenv("SUPABASE_URL", f"https://{PROJECT_REF}.supabase.co").rstrip("/")
API_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    or os.getenv("SUPABASE_API_KEY")
    or os.getenv("SUPABASE_SECRET_KEY")
)
SERVICES_TABLE = os.getenv("SUPABASE_SERVICES_TABLE", "orch_services")
REQUESTS_TABLE = os.getenv("SUPABASE_REQUESTS_TABLE", "orch_requests")
SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA", "public")

DATA_BACKEND = os.getenv("DATA_BACKEND", "supabase").strip().lower()
def _normalize_database_url(raw_url: str) -> str:
    if not raw_url:
        return raw_url
    raw_url = raw_url.strip().strip('"').strip("'")
    if raw_url.startswith("postgres://"):
        raw_url = "postgresql://" + raw_url[len("postgres://") :]
    parts = urlsplit(raw_url)
    if not parts.password or "%" in parts.password:
        return raw_url
    encoded_password = quote(parts.password, safe="")
    netloc = parts.netloc.replace(f":{parts.password}@", f":{encoded_password}@")
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


DATABASE_URL = _normalize_database_url(os.getenv("DATABASE_URL", ""))


def storage_backend_name() -> str:
    return "postgres" if DATA_BACKEND == "postgres" else "supabase"


def is_supabase_enabled() -> bool:
    return storage_backend_name() == "supabase" and bool(API_KEY and BASE_URL)


def is_postgres_enabled() -> bool:
    return storage_backend_name() == "postgres" and bool(DATABASE_URL) and psycopg is not None


def is_storage_enabled() -> bool:
    if storage_backend_name() == "postgres":
        return is_postgres_enabled()
    return is_supabase_enabled()


def _headers(write: bool = False) -> dict[str, str]:
    headers = {
        "apikey": API_KEY or "",
        "Authorization": f"Bearer {API_KEY or ''}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if SUPABASE_SCHEMA:
        headers["Accept-Profile"] = SUPABASE_SCHEMA
        headers["Content-Profile"] = SUPABASE_SCHEMA
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


def _pg_conn():
    if not is_postgres_enabled():
        return None
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def _request_row_to_record(row: dict[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {}
    raw_info = row.get("info")
    if isinstance(raw_info, dict):
        record.update(raw_info)
    elif isinstance(raw_info, str) and raw_info.strip():
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


def _service_row_to_service(row: dict[str, Any]) -> dict[str, Any]:
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
        "worker_count": int(row.get("worker_count") or 1),
        "created_at": row.get("created_at"),
    }


def fetch_services_from_supabase() -> list[dict[str, Any]]:
    if is_postgres_enabled():
        return _fetch_services_from_postgres()

    query = (
        f"{SERVICES_TABLE}?select=id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,worker_count,created_at"
        "&order=created_at.desc"
    )
    resp = _request("GET", query, headers=_headers(), allow_error=True)
    if resp is None:
        return []
    if resp.status_code == 400 and "worker_count" in (resp.text or ""):
        resp = _request(
            "GET",
            f"{SERVICES_TABLE}?select=id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,created_at&order=created_at.desc",
            headers=_headers(),
        )
        if resp is None:
            return []

    return [_service_row_to_service(row) for row in resp.json()]


def _fetch_services_from_postgres() -> list[dict[str, Any]]:
    conn = _pg_conn()
    if conn is None:
        return []
    with conn:
        with conn.cursor() as cur:
            table = f'"{SUPABASE_SCHEMA}"."{SERVICES_TABLE}"'
            q = f"SELECT id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,worker_count,created_at FROM {table} ORDER BY created_at DESC"
            try:
                cur.execute(q)
            except Exception as exc:
                if "worker_count" in str(exc):
                    cur.execute(
                        f"SELECT id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,created_at FROM {table} ORDER BY created_at DESC"
                    )
                else:
                    raise
            rows = cur.fetchall()
    return [_service_row_to_service(row) for row in rows]


def fetch_service_from_supabase(service_id: str) -> Optional[dict[str, Any]]:
    if is_postgres_enabled():
        return _fetch_service_from_postgres(service_id)

    sid = quote_plus(service_id)
    resp = _request(
        "GET",
        f"{SERVICES_TABLE}?select=id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,worker_count,created_at&id=eq.{sid}&limit=1",
        headers=_headers(),
        allow_error=True,
    )
    if resp is None:
        return None
    if resp.status_code == 400 and "worker_count" in (resp.text or ""):
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
    return _service_row_to_service(rows[0])


def _fetch_service_from_postgres(service_id: str) -> Optional[dict[str, Any]]:
    conn = _pg_conn()
    if conn is None:
        return None
    with conn:
        with conn.cursor() as cur:
            table = f'"{SUPABASE_SCHEMA}"."{SERVICES_TABLE}"'
            try:
                cur.execute(
                    f"SELECT id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,worker_count,created_at FROM {table} WHERE id=%s LIMIT 1",
                    (service_id,),
                )
            except Exception as exc:
                if "worker_count" in str(exc):
                    cur.execute(
                        f"SELECT id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,created_at FROM {table} WHERE id=%s LIMIT 1",
                        (service_id,),
                    )
                else:
                    raise
            row = cur.fetchone()
    return _service_row_to_service(row) if row else None


def sync_service_to_supabase(service: dict[str, Any]) -> None:
    if is_postgres_enabled():
        _sync_service_to_postgres(service)
        return

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
        "worker_count": int(service.get("worker_count", 1) or 1),
        "created_at": service.get("created_at"),
    }
    resp = _request("POST", f"{SERVICES_TABLE}", headers=_headers(write=True), data=json.dumps([payload]), allow_error=True)
    if resp is not None and resp.status_code == 400 and "delay_seconds" in (resp.text or ""):
        payload_compat = dict(payload)
        payload_compat.pop("delay_seconds", None)
        resp = _request("POST", f"{SERVICES_TABLE}", headers=_headers(write=True), data=json.dumps([payload_compat]), allow_error=True)
    if resp is not None and resp.status_code == 400 and "worker_count" in (resp.text or ""):
        payload_compat = dict(payload)
        payload_compat.pop("worker_count", None)
        resp = _request("POST", f"{SERVICES_TABLE}", headers=_headers(write=True), data=json.dumps([payload_compat]), allow_error=True)

    if resp is not None and resp.status_code == 409:
        sid = quote_plus(str(service.get("id")))
        _request("PATCH", f"{SERVICES_TABLE}?id=eq.{sid}", headers=_headers(write=True), data=json.dumps(payload), allow_error=True)


def _sync_service_to_postgres(service: dict[str, Any]) -> None:
    conn = _pg_conn()
    if conn is None:
        return
    table = f'"{SUPABASE_SCHEMA}"."{SERVICES_TABLE}"'
    payload = {
        "id": service.get("id"),
        "name": service.get("name"),
        "type": service.get("type"),
        "endpoint": service.get("url"),
        "description": service.get("description") or "",
        "timeout": int(service.get("timeout", 120)),
        "delay_seconds": float(service.get("delay_seconds", 3) or 3),
        "enabled": bool(service.get("enabled", True)),
        "custom_header": json.dumps(service.get("headers", {})),
        "worker_count": int(service.get("worker_count", 1) or 1),
        "created_at": service.get("created_at") or datetime.utcnow().isoformat(),
    }
    with conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    INSERT INTO {table}
                    (id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,worker_count,created_at)
                    VALUES (%(id)s,%(name)s,%(type)s,%(endpoint)s,%(description)s,%(timeout)s,%(delay_seconds)s,%(enabled)s,%(custom_header)s::jsonb,%(worker_count)s,%(created_at)s)
                    ON CONFLICT (id) DO UPDATE SET
                      name=EXCLUDED.name,
                      type=EXCLUDED.type,
                      endpoint=EXCLUDED.endpoint,
                      description=EXCLUDED.description,
                      timeout=EXCLUDED.timeout,
                      delay_seconds=EXCLUDED.delay_seconds,
                      enabled=EXCLUDED.enabled,
                      custom_header=EXCLUDED.custom_header,
                      worker_count=EXCLUDED.worker_count
                    """,
                    payload,
                )
            except Exception as exc:
                if "worker_count" in str(exc):
                    cur.execute(
                        f"""
                        INSERT INTO {table}
                        (id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,created_at)
                        VALUES (%(id)s,%(name)s,%(type)s,%(endpoint)s,%(description)s,%(timeout)s,%(delay_seconds)s,%(enabled)s,%(custom_header)s::jsonb,%(created_at)s)
                        ON CONFLICT (id) DO UPDATE SET
                          name=EXCLUDED.name,
                          type=EXCLUDED.type,
                          endpoint=EXCLUDED.endpoint,
                          description=EXCLUDED.description,
                          timeout=EXCLUDED.timeout,
                          delay_seconds=EXCLUDED.delay_seconds,
                          enabled=EXCLUDED.enabled,
                          custom_header=EXCLUDED.custom_header
                        """,
                        payload,
                    )
                else:
                    raise


def delete_service_from_supabase(service_id: str) -> bool:
    if is_postgres_enabled():
        conn = _pg_conn()
        if conn is None:
            return False
        with conn:
            with conn.cursor() as cur:
                table = f'"{SUPABASE_SCHEMA}"."{SERVICES_TABLE}"'
                cur.execute(f"DELETE FROM {table} WHERE id=%s", (service_id,))
                return cur.rowcount > 0

    sid = quote_plus(service_id)
    resp = _request("DELETE", f"{SERVICES_TABLE}?id=eq.{sid}", headers=_headers(), allow_error=True)
    return resp is not None and resp.status_code < 400


def sync_request_to_supabase(record: dict[str, Any]) -> None:
    if is_postgres_enabled():
        _sync_request_to_postgres(record)
        return

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
    resp = _request("POST", f"{REQUESTS_TABLE}", headers=_headers(write=True), data=json.dumps([payload]), allow_error=True)
    if resp is not None and resp.status_code == 409:
        rid = quote_plus(str(record.get("id")))
        _request("PATCH", f"{REQUESTS_TABLE}?id=eq.{rid}", headers=_headers(write=True), data=json.dumps(payload), allow_error=True)


def _sync_request_to_postgres(record: dict[str, Any]) -> None:
    conn = _pg_conn()
    if conn is None:
        return
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
    table = f'"{SUPABASE_SCHEMA}"."{REQUESTS_TABLE}"'
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table} (id,service_id,status,info,priority,duration,created_at)
                VALUES (%(id)s,%(service_id)s,%(status)s,%(info)s::jsonb,%(priority)s,%(duration)s,%(created_at)s)
                ON CONFLICT (id) DO UPDATE SET
                  service_id=EXCLUDED.service_id,
                  status=EXCLUDED.status,
                  info=EXCLUDED.info,
                  priority=EXCLUDED.priority,
                  duration=EXCLUDED.duration
                """,
                payload,
            )


def fetch_request_from_supabase(request_id: str) -> Optional[dict[str, Any]]:
    if is_postgres_enabled():
        conn = _pg_conn()
        if conn is None:
            return None
        with conn:
            with conn.cursor() as cur:
                table = f'"{SUPABASE_SCHEMA}"."{REQUESTS_TABLE}"'
                cur.execute(
                    f"SELECT id,service_id,status,info,priority,duration,created_at FROM {table} WHERE id=%s LIMIT 1",
                    (request_id,),
                )
                row = cur.fetchone()
        return _request_row_to_record(row) if row else None

    rid = quote_plus(request_id)
    resp = _request(
        "GET",
        f"{REQUESTS_TABLE}?select=id,service_id,status,info,priority,duration,created_at&id=eq." + rid + "&limit=1",
        headers=_headers(),
    )
    if resp is None:
        return None
    rows = resp.json()
    if not rows:
        return None
    return _request_row_to_record(rows[0])


def fetch_requests_from_supabase(service_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> list[dict[str, Any]]:
    if is_postgres_enabled():
        limit = max(1, min(limit, 1000))
        conn = _pg_conn()
        if conn is None:
            return []
        with conn:
            with conn.cursor() as cur:
                table = f'"{SUPABASE_SCHEMA}"."{REQUESTS_TABLE}"'
                q = f"SELECT id,service_id,status,info,priority,duration,created_at FROM {table}"
                where = []
                params: list[Any] = []
                if service_id:
                    where.append("service_id=%s")
                    params.append(service_id)
                if status:
                    where.append("status=%s")
                    params.append(status)
                if where:
                    q += " WHERE " + " AND ".join(where)
                q += " ORDER BY created_at DESC LIMIT %s"
                params.append(limit)
                cur.execute(q, tuple(params))
                rows = cur.fetchall()
        return [_request_row_to_record(row) for row in rows]

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
    if is_postgres_enabled():
        conn = _pg_conn()
        if conn is None:
            return False
        with conn:
            with conn.cursor() as cur:
                table = f'"{SUPABASE_SCHEMA}"."{REQUESTS_TABLE}"'
                cur.execute(f"DELETE FROM {table} WHERE id=%s", (request_id,))
                return cur.rowcount > 0

    rid = quote_plus(request_id)
    resp = _request("DELETE", f"{REQUESTS_TABLE}?id=eq.{rid}", headers=_headers(), allow_error=True)
    return resp is not None and resp.status_code < 400


def update_request_fields(request_id: str, updates: dict[str, Any]) -> bool:
    if is_postgres_enabled():
        conn = _pg_conn()
        if conn is None:
            return False
        with conn:
            with conn.cursor() as cur:
                table = f'"{SUPABASE_SCHEMA}"."{REQUESTS_TABLE}"'
                cur.execute(f"SELECT info,status,priority,duration FROM {table} WHERE id=%s LIMIT 1", (request_id,))
                row = cur.fetchone()
                if not row:
                    return False
                info = row.get("info") or {}
                if isinstance(info, str):
                    try:
                        info = json.loads(info)
                    except Exception:
                        info = {}
                if not isinstance(info, dict):
                    info = {}
                info.update(updates)
                status = updates.get("status", row.get("status"))
                priority = int(updates.get("priority", row.get("priority") or 5))
                duration = updates.get("duration", row.get("duration"))
                cur.execute(
                    f"UPDATE {table} SET status=%s, info=%s::jsonb, priority=%s, duration=%s WHERE id=%s",
                    (status, json.dumps(info, default=str), priority, duration, request_id),
                )
                return cur.rowcount > 0

    rid = quote_plus(request_id)
    resp = _request(
        "PATCH",
        f"{REQUESTS_TABLE}?id=eq.{rid}",
        headers=_headers(write=True),
        data=json.dumps(updates),
        allow_error=True,
    )
    return resp is not None and resp.status_code < 400


def claim_next_queued_request(service_id: str) -> Optional[dict[str, Any]]:
    if is_postgres_enabled():
        conn = _pg_conn()
        if conn is None:
            return None
        with conn:
            with conn.cursor() as cur:
                table = f'"{SUPABASE_SCHEMA}"."{REQUESTS_TABLE}"'
                cur.execute(
                    f"""
                    SELECT id,service_id,status,info,priority,duration,created_at
                    FROM {table}
                    WHERE service_id=%s AND status='queued'
                    ORDER BY priority ASC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    (service_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                record = _request_row_to_record(row)
                record["status"] = "running"
                record["updated_at"] = datetime.utcnow().isoformat()
                cur.execute(
                    f"UPDATE {table} SET status='running', info=%s::jsonb WHERE id=%s",
                    (json.dumps(record, default=str), row["id"]),
                )
                return record

    sid = quote_plus(service_id)
    resp = _request(
        "GET",
        f"{REQUESTS_TABLE}?select=id,service_id,status,info,priority,duration,created_at&service_id=eq.{sid}&status=eq.queued&order=priority.asc,created_at.asc&limit=1",
        headers=_headers(),
    )
    if resp is None:
        return None
    rows = resp.json()
    if not rows:
        return None

    row = rows[0]
    rid = quote_plus(str(row.get("id")))
    claim_resp = _request(
        "PATCH",
        f"{REQUESTS_TABLE}?id=eq.{rid}&status=eq.queued",
        headers={**_headers(write=True), "Prefer": "return=representation"},
        data=json.dumps({"status": "running"}),
        allow_error=True,
    )
    if claim_resp is None or claim_resp.status_code >= 400:
        return None
    claimed_rows = claim_resp.json() if claim_resp.text else []
    if not claimed_rows:
        return None
    return _request_row_to_record(claimed_rows[0])
