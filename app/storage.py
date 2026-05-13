import json
import os
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote, urlsplit, urlunsplit

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None


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
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")
SERVICES_TABLE = os.getenv("SERVICES_TABLE", "orch_services")
REQUESTS_TABLE = os.getenv("REQUESTS_TABLE", "orch_requests")


def storage_backend_name() -> str:
    return "postgres"


def is_storage_enabled() -> bool:
    return bool(DATABASE_URL) and psycopg is not None


def _pg_conn():
    if not is_storage_enabled():
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


def list_services() -> list[dict[str, Any]]:
    conn = _pg_conn()
    if conn is None:
        return []
    with conn:
        with conn.cursor() as cur:
            table = f'"{PG_SCHEMA}"."{SERVICES_TABLE}"'
            try:
                cur.execute(
                    f"SELECT id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,worker_count,created_at FROM {table} ORDER BY created_at DESC"
                )
            except Exception as exc:
                if "worker_count" in str(exc):
                    cur.execute(
                        f"SELECT id,name,type,endpoint,description,timeout,delay_seconds,enabled,custom_header,created_at FROM {table} ORDER BY created_at DESC"
                    )
                else:
                    raise
            return [_service_row_to_service(row) for row in cur.fetchall()]


def get_service(service_id: str) -> Optional[dict[str, Any]]:
    conn = _pg_conn()
    if conn is None:
        return None
    with conn:
        with conn.cursor() as cur:
            table = f'"{PG_SCHEMA}"."{SERVICES_TABLE}"'
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


def upsert_service(service: dict[str, Any]) -> None:
    conn = _pg_conn()
    if conn is None:
        return
    table = f'"{PG_SCHEMA}"."{SERVICES_TABLE}"'
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
                      name=EXCLUDED.name, type=EXCLUDED.type, endpoint=EXCLUDED.endpoint, description=EXCLUDED.description,
                      timeout=EXCLUDED.timeout, delay_seconds=EXCLUDED.delay_seconds, enabled=EXCLUDED.enabled,
                      custom_header=EXCLUDED.custom_header, worker_count=EXCLUDED.worker_count
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
                          name=EXCLUDED.name, type=EXCLUDED.type, endpoint=EXCLUDED.endpoint, description=EXCLUDED.description,
                          timeout=EXCLUDED.timeout, delay_seconds=EXCLUDED.delay_seconds, enabled=EXCLUDED.enabled,
                          custom_header=EXCLUDED.custom_header
                        """,
                        payload,
                    )
                else:
                    raise


def delete_service(service_id: str) -> bool:
    conn = _pg_conn()
    if conn is None:
        return False
    with conn:
        with conn.cursor() as cur:
            table = f'"{PG_SCHEMA}"."{SERVICES_TABLE}"'
            cur.execute(f"DELETE FROM {table} WHERE id=%s", (service_id,))
            return cur.rowcount > 0


def upsert_request(record: dict[str, Any]) -> None:
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
    table = f'"{PG_SCHEMA}"."{REQUESTS_TABLE}"'
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table} (id,service_id,status,info,priority,duration,created_at)
                VALUES (%(id)s,%(service_id)s,%(status)s,%(info)s::jsonb,%(priority)s,%(duration)s,%(created_at)s)
                ON CONFLICT (id) DO UPDATE SET
                  service_id=EXCLUDED.service_id, status=EXCLUDED.status, info=EXCLUDED.info,
                  priority=EXCLUDED.priority, duration=EXCLUDED.duration
                """,
                payload,
            )


def get_request(request_id: str) -> Optional[dict[str, Any]]:
    conn = _pg_conn()
    if conn is None:
        return None
    with conn:
        with conn.cursor() as cur:
            table = f'"{PG_SCHEMA}"."{REQUESTS_TABLE}"'
            cur.execute(
                f"SELECT id,service_id,status,info,priority,duration,created_at FROM {table} WHERE id=%s LIMIT 1",
                (request_id,),
            )
            row = cur.fetchone()
    return _request_row_to_record(row) if row else None


def list_requests(service_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 1000))
    conn = _pg_conn()
    if conn is None:
        return []
    with conn:
        with conn.cursor() as cur:
            table = f'"{PG_SCHEMA}"."{REQUESTS_TABLE}"'
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
            return [_request_row_to_record(row) for row in cur.fetchall()]


def delete_request(request_id: str) -> bool:
    conn = _pg_conn()
    if conn is None:
        return False
    with conn:
        with conn.cursor() as cur:
            table = f'"{PG_SCHEMA}"."{REQUESTS_TABLE}"'
            cur.execute(f"DELETE FROM {table} WHERE id=%s", (request_id,))
            return cur.rowcount > 0


def update_request_fields(request_id: str, updates: dict[str, Any]) -> bool:
    conn = _pg_conn()
    if conn is None:
        return False
    with conn:
        with conn.cursor() as cur:
            table = f'"{PG_SCHEMA}"."{REQUESTS_TABLE}"'
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


def claim_next_queued_request(service_id: str) -> Optional[dict[str, Any]]:
    conn = _pg_conn()
    if conn is None:
        return None
    with conn:
        with conn.cursor() as cur:
            table = f'"{PG_SCHEMA}"."{REQUESTS_TABLE}"'
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


def delete_completed_requests_older_than(hours: float, statuses: Optional[list[str]] = None) -> int:
    conn = _pg_conn()
    if conn is None:
        return 0
    statuses = statuses or ["success", "failed", "cancelled"]
    if not statuses:
        return 0
    table = f'"{PG_SCHEMA}"."{REQUESTS_TABLE}"'
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE status = ANY(%s) AND created_at < (NOW() - (%s * INTERVAL '1 hour'))",
                (statuses, float(hours)),
            )
            return int(cur.rowcount or 0)


def try_start_request_with_service_limit(request_id: str, service_id: str, max_running: int) -> bool:
    """
    Atomically mark request as running if current running count for the same service
    is below max_running. Uses advisory transaction lock per service to serialize
    the check+update critical section.
    """
    conn = _pg_conn()
    if conn is None:
        return False
    max_running = max(1, int(max_running))
    table = f'"{PG_SCHEMA}"."{REQUESTS_TABLE}"'
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS locked", (f"svc:{service_id}",))
            row = cur.fetchone() or {}
            if not bool(row.get("locked")):
                return False

            cur.execute(
                f"SELECT COUNT(*) AS c FROM {table} WHERE service_id=%s AND status='running'",
                (service_id,),
            )
            running_count = int((cur.fetchone() or {}).get("c") or 0)
            if running_count >= max_running:
                return False

            cur.execute(
                f"SELECT info FROM {table} WHERE id=%s LIMIT 1",
                (request_id,),
            )
            req_row = cur.fetchone()
            if not req_row:
                return False

            info = req_row.get("info") or {}
            if isinstance(info, str):
                try:
                    info = json.loads(info)
                except Exception:
                    info = {}
            if not isinstance(info, dict):
                info = {}
            info["status"] = "running"
            info["updated_at"] = datetime.utcnow().isoformat()

            cur.execute(
                f"UPDATE {table} SET status='running', info=%s::jsonb WHERE id=%s AND status IN ('queued','retrying')",
                (json.dumps(info, default=str), request_id),
            )
            return int(cur.rowcount or 0) > 0
