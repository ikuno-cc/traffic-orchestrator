import os
from urllib.parse import quote, urlsplit, urlunsplit

from celery import Celery


def _normalize_postgres_url(raw_url: str) -> str:
    """Replace every occurrence of the deprecated 'postgres://' scheme with
    'postgresql://' regardless of which prefix (sqla+, db+, bare) is used.
    SQLAlchemy 2.x removed the 'postgres' dialect alias entirely."""
    if not raw_url:
        return raw_url
    raw_url = raw_url.strip().strip('"').strip("'")
    # Handle all known prefix variants so nothing slips through.
    for old, new in [
        ("sqla+postgres://", "sqla+postgresql://"),
        ("db+postgres://", "db+postgresql://"),
        ("postgres://", "postgresql://"),
    ]:
        if raw_url.startswith(old):
            raw_url = new + raw_url[len(old):]
            break
    parts = urlsplit(raw_url)
    if not parts.password:
        return raw_url
    if "%" in parts.password:
        return raw_url
    encoded_password = quote(parts.password, safe="")
    netloc = parts.netloc.replace(f":{parts.password}@", f":{encoded_password}@")
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _ensure_psycopg_driver(url: str, prefix: str) -> str:
    if not url:
        return url
    if url.startswith(f"{prefix}postgresql+psycopg://"):
        return url
    if url.startswith(f"{prefix}postgresql://"):
        return f"{prefix}postgresql+psycopg://{url[len(prefix + 'postgresql://'):]}"
    if url.startswith(f"{prefix}postgres://"):
        return f"{prefix}postgresql+psycopg://{url[len(prefix + 'postgres://'):]}"
    if url.startswith("postgresql://"):
        return f"{prefix}postgresql+psycopg://{url[len('postgresql://'):]}"
    if url.startswith("postgres://"):
        return f"{prefix}postgresql+psycopg://{url[len('postgres://'):]}"
    return url


DATABASE_URL = _normalize_postgres_url(os.getenv("DATABASE_URL", "").strip())
CELERY_BROKER_URL = _normalize_postgres_url(os.getenv("CELERY_BROKER_URL", "").strip())
CELERY_RESULT_BACKEND = _normalize_postgres_url(os.getenv("CELERY_RESULT_BACKEND", "").strip())

broker_url = CELERY_BROKER_URL or (f"sqla+{DATABASE_URL}" if DATABASE_URL else "")
result_backend = CELERY_RESULT_BACKEND or (f"db+{DATABASE_URL}" if DATABASE_URL else "")
# Normalize again after prefix assembly – catches cases where the env var
# was already prefixed with sqla+/db+ but still used the legacy 'postgres://' scheme.
broker_url = _normalize_postgres_url(broker_url)
result_backend = _normalize_postgres_url(result_backend)
broker_url = _ensure_psycopg_driver(broker_url, "sqla+")
result_backend = _ensure_psycopg_driver(result_backend, "db+")

if not broker_url:
    raise RuntimeError("Celery broker is not configured. Set CELERY_BROKER_URL or DATABASE_URL.")

if not result_backend:
    raise RuntimeError("Celery result backend is not configured. Set CELERY_RESULT_BACKEND or DATABASE_URL.")

_worker_concurrency_env = os.getenv("WORKER_CONCURRENCY", "").strip()
_worker_concurrency = int(_worker_concurrency_env) if _worker_concurrency_env.isdigit() else None

celery_app = Celery("traffic_orch", broker=broker_url, backend=result_backend, include=["workers.tasks"])
_conf: dict = dict(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    # Default queue used as a rollout-safe fallback when a worker has not yet
    # subscribed to a service-specific svc.<id> queue.
    task_default_queue="celery",
)
if _worker_concurrency is not None:
    # Honour WORKER_CONCURRENCY env var so the physical process pool size matches
    # the total configured worker_count across all services, not just the CPU count.
    _conf["worker_concurrency"] = _worker_concurrency
celery_app.conf.update(**_conf)
