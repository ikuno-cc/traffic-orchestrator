import os
from urllib.parse import quote, urlsplit, urlunsplit

from celery import Celery


def _normalize_postgres_url(raw_url: str) -> str:
    if not raw_url:
        return raw_url
    raw_url = raw_url.strip().strip('"').strip("'")
    if raw_url.startswith("sqla+postgres://"):
        raw_url = "sqla+postgresql://" + raw_url[len("sqla+postgres://") :]
    elif raw_url.startswith("db+postgres://"):
        raw_url = "db+postgresql://" + raw_url[len("db+postgres://") :]
    elif raw_url.startswith("postgres://"):
        raw_url = "postgresql://" + raw_url[len("postgres://") :]
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
    return url


DATABASE_URL = _normalize_postgres_url(os.getenv("DATABASE_URL", "").strip())
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "").strip()
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "").strip()

broker_url = CELERY_BROKER_URL or (f"sqla+{DATABASE_URL}" if DATABASE_URL else "")
result_backend = CELERY_RESULT_BACKEND or (f"db+{DATABASE_URL}" if DATABASE_URL else "")
broker_url = _ensure_psycopg_driver(broker_url, "sqla+")
result_backend = _ensure_psycopg_driver(result_backend, "db+")

if not broker_url:
    raise RuntimeError("Celery broker is not configured. Set CELERY_BROKER_URL or DATABASE_URL.")

if not result_backend:
    raise RuntimeError("Celery result backend is not configured. Set CELERY_RESULT_BACKEND or DATABASE_URL.")

celery_app = Celery("traffic_orch", broker=broker_url, backend=result_backend, include=["workers.tasks"])
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)
