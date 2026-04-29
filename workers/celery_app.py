from celery import Celery
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

celery_app = Celery(
    "orchestrator",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["workers.tasks"],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_acks_late=False,
    worker_prefetch_multiplier=1,
    task_queue_max_priority=10,
    task_default_priority=5,
    broker_transport_options={"priority_steps": list(range(10))},
    broker_connection_retry_on_startup=True,
    result_expires=86400,  # 24h
)
