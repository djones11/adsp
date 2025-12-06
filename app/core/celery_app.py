import os

from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_process_init, worker_ready
from prometheus_client import (
    CollectorRegistry,
    ProcessCollector,
    multiprocess,
    start_http_server,
)

from app.core.config import settings

# Ensure multiproc dir exists
if not os.path.exists(settings.PROMETHEUS_MULTIPROC_DIR):
    os.makedirs(settings.PROMETHEUS_MULTIPROC_DIR, exist_ok=True)

celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["app.tasks.stop_search_tasks"],
)

celery_app.conf.task_routes = {"app.tasks.*": "main-queue"}


@worker_ready.connect
def start_prometheus_server(sender, **kwargs):
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    ProcessCollector(registry=registry)
    start_http_server(settings.WORKER_PORT, registry=registry)


@worker_process_init.connect
def init_worker_process(*args, **kwargs):
    pass


# Schedule: Run at the scheduled hour each day
celery_app.conf.beat_schedule = {
    "daily-processing-task": {
        "task": "app.tasks.stop_search_tasks.ingest_stop_searches",
        "schedule": crontab(hour=settings.POLL_HOUR, minute=0),
    },
}

celery_app.conf.update(timezone="UTC")
