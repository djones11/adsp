import os

from celery import Celery  # type: ignore
from celery.schedules import crontab  # type: ignore
from celery.signals import worker_process_init, worker_ready  # type: ignore
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
    include=["app.tasks.populate_stop_searches"],
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
    # This hook is called when a worker process is initialized.
    # We don't need to do anything specific for prometheus_client here
    # as it automatically detects the env var, but it's good practice
    # to ensure the dir exists (done above).
    pass


# Schedule: Run at the scheduled hour each day
celery_app.conf.beat_schedule = {
    "daily-processing-task": {
        "task": "app.tasks.populate_stop_searches.populate_stop_searches",
        "schedule": crontab(hour=settings.POLL_HOUR, minute=0),
    },
}

celery_app.conf.update(timezone="UTC")
