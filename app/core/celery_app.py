import os

from celery import Celery  # type: ignore
from celery.schedules import crontab  # type: ignore
from celery.signals import worker_process_init, worker_ready  # type: ignore
from prometheus_client import CollectorRegistry, multiprocess, start_http_server

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@rabbitmq:5672//")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")
WORKER_PORT = os.getenv("WORKER_PORT", "8001")
POLL_HOUR = int(os.getenv("POLL_HOUR", "2"))
PROMETHEUS_MULTIPROC_DIR = os.getenv(
    "PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc"
)

# Ensure multiproc dir exists
if not os.path.exists(PROMETHEUS_MULTIPROC_DIR):
    os.makedirs(PROMETHEUS_MULTIPROC_DIR, exist_ok=True)

celery_app = Celery(
    "worker",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=["app.tasks.populate_stop_searches"],
)

celery_app.conf.task_routes = {"app.tasks.*": "main-queue"}


@worker_ready.connect
def start_prometheus_server(sender, **kwargs):
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    start_http_server(int(WORKER_PORT), registry=registry)


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
        "schedule": crontab(hour=POLL_HOUR, minute=0),
    },
}

celery_app.conf.update(timezone="UTC")
