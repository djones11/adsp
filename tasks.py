import json
import os
import socket
import subprocess
import sys
import webbrowser
from typing import Literal, Optional, cast, get_args

import httpx
from dotenv import load_dotenv
from invoke.collection import Collection
from invoke.context import Context
from invoke.tasks import Task, task

load_dotenv()

# Configuration
WEB_PORT = os.getenv("WEB_PORT", "8000")
PROMETHEUS_PORT = os.getenv("PROMETHEUS_PORT", "9090")
GRAFANA_PORT = os.getenv("GRAFANA_PORT", "3000")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", "5672")
RABBITMQ_UI_PORT = os.getenv("RABBITMQ_UI_PORT", "15672")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "adsp")
LOKI_PORT = os.getenv("LOKI_PORT", "3100")
CADVISOR_PORT = os.getenv("CADVISOR_PORT", "8080")

# Define valid services
Service = Literal[
    "db", "rabbitmq", "redis", "web", "worker", "beat", "prometheus", "grafana"
]


def validate_service(service: Optional[str]) -> bool:
    if service and service not in get_args(Service):
        print(
            f"Error: Invalid service '{service}'. "
            f"Must be one of: {', '.join(get_args(Service))}"
        )
        return False

    return True


def run(cmd: str, **kwargs) -> None:
    """Helper function to run a shell command."""
    result = subprocess.run(cmd, shell=True, **kwargs)

    if result.returncode != 0:
        sys.exit(result.returncode)


# --- Root Tasks ---


@task
def grafana(c: Context) -> None:
    """Open Grafana dashboard in the default browser."""

    print("Opening Grafana...")
    webbrowser.open(f"http://localhost:{GRAFANA_PORT}")


@task
def type_check(c: Context) -> None:
    """Run type checking with mypy."""
    print("Running type checking...")
    run("uv run mypy .")


@task
def lint(c: Context) -> None:
    """Run linting and type checking."""
    print("Running linting...")
    run("uv run ruff check .")
    type_check(c)


@task
def format(c: Context) -> None:
    """Run formatting and import sorting."""
    print("Running formatting...")
    run("uv run ruff check --select I --fix .")
    run("uv run ruff format .")


@task
def security(c: Context) -> None:
    """Run security checks using bandit."""
    print("Running security checks...")
    run("uv run bandit -lll -r app/")


# --- Web Tasks ---


@task
def test(c: Context, path: Optional[str] = None) -> None:
    """Run tests inside the container."""
    print("Running tests...")

    cmd = "docker compose exec web pytest"

    if path:
        cmd += f" tests/{path}"

        # If testing a specific file, disable test parallelization
        if path.endswith(".py"):
            cmd += " -n 0"
        else:
            cmd += " -n auto"
    else:
        cmd += " -n auto"

    run(cmd)


@task
def trigger_stop_search_ingestion(
    c: Context,
) -> None:
    """Manual trigger of the daily data population job via script."""
    print("Manually triggering daily data population job ...")
    cmd = "docker compose exec web python scripts/trigger_stop_search_ingestion.py"
    run(cmd)


@task
def remediate_failed_rows(c: Context) -> None:
    """Manual trigger of the remediation job via script."""
    print("Manually triggering remediation...")
    run("docker compose exec web python scripts/trigger_remediation.py")


@task(
    help={
        "force": "Filter by police force",
        "start": "Filter by start date (YYYY-MM-DD)",
        "end": "Filter by end date (YYYY-MM-DD)",
        "page": "Page number",
        "limit": "Items per page",
    }
)
def get_stop_searches(
    c: Context,
    force: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    page: int = 1,
    limit: int = 50,
) -> None:
    """
    Retrieve stop and search data from the API.
    Example of usage of the stop-searches endpoint.
    """
    url = f"http://localhost:{WEB_PORT}/v1/stop-searches"
    params = [f"page={page}", f"page_size={limit}"]

    if force:
        params.append(f"force={force}")
    if start:
        params.append(f"date_start={start}")
    if end:
        params.append(f"date_end={end}")

    full_url = f"{url}?{'&'.join(params)}"
    print(f"Fetching: {full_url}")

    try:
        response = httpx.get(full_url)
        response.raise_for_status()
        data = response.json()

        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"Error fetching data: {e}")
        if "response" in locals():
            print(response.text)


# --- Docker Tasks ---


@task(
    help={
        "build": "Build images before starting",
        "local": "Enable hot reload for worker",
    }
)
def up(c: Context, build: bool = False, local: bool = False) -> None:
    """
    Start all services in detached mode.
    Use --build to force rebuild. Use --local for hot reload.
    """
    print("Starting services...")
    cmd = "docker compose -f docker-compose.yml"

    if local:
        print("Enabling local development mode (hot reload)...")
        cmd += " -f docker-compose.dev.yml"

    cmd += " up -d"

    if build:
        cmd += " --build"

    run(cmd)


@task
def down(c: Context) -> None:
    """Stop all services."""
    print("Stopping services...")
    run("docker compose down")


@task
def verify(c: Context) -> None:
    """Verify that all services are running and accessible."""
    print("Verifying endpoints...")

    def check_tcp(host: str, port: int) -> bool:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            return False

    endpoints = [
        ("API", f"http://localhost:{WEB_PORT}/health"),
        ("Prometheus", f"http://localhost:{PROMETHEUS_PORT}/-/healthy"),
        ("Grafana", f"http://localhost:{GRAFANA_PORT}/api/health"),
        ("Postgres", ("localhost", int(POSTGRES_PORT))),
        ("RabbitMQ", f"http://localhost:{RABBITMQ_UI_PORT}"),
        ("Redis", ("localhost", int(REDIS_PORT))),
        ("Loki", f"http://localhost:{LOKI_PORT}/ready"),
    ]

    for name, target in endpoints:
        try:
            if isinstance(target, tuple):
                host, port = target
                status = "✅ UP" if check_tcp(host, port) else "❌ DOWN"
                url = f"tcp://{host}:{port}"
            else:
                url = str(target)
                response = httpx.get(url, timeout=1)
                status_code = response.status_code
                status = "✅ UP" if status_code < 400 else f"⚠️  Status {status_code}"

            print(f"{name:<20} {url:<40} {status}")
        except Exception as e:
            # Handle tuple unpacking for error message if target is tuple
            url_str = (
                f"tcp://{target[0]}:{target[1]}"
                if isinstance(target, tuple)
                else target
            )
            print(f"{name:<20} {url_str:<40} ❌ DOWN ({e})")


@task(help={"service": "Service name to restart (e.g. web, worker)"})
def restart(c: Context, service: Optional[Service] = None) -> None:
    """Restart all services or a specific service."""
    if service and not validate_service(service):
        return

    print(f"Restarting {'service ' + service if service else 'all services'}...")
    cmd = "docker compose restart"

    if service:
        cmd += f" {service}"

    run(cmd)


# --- Database Tasks ---


@task
def migrate(c: Context) -> None:
    """Apply database migrations."""
    print("Applying migrations...")
    run("docker compose exec web alembic upgrade head")


@task(help={"message": "Migration message"})
def make_migrations(c: Context, message: str = "New migration") -> None:
    """Create a new migration revision."""
    print(f"Creating migration: {message}")
    run(f'docker compose exec web alembic revision --autogenerate -m "{message}"')


@task
def shell(c: Context) -> None:
    """Open a psql shell to the database."""
    print("Opening psql shell...")
    run(f"docker compose exec db psql -U {POSTGRES_USER} -d {POSTGRES_DB}")


@task(help={"command": "SQL command to execute"})
def run_sql(c: Context, command: str) -> None:
    """Execute a SQL command on the database."""
    # Escape double quotes in the command for the shell
    escaped_command = command.replace('"', '\\"')
    cmd = (
        f"docker compose exec db psql -U {POSTGRES_USER} "
        f'-d {POSTGRES_DB} -c "{escaped_command}"'
    )

    run(cmd)


# --- Log Tasks ---


@task(help={"service": "Service name to filter logs (e.g. web, worker)"})
def view(c: Context, service: Optional[Service] = None) -> None:
    """Follow logs for all services or a specific service."""
    if not validate_service(service):
        return

    cmd = "docker compose logs -f"

    if service:
        cmd += f" {service}"

    run(cmd)


@task(
    help={
        "output": "Output file path",
        "since": "Show logs since timestamp (e.g. 2013-01-02T13:23:37Z) "
        "or relative (e.g. 42m for 42 minutes)",
        "until": "Show logs before a timestamp (e.g. 2013-01-02T13:23:37Z) "
        "or relative (e.g. 42m for 42 minutes)",
        "service": "Service name to filter logs (e.g. web, worker)",
    }
)
def export(
    c: Context,
    output: str = "services.log",
    since: Optional[str] = None,
    until: Optional[str] = None,
    service: Optional[Service] = None,
) -> None:
    """Export logs to a file."""
    if not validate_service(service):
        return

    print(f"Exporting logs to {output}...")
    cmd = "docker compose logs"

    if since:
        cmd += f" --since {since}"

    if until:
        cmd += f" --until {until}"

    if service:
        cmd += f" {service}"

    with open(output, "w") as f:
        run(cmd, stdout=f)


# --- Collection Setup ---

ns = Collection()

# Root tasks
ns.add_task(cast(Task, grafana))
ns.add_task(cast(Task, lint))
ns.add_task(cast(Task, type_check))
ns.add_task(cast(Task, format))
ns.add_task(cast(Task, security))

# Add web tasks
web_ns = Collection("web")
web_ns.add_task(cast(Task, test))
web_ns.add_task(cast(Task, trigger_stop_search_ingestion))
web_ns.add_task(cast(Task, remediate_failed_rows))
web_ns.add_task(cast(Task, get_stop_searches))
ns.add_collection(web_ns)

# Add docker tasks
docker_ns = Collection("docker")
docker_ns.add_task(cast(Task, up))
docker_ns.add_task(cast(Task, down))
docker_ns.add_task(cast(Task, verify))
docker_ns.add_task(cast(Task, restart))
ns.add_collection(docker_ns)

# Add database tasks under 'db' namespace
db_ns = Collection("db")
db_ns.add_task(cast(Task, migrate))
db_ns.add_task(cast(Task, make_migrations))
db_ns.add_task(cast(Task, shell))
db_ns.add_task(cast(Task, run_sql), name="run")

ns.add_collection(db_ns)

# Add log tasks under 'logs' namespace
logs_ns = Collection("logs")
logs_ns.add_task(cast(Task, view))
logs_ns.add_task(cast(Task, export))
ns.add_collection(logs_ns)
