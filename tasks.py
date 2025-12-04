import os
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
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "adsp")

WEB_ENDPOINT = f"http://localhost:{WEB_PORT}"

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
    result = subprocess.run("uv run mypy .", shell=True)
    if result.returncode != 0:
        sys.exit(result.returncode)


@task
def lint(c: Context) -> None:
    """Run linting and type checking."""
    print("Running linting...")
    result = subprocess.run("uv run ruff check .", shell=True)

    if result.returncode != 0:
        sys.exit(result.returncode)

    type_check(c)


@task
def format(c: Context) -> None:
    """Run formatting and import sorting."""
    print("Running formatting...")
    subprocess.run("uv run ruff check --select I --fix .", shell=True, check=True)
    subprocess.run("uv run ruff format .", shell=True, check=True)


# --- Web Tasks ---


@task
def test(c: Context) -> None:
    """Run tests inside the container."""
    print("Running tests...")

    result = subprocess.run("docker compose exec web pytest", shell=True)

    if result.returncode != 0:
        sys.exit(result.returncode)


@task(help={"date": "Date in YYYY-MM format"})
def trigger_populate_stop_searches(c: Context, date: Optional[str] = None) -> None:
    """Manual trigger of the daily data population job via script."""
    print(f"Manually triggering daily data population job (date={date})...")
    cmd = "docker compose exec web python scripts/trigger_populate_stop_searches.py"

    if date:
        cmd += f" --date {date}"
    
    subprocess.run(
        cmd,
        shell=True,
        check=True,
    )


@task
def remediate_failed_rows(c: Context) -> None:
    """Manual trigger of the remediation job via script."""
    print("Manually triggering remediation...")
    subprocess.run(
        "docker compose exec web python scripts/trigger_remediation.py",
        shell=True,
        check=True,
    )


# --- Docker Tasks ---


@task(help={"build": "Build images before starting", "local": "Enable hot reload for worker"})
def up(c: Context, build: bool = False, local: bool = False) -> None:
    """Start all services in detached mode. Use --build to force rebuild. Use --local for hot reload."""
    print("Starting services...")
    cmd = "docker compose -f docker-compose.yml"

    if local:
        print("Enabling local development mode (hot reload)...")
        cmd += " -f docker-compose.dev.yml"

    cmd += " up -d"

    if build:
        cmd += " --build"

    subprocess.run(cmd, shell=True, check=True)


@task
def down(c: Context) -> None:
    """Stop all services."""
    print("Stopping services...")
    subprocess.run("docker compose down", shell=True, check=True)


@task
def verify(c: Context) -> None:
    """Verify that all services are running and accessible."""
    print("Verifying endpoints...")

    endpoints = [
        ("API", f"http://localhost:{WEB_PORT}/health"),
        ("Prometheus", f"http://localhost:{PROMETHEUS_PORT}"),
        ("Grafana", f"http://localhost:{GRAFANA_PORT}"),
    ]

    for name, url in endpoints:
        try:
            response = httpx.get(url)
            status_code = response.status_code
            status = "✅ UP" if status_code < 400 else f"⚠️  Status {status_code}"
            print(f"{name:<15} {url:<30} {status}")
        except Exception as e:
            print(f"{name:<15} {url:<30} ❌ DOWN ({e})")


# --- Database Tasks ---


@task
def migrate(c: Context) -> None:
    """Apply database migrations."""
    print("Applying migrations...")
    subprocess.run(
        "docker compose exec web alembic upgrade head", shell=True, check=True
    )


@task(help={"message": "Migration message"})
def make_migrations(c: Context, message: str = "New migration") -> None:
    """Create a new migration revision."""
    print(f"Creating migration: {message}")
    subprocess.run(
        f'docker compose exec web alembic revision --autogenerate -m "{message}"',
        shell=True,
        check=True,
    )


@task
def init(c: Context) -> None:
    """Load initial data into the database."""
    print("Loading initial data...")
    subprocess.run(
        "docker compose exec web python scripts/initial_load.py data/sample_data.csv",
        shell=True,
        check=True,
    )


@task
def shell(c: Context) -> None:
    """Open a psql shell to the database."""
    subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "db",
            "psql",
            "-U",
            POSTGRES_USER,
            "-d",
            POSTGRES_DB,
        ]
    )


@task(help={"command": "SQL command to execute"})
def run_sql(c: Context, command: str) -> None:
    """Execute a SQL command on the database."""
    # Escape double quotes in the command for the shell
    escaped_command = command.replace('"', '\\"')
    cmd = (
        f"docker compose exec db psql -U {POSTGRES_USER} "
        f'-d {POSTGRES_DB} -c "{escaped_command}"'
    )

    result = subprocess.run(cmd, shell=True)

    if result.returncode != 0:
        sys.exit(result.returncode)


# --- Log Tasks ---


@task(help={"service": "Service name to filter logs (e.g. web, worker)"})
def view(c: Context, service: Optional[Service] = None, tail: Optional[int] = None) -> None:
    """Follow logs for all services or a specific service."""
    if not validate_service(service):
        return

    cmd = "docker compose logs -f"

    if service:
        cmd += f" {service}"

    if tail:
        cmd += f" --tail {tail}"

    subprocess.run(cmd, shell=True, check=True)


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
        subprocess.run(cmd, shell=True, check=True, stdout=f)


# --- Collection Setup ---

ns = Collection()

# Root tasks
ns.add_task(cast(Task, grafana))
ns.add_task(cast(Task, lint))
ns.add_task(cast(Task, type_check))
ns.add_task(cast(Task, format))

# Add web tasks
web_ns = Collection("web")
web_ns.add_task(cast(Task, test))
web_ns.add_task(cast(Task, trigger_populate_stop_searches))
web_ns.add_task(cast(Task, remediate_failed_rows))
ns.add_collection(web_ns)

# Add docker tasks
docker_ns = Collection("docker")
docker_ns.add_task(cast(Task, up))
docker_ns.add_task(cast(Task, down))
docker_ns.add_task(cast(Task, verify))
ns.add_collection(docker_ns)

# Add database tasks under 'db' namespace
db_ns = Collection("db")
db_ns.add_task(cast(Task, migrate))
db_ns.add_task(cast(Task, make_migrations))
db_ns.add_task(cast(Task, init))
db_ns.add_task(cast(Task, shell))
db_ns.add_task(cast(Task, run_sql), name="run")

ns.add_collection(db_ns)

# Add log tasks under 'logs' namespace
logs_ns = Collection("logs")
logs_ns.add_task(cast(Task, view))
logs_ns.add_task(cast(Task, export))
ns.add_collection(logs_ns)
