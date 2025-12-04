# ADSP Project

This is a backend project built with FastAPI, Celery, PostgreSQL, Redis, and RabbitMQ. It handles daily data processing with robust error handling and monitoring.

## Features

- **FastAPI**: REST API for triggering jobs and health checks.
- **Celery**: Distributed task queue for background processing.
- **PostgreSQL**: Relational database for storage.
- **Redis**: Result backend for Celery.
- **RabbitMQ**: Message broker for Celery.
- **Prometheus & Grafana**: Monitoring stack.
- **Error Handling**: Failed rows during import are saved to a separate table (`failed_rows`) without stopping the process.
- **Performance**: Uses PostgreSQL `COPY` command for high-speed bulk inserts.

## Architecture Decisions

### FastAPI
- **Why**: Chosen for its high performance (Starlette-based), native asynchronous support, and automatic generation of OpenAPI documentation.
- **Pros**: Faster execution than Flask/Django, strict type checking with Pydantic reduces runtime errors, and modern Python type hint support.
- **Cons**: Smaller ecosystem of plugins compared to Django, and not as feature rich.

### Celery & RabbitMQ
- **Why**: We needed a robust, distributed task queue for heavy background processing. RabbitMQ is the industry standard broker for reliability.
- **Pros**: Celery handles retries, scheduling (Beat), and worker management out of the box. RabbitMQ ensures message persistence better than Redis as a broker.
- **Cons**: Adds infrastructure complexity (requires a broker and worker processes) compared to simple `asyncio` tasks or Python threads.

### PostgreSQL
- **Why**: Selected for data integrity, ACID compliance, and specific features like `COPY` for bulk loading.
- **Pros**: Extremely reliable, handles complex queries and relationships well, and has a rich ecosystem.
- **Cons**: Heavier resource usage than NoSQL options for simple key-value needs, but necessary for structured relational data.

### Redis
- **Why**: Used primarily as the Result Backend for Celery to store task states and return values quickly.
- **Pros**: In-memory speed makes it ideal for caching and temporary state storage.
- **Cons**: Data persistence requires configuration; not suitable as a primary database for critical relational data in this context.

### Prometheus & Grafana
- **Why**: To provide real-time visibility into system health and processing metrics.
- **Pros**: Prometheus uses a pull model ideal for microservices; Grafana provides powerful visualization and alerting capabilities.
- **Cons**: Adds two additional services to the stack, increasing the memory footprint and operational complexity.

## Setup

1. **Prerequisites**: Ensure Docker and Docker Compose are installed. Python 3.10+ is recommended for local management.

2. **Install Management Tools**:
   Install `invoke` and other dev dependencies to manage the project easily.
   ```bash
   pip install -e .[dev]
   ```

3. **Start Services**:
   ```bash
   invoke up
   ```
   *Or manually:* `docker compose up -d`

4. **Run Migrations**:
   ```bash
   invoke db.migrate
   ```
   *Or manually:* `docker compose exec web alembic upgrade head`

5. **Initial Data Load**:
   ```bash
   invoke db.init
   ```
   *Or manually:* `docker compose exec web python scripts/initial_load.py data/sample_data.csv`

## Management Commands (Invoke)

This project uses `invoke` to manage common tasks. Run `invoke --list` to see all available commands.

- `invoke up`: Start services (use `--build` to force rebuild).
- `invoke down`: Stop services.
- `invoke logs`: View logs.
- `invoke test`: Run tests.
- `invoke trigger`: Trigger the background job.
- `invoke verify`: Check health of endpoints.

### Database Commands
- `invoke db.migrate`: Apply migrations.
- `invoke db.makemigrations --message "..."`: Create a new migration.
- `invoke db.init`: Load initial data.
- `invoke db.shell`: Open a psql shell.
- `invoke db.run --command "SELECT * FROM items"`: Run a SQL command.

## Usage

- **API**: Access the API documentation at `http://localhost:8000/docs`.
- **Trigger Job**: POST to `http://localhost:8000/trigger-job` to manually start the daily task.
- **Monitoring**:
  - Prometheus: `http://localhost:9090`
  - Grafana: `http://localhost:3000` (Default login: admin/admin)

## Project Structure

- `app/`: Main application code.
  - `core/`: Configuration and Celery app.
  - `db/`: Database session and base models.
  - `models/`: SQLAlchemy models.
  - `schemas/`: Pydantic schemas.
  - `services/`: Business logic (Importer).
  - `tasks/`: Celery tasks.
- `docker/`: Docker configuration files.
- `scripts/`: Utility scripts.
- `alembic/`: Database migrations.
