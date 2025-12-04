FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir .[dev]

# Copy application code
COPY . .

# Make scripts executable
RUN chmod +x scripts/*.py

ENV PYTHONPATH=/app
