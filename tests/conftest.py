import os
import sys
import threading
import time

import httpx
import pytest
import uvicorn
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import Base, get_db
from app.main import app

FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")


@compiles(JSONB, "sqlite")
def compile_jsonb(*args, **kwargs):
    return "JSON"


# Use SQLite for tests to avoid needing a running Postgres instance
DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def db():
    Base.metadata.create_all(bind=engine)
    session = TestingSessionLocal()

    yield session

    session.close()

    Base.metadata.drop_all(bind=engine)


class TestClientWrapper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url)

    def get(self, url, **kwargs):
        return self.client.get(url, **kwargs)

    def post(self, url, **kwargs):
        return self.client.post(url, **kwargs)

    def put(self, url, **kwargs):
        return self.client.put(url, **kwargs)

    def delete(self, url, **kwargs):
        return self.client.delete(url, **kwargs)

    def patch(self, url, **kwargs):
        return self.client.patch(url, **kwargs)


class UvicornThread(threading.Thread):
    def __init__(self, app, port):
        super().__init__()
        self.server = uvicorn.Server(
            config=uvicorn.Config(
                app, host="127.0.0.1", port=port, log_level="critical"
            )
        )
        self.daemon = True

    def run(self):
        self.server.run()

    def stop(self):
        self.server.should_exit = True


@pytest.fixture(scope="session")
def server(worker_id):
    if worker_id == "master":
        port = 8002
    else:
        # gw0 -> 8003, gw1 -> 8004, etc.
        try:
            suffix = int(worker_id.replace("gw", ""))
            port = 8003 + suffix
        except ValueError:
            port = 8002

    thread = UvicornThread(app, port)
    thread.start()

    # Wait for server to start
    max_retries = 50

    for _ in range(max_retries):
        try:
            httpx.get(f"http://127.0.0.1:{port}/health")
            break
        except httpx.ConnectError:
            time.sleep(0.1)
    else:
        raise RuntimeError("Server failed to start")

    yield f"http://127.0.0.1:{port}"

    thread.stop()
    thread.join()


@pytest.fixture(scope="function")
def client(server, db):
    def override_get_db():
        try:
            yield db
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db

    yield TestClientWrapper(server)

    app.dependency_overrides.clear()
