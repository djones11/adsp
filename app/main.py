from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.v1.api import api_router
from app.core.config import settings

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.PROJECT_VERSION,
)

app.include_router(api_router, prefix=settings.V1_STR)

Instrumentator().instrument(app).expose(app)


@app.get("/")
def read_root():
    return {"message": "Welcome to the ADSP Project API"}


@app.get("/health")
def health_check():
    return {"status": "ok"}
