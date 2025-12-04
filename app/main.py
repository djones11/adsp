import os
from datetime import date
from typing import Optional

from fastapi import FastAPI, Query
from prometheus_fastapi_instrumentator import Instrumentator

from app.db.session import SessionLocal
from app.models.stop_search import StopSearch
from app.schemas.stop_search import PaginatedResponse
from app.schemas.stop_search import StopSearch as StopSearchSchema

app = FastAPI(
    title=os.getenv("PROJECT_NAME", "ADSP Project"),
    version=os.getenv("PROJECT_VERSION", "0.1.0"),
)

Instrumentator().instrument(app).expose(app)


def get_db():
    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()


@app.get("/")
def read_root():
    return {"message": "Welcome to the ADSP Project API"}


@app.get("/v1/stop-searches", response_model=PaginatedResponse[StopSearchSchema])
def get_stop_searches(
    date_start: Optional[date] = Query(
        None, description="Filter by start date (inclusive)"
    ),
    date_end: Optional[date] = Query(
        None, description="Filter by end date (inclusive)"
    ),
    force: Optional[str] = Query(None, description="Filter by police force"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=1000, description="Items per page"),
):
    """
    Retrieve Stop and Search data with optional filtering and pagination.
    """
    with SessionLocal() as db:
        query = db.query(StopSearch)

        if date_start:
            query = query.filter(StopSearch.datetime >= date_start)
        if date_end:
            query = query.filter(StopSearch.datetime <= date_end)
        if force:
            query = query.filter(StopSearch.force == force)

        total = query.count()

        offset = (page - 1) * page_size
        data = query.offset(offset).limit(page_size).all()

        return {"total": total, "page": page, "page_size": page_size, "data": data}


@app.get("/health")
def health_check():
    return {"status": "ok"}
