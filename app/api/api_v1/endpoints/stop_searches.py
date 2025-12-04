from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.session import SessionLocal, get_db
from app.models.stop_search import StopSearch
from app.schemas.stop_search import PaginatedResponse
from app.schemas.stop_search import StopSearch as StopSearchSchema

router = APIRouter()


@router.get("/", response_model=PaginatedResponse[StopSearchSchema])
def get_stop_searches(
    db: Session = Depends(get_db),
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
    query = db.query(StopSearch)

    if date_start:
        query = query.filter(StopSearch.datetime >= date_start)
    if date_end:
        query = query.filter(StopSearch.datetime <= date_end)
    if force:
        query = query.filter(StopSearch.force == force)

    total = query.count()
    data = query.offset((page - 1) * page_size).limit(page_size).all()

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": data,
    }
