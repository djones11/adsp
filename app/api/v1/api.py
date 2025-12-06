from fastapi import APIRouter

from app.api.v1.endpoints import stop_searches

api_router = APIRouter()

api_router.include_router(
    stop_searches.router, prefix="/stop-searches", tags=["stop-searches"]
)
