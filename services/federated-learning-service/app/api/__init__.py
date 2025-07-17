from fastapi import APIRouter
from .v1 import sessions

api_router = APIRouter()
api_router.include_router(sessions.router, prefix="/sessions", tags=["sessions"]) 