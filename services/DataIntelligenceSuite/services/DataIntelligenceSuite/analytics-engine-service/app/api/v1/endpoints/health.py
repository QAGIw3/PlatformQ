"""
Health endpoint
"""

from fastapi import APIRouter, Depends
from dependency_injector.wiring import inject, Provide

from ....core.container import Container
from ....services.health import HealthChecker

router = APIRouter()


@router.get("/")
@inject
async def health_status(
    health_checker: HealthChecker = Depends(Provide[Container.health_checker])
):
    """Get health status"""
    return await health_checker.get_status()
