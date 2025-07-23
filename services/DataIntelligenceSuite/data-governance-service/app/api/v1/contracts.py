"""
Data Contracts API endpoints
"""
from fastapi import APIRouter, Depends
from data_intelligence_common.core.api.response_models import SuccessResponse

from ...core.container import Container
from ..dependencies import get_container, get_current_user


router = APIRouter()


@router.get("/contracts")
async def list_contracts(
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """List data contracts"""
    return SuccessResponse(
        message="Contracts retrieved",
        data={"contracts": []}
    ) 