"""
Compliance API endpoints
"""
from fastapi import APIRouter, Depends
from data_intelligence_common.core.api.response_models import SuccessResponse

from ...core.container import Container
from ..dependencies import get_container, get_current_user


router = APIRouter()


@router.get("/reports")
async def list_compliance_reports(
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """List compliance reports"""
    return SuccessResponse(
        message="Reports retrieved",
        data={"reports": []}
    ) 