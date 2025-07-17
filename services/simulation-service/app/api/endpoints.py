from fastapi import APIRouter, Depends
from cassandra.cluster import Session
from .deps import get_current_tenant_and_user

# Import multi-physics endpoints
from .endpoints import multi_physics

router = APIRouter()

# Include multi-physics routes
router.include_router(multi_physics.router, prefix="/multi-physics", tags=["multi-physics"])

@router.get("/example")
def example_endpoint(context: dict = Depends(get_current_tenant_and_user)):
    """
    An example protected endpoint.
    """
    tenant_id = context["tenant_id"]
    user = context["user"]
    
    return {
        "message": f"Hello {user.full_name} from tenant {tenant_id}!",
        "service": "simulation-service"
    } 