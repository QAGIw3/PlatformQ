"""
API Dependencies

Common dependencies for API endpoints
"""

from typing import Dict, Any, Optional
from fastapi import Depends, HTTPException, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from ..core.container import Container
from ..core.cdc_manager import CDCManager
from ..core.stream_manager import StreamManager
from ..core.batch_manager import BatchManager
from ..core.catalog_manager import CatalogManager
from ..core.storage_manager import StorageManager
from ..core.lakehouse_manager import LakehouseManager

# Security
security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    x_user_id: Optional[str] = Header(None)
) -> Dict[str, Any]:
    """Get current authenticated user"""
    # TODO: Implement actual authentication logic
    # For now, return a mock user
    return {
        "id": x_user_id or "default-user",
        "email": "user@platform.com",
        "roles": ["user", "data-engineer"]
    }


async def verify_admin_role(
    current_user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Verify user has admin role"""
    if "admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=403,
            detail="Admin role required for this operation"
        )
    return current_user


# Manager dependencies
def get_container() -> Container:
    """Get dependency injection container"""
    return Container()


def get_cdc_manager(
    container: Container = Depends(get_container)
) -> CDCManager:
    """Get CDC manager instance"""
    return container.cdc_manager()


def get_stream_manager(
    container: Container = Depends(get_container)
) -> StreamManager:
    """Get stream manager instance"""
    return container.stream_manager()


def get_batch_manager(
    container: Container = Depends(get_container)
) -> BatchManager:
    """Get batch manager instance"""
    return container.batch_manager()


def get_catalog_manager(
    container: Container = Depends(get_container)
) -> CatalogManager:
    """Get catalog manager instance"""
    return container.catalog_manager()


def get_storage_manager(
    container: Container = Depends(get_container)
) -> StorageManager:
    """Get storage manager instance"""
    return container.storage_manager()


def get_lakehouse_manager(
    container: Container = Depends(get_container)
) -> LakehouseManager:
    """Get lakehouse manager instance"""
    return container.lakehouse_manager()


# Pagination dependencies
class PaginationParams:
    """Common pagination parameters"""
    
    def __init__(
        self,
        page: int = 1,
        page_size: int = 20,
        sort_by: Optional[str] = None,
        sort_order: str = "asc"
    ):
        if page < 1:
            raise HTTPException(
                status_code=400,
                detail="Page number must be >= 1"
            )
        if page_size < 1 or page_size > 100:
            raise HTTPException(
                status_code=400,
                detail="Page size must be between 1 and 100"
            )
        if sort_order not in ["asc", "desc"]:
            raise HTTPException(
                status_code=400,
                detail="Sort order must be 'asc' or 'desc'"
            )
            
        self.page = page
        self.page_size = page_size
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.offset = (page - 1) * page_size


# Filter dependencies
class FilterParams:
    """Common filter parameters"""
    
    def __init__(
        self,
        search: Optional[str] = None,
        status: Optional[str] = None,
        created_after: Optional[str] = None,
        created_before: Optional[str] = None,
        tags: Optional[str] = None
    ):
        self.search = search
        self.status = status
        self.created_after = created_after
        self.created_before = created_before
        self.tags = tags.split(",") if tags else [] 