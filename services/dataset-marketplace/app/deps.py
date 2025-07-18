"""
Dependencies Module

FastAPI dependency injection functions
"""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
import logging

from platformq_shared.auth import verify_token
from platformq_shared.database import get_db
from sqlalchemy.orm import Session

from app.models import DatasetListing
from app.repository import DatasetListingRepository

logger = logging.getLogger(__name__)

# Security
security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    """Get current authenticated user from token"""
    try:
        token = credentials.credentials
        user = verify_token(token)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token"
            )
        return user
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )


def get_trust_engine():
    """Get trust-weighted data engine instance"""
    from app.main import trust_engine
    if not trust_engine:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Trust engine not initialized"
        )
    return trust_engine


def get_seatunnel_integration():
    """Get SeaTunnel integration instance"""
    from app.main import seatunnel_integration
    if not seatunnel_integration:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="SeaTunnel integration not initialized"
        )
    return seatunnel_integration


def get_graph_intelligence():
    """Get graph intelligence integration instance"""
    from app.main import graph_intelligence
    if not graph_intelligence:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph intelligence not initialized"
        )
    return graph_intelligence


async def verify_dataset_owner(
    dataset_id: str,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
) -> DatasetListing:
    """Verify that the current user owns the dataset"""
    repo = DatasetListingRepository(db)
    dataset = repo.get_by_listing_id(dataset_id)
    
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )
    
    if dataset.seller_id != current_user["user_id"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to access this dataset"
        )
    
    return dataset


async def verify_dataset_access(
    dataset_id: str,
    current_user: dict = Depends(get_current_user),
    trust_engine = Depends(get_trust_engine)
) -> bool:
    """Verify that the user has access to the dataset"""
    # Check if user has purchased or has sufficient trust score
    has_access = await trust_engine.check_user_access(
        user_id=current_user["user_id"],
        dataset_id=dataset_id
    )
    
    if not has_access:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions or trust score to access this dataset"
        )
    
    return True


def get_monitoring_metrics():
    """Get monitoring metrics instance"""
    from app.monitoring import metrics
    return metrics 