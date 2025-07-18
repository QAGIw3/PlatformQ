"""
MLOps Service Client

Client for integrating with the MLOps service for model training compute requirements.
"""

import httpx
import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MLOpsClient:
    """Client for MLOps service integration"""
    
    def __init__(self, base_url: str = "http://mlops-service:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url, timeout=30.0)
        
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()
        
    async def get_training_requirements(
        self,
        tenant_id: str,
        model_name: str,
        training_type: str = "full"
    ) -> Dict[str, Any]:
        """Get compute requirements for model training"""
        try:
            response = await self.client.get(
                f"/api/v1/models/training-requirements",
                params={
                    "model_name": model_name,
                    "training_type": training_type
                },
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get training requirements: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting training requirements: {e}")
            return None
            
    async def notify_compute_reserved(
        self,
        tenant_id: str,
        reservation_id: str,
        gpu_type: str,
        gpu_count: int,
        start_time: datetime,
        duration_hours: int
    ) -> bool:
        """Notify MLOps that compute has been reserved"""
        try:
            response = await self.client.post(
                "/api/v1/compute/reservation-confirmed",
                json={
                    "reservation_id": reservation_id,
                    "gpu_type": gpu_type,
                    "gpu_count": gpu_count,
                    "start_time": start_time.isoformat(),
                    "duration_hours": duration_hours
                },
                headers={"X-Tenant-ID": tenant_id}
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Error notifying compute reservation: {e}")
            return False
            
    async def get_scheduled_training_jobs(
        self,
        tenant_id: str,
        days_ahead: int = 7
    ) -> List[Dict[str, Any]]:
        """Get scheduled training jobs for capacity planning"""
        try:
            response = await self.client.get(
                f"/api/v1/training/scheduled",
                params={
                    "days_ahead": days_ahead
                },
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 200:
                return response.json().get("jobs", [])
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error getting scheduled jobs: {e}")
            return []
            
    async def estimate_training_cost(
        self,
        model_type: str,
        dataset_size_gb: float,
        gpu_type: str
    ) -> Optional[Dict[str, Any]]:
        """Estimate training cost for a model"""
        try:
            response = await self.client.post(
                "/api/v1/models/estimate-cost",
                json={
                    "model_type": model_type,
                    "dataset_size_gb": dataset_size_gb,
                    "gpu_type": gpu_type
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error estimating training cost: {e}")
            return None 