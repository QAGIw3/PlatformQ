"""CloudKitty client for billing integration"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings

logger = logging.getLogger(__name__)


class CloudKittyClient:
    """Client for CloudKitty billing service"""
    
    def __init__(self):
        self.base_url = settings.cloudkitty_url
        self.auth_url = settings.cloudkitty_auth_url
        self.username = settings.cloudkitty_username
        self.password = settings.cloudkitty_password
        self.project_id = settings.cloudkitty_project_id
        self.api_version = settings.cloudkitty_api_version
        
        self.client = httpx.AsyncClient(timeout=30.0)
        self._auth_token = None
        self._token_expiry = None
    
    async def __aenter__(self):
        await self.authenticate()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def authenticate(self):
        """Authenticate with Keystone for CloudKitty access"""
        
        # For dev environment, use mock authentication
        if settings.environment == "development":
            self._auth_token = "mock-dev-token"
            self._token_expiry = datetime.utcnow() + timedelta(hours=1)
            logger.info("Using mock authentication for development")
            return
        
        auth_data = {
            "auth": {
                "identity": {
                    "methods": ["password"],
                    "password": {
                        "user": {
                            "name": self.username,
                            "domain": {"id": "default"},
                            "password": self.password
                        }
                    }
                },
                "scope": {
                    "project": {
                        "id": self.project_id
                    }
                }
            }
        }
        
        try:
            response = await self.client.post(
                f"{self.auth_url}/auth/tokens",
                json=auth_data
            )
            response.raise_for_status()
            
            self._auth_token = response.headers.get("X-Subject-Token")
            self._token_expiry = datetime.utcnow() + timedelta(hours=1)
            
            logger.info("Successfully authenticated with CloudKitty")
        except Exception as e:
            logger.error(f"Failed to authenticate with CloudKitty: {e}")
            raise
    
    async def _ensure_authenticated(self):
        """Ensure we have a valid auth token"""
        if not self._auth_token or datetime.utcnow() >= self._token_expiry:
            await self.authenticate()
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with auth token"""
        return {
            "X-Auth-Token": self._auth_token,
            "Content-Type": "application/json"
        }
    
    async def create_rating_entry(
        self,
        settlement_id: str,
        resource_type: str,
        quantity: float,
        unit_price: float,
        start_time: datetime,
        end_time: datetime,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a rating entry for settlement billing"""
        
        await self._ensure_authenticated()
        
        # For dev environment, return mock data
        if settings.environment == "development":
            return {
                "rating_id": f"mock-rating-{settlement_id}",
                "status": "created",
                "total_cost": quantity * unit_price,
                "currency": "USD"
            }
        
        data = {
            "service": "compute",
            "volume": quantity,
            "rating": {
                "price": unit_price
            },
            "metadata": {
                "settlement_id": settlement_id,
                "resource_type": resource_type,
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                **(metadata or {})
            }
        }
        
        try:
            response = await self.client.post(
                f"{self.base_url}/v{self.api_version}/rating/dataframes",
                json=data,
                headers=self._get_headers()
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create rating entry: {e}")
            raise
    
    async def get_summary(
        self,
        tenant_id: str,
        start_time: datetime,
        end_time: datetime,
        groupby: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get billing summary for a tenant"""
        
        await self._ensure_authenticated()
        
        # For dev environment, return mock data
        if settings.environment == "development":
            return {
                "summary": {
                    "tenant_id": tenant_id,
                    "total": 1234.56,
                    "currency": "USD",
                    "period": {
                        "start": start_time.isoformat(),
                        "end": end_time.isoformat()
                    },
                    "services": {
                        "compute": {
                            "cpu": 500.00,
                            "gpu": 600.00,
                            "memory": 134.56
                        }
                    }
                }
            }
        
        params = {
            "tenant_id": tenant_id,
            "begin": start_time.isoformat(),
            "end": end_time.isoformat()
        }
        
        if groupby:
            params["groupby"] = ",".join(groupby)
        
        try:
            response = await self.client.get(
                f"{self.base_url}/v{self.api_version}/summary",
                params=params,
                headers=self._get_headers()
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get billing summary: {e}")
            raise
    
    async def get_rated_data(
        self,
        settlement_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get rated data for a specific settlement"""
        
        await self._ensure_authenticated()
        
        # For dev environment, return mock data
        if settings.environment == "development":
            return {
                "settlement_id": settlement_id,
                "rated_amount": 100.0,
                "currency": "USD",
                "status": "rated",
                "breakdown": {
                    "base_cost": 80.0,
                    "risk_premium": 10.0,
                    "escrow": 10.0
                }
            }
        
        params = {
            "filters": f"metadata.settlement_id:{settlement_id}"
        }
        
        try:
            response = await self.client.get(
                f"{self.base_url}/v{self.api_version}/storage/dataframes",
                params=params,
                headers=self._get_headers()
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("dataframes"):
                return data["dataframes"][0]
            return None
            
        except Exception as e:
            logger.error(f"Failed to get rated data: {e}")
            raise
    
    async def create_invoice(
        self,
        tenant_id: str,
        settlements: List[str],
        period_start: datetime,
        period_end: datetime
    ) -> Dict[str, Any]:
        """Create an invoice for multiple settlements"""
        
        await self._ensure_authenticated()
        
        # For dev environment, return mock data
        if settings.environment == "development":
            return {
                "invoice_id": f"inv-{tenant_id}-{datetime.utcnow().strftime('%Y%m%d')}",
                "status": "pending",
                "total_amount": 2500.00,
                "currency": "USD",
                "settlements": settlements,
                "created_at": datetime.utcnow().isoformat()
            }
        
        data = {
            "tenant_id": tenant_id,
            "period": {
                "begin": period_start.isoformat(),
                "end": period_end.isoformat()
            },
            "metadata": {
                "settlements": settlements
            }
        }
        
        try:
            response = await self.client.post(
                f"{self.base_url}/v{self.api_version}/invoice",
                json=data,
                headers=self._get_headers()
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create invoice: {e}")
            raise
    
    async def apply_discount(
        self,
        tenant_id: str,
        discount_percentage: float,
        reason: str
    ) -> Dict[str, Any]:
        """Apply a discount to tenant billing"""
        
        await self._ensure_authenticated()
        
        # For dev environment, return mock data
        if settings.environment == "development":
            return {
                "discount_id": f"disc-{tenant_id}-{int(discount_percentage)}",
                "applied": True,
                "percentage": discount_percentage,
                "reason": reason
            }
        
        data = {
            "tenant_id": tenant_id,
            "discount": {
                "type": "percentage",
                "value": discount_percentage,
                "reason": reason
            }
        }
        
        try:
            response = await self.client.post(
                f"{self.base_url}/v{self.api_version}/discounts",
                json=data,
                headers=self._get_headers()
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to apply discount: {e}")
            raise 