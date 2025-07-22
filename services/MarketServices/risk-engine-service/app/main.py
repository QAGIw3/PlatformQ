"""Risk Engine Service - Real-time risk assessment and management."""

import logging
from typing import Dict, Any
from contextlib import asynccontextmanager
from fastapi import Depends

from platformq_shared import (
    PlatformQService,
    ServiceConfig,
    UnifiedMonitoring,
    get_current_user,
    get_current_trader,
    require_roles,
    monitor_operation
)

from .config import Settings
from .api import risk, margin, var, stress, limits, monitoring
from .api.direct import router as direct_router
from .dependencies import initialize_dependencies, cleanup_dependencies

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RiskEngineService(PlatformQService):
    """Risk Engine Service with real-time risk calculations"""
    
    def __init__(self):
        # Initialize settings
        settings = Settings()
        
        # Create service config
        config = ServiceConfig(
            service_name="risk-engine-service",
            service_id=settings.service_id,
            version="2.0.0",
            port=settings.SERVICE_PORT,
            metrics_port=settings.METRICS_PORT,
            enable_auth=True,
            enable_monitoring=True,
            enable_tracing=True,
            enable_health_check=True,
            consul_enabled=settings.CONSUL_ENABLED,
            consul_host=settings.CONSUL_HOST,
            consul_port=settings.CONSUL_PORT,
            vault_enabled=True,
            vault_addr=settings.VAULT_ADDR,
            vault_role=settings.VAULT_ROLE
        )
        
        # Initialize base service
        super().__init__(config)
        
        self.settings = settings
        
    async def startup(self):
        """Custom startup logic"""
        logger.info("Starting Risk Engine Service...")
        
        # Initialize dependencies
        await initialize_dependencies()
        
        # Call parent startup
        await super().startup()
        
        logger.info("Risk Engine Service started successfully")
        
    async def shutdown(self):
        """Custom shutdown logic"""
        logger.info("Shutting down Risk Engine Service...")
        
        # Cleanup dependencies
        await cleanup_dependencies()
        
        # Call parent shutdown
        await super().shutdown()
        
        logger.info("Risk Engine Service stopped")
        
    def setup_routes(self):
        """Setup API routes"""
        # Include all API routers
        self.app.include_router(risk.router)
        self.app.include_router(margin.router)
        self.app.include_router(var.router)
        self.app.include_router(stress.router)
        self.app.include_router(limits.router)
        self.app.include_router(monitoring.router)
        self.app.include_router(direct_router)
        
        # Add custom endpoints
        @self.app.get("/", tags=["root"])
        async def root():
            """Root endpoint"""
            return {
                "service": "Risk Engine Service",
                "version": "2.0.0",
                "status": "running",
                "features": [
                    "Real-time risk calculations",
                    "ML-based risk predictions",
                    "Margin management",
                    "VaR calculations",
                    "Stress testing",
                    "Position limit management",
                    "Direct communication support",
                    "Real-time monitoring",
                    "Liquidation probability prediction"
                ],
                "endpoints": {
                    "risk": "/api/v1/risk",
                    "margin": "/api/v1/margin",
                    "var": "/api/v1/var",
                    "stress": "/api/v1/stress",
                    "limits": "/api/v1/limits",
                    "monitoring": "/api/v1/monitoring",
                    "direct": "/api/v1/direct",
                    "websocket": "/api/v1/monitoring/ws/{user_id}"
                }
            }
        
        @self.app.get("/api/v1/status", tags=["status"])
        @monitor_operation("service_status")
        async def get_status(current_user: Dict = Depends(get_current_user)):
            """Get service status"""
            return {
                "service": "Risk Engine Service",
                "status": "healthy",
                "authenticated_user": current_user.get("user_id"),
                "monitoring": {
                    "active": True,
                    "metrics_enabled": True,
                    "tracing_enabled": True
                }
            }


# Create service instance
service = RiskEngineService()
app = service.app


if __name__ == "__main__":
    import uvicorn
    
    # Run the service
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=service.settings.SERVICE_PORT,
        reload=True,
        log_level="info"
    ) 