"""
Example service demonstrating how to use the refactored DataIntelligenceBaseService.

This example shows:
- How to configure the service with ServiceConfig
- How to use caching
- How to publish and subscribe to events
- How to use circuit breakers
- How to set up custom health checks
"""

from typing import Dict, Any
from fastapi import HTTPException
import asyncio
import logging

from . import (
    DataIntelligenceBaseService,
    ServiceMetadata,
    ServiceConfig,
    create_data_intelligence_app,
    CacheConfig,
    EventConfig
)

logger = logging.getLogger(__name__)


class ExampleDataService(DataIntelligenceBaseService):
    """Example implementation of a DataIntelligence service."""
    
    async def initialize_service(self):
        """Initialize service-specific components."""
        logger.info("Initializing ExampleDataService")
        
        # Subscribe to events if enabled
        if self.config.enable_events:
            self.subscribe_event(
                "data-events",
                "example-service-subscription",
                self.handle_data_event
            )
            
        # Add custom health check
        self.health_manager.add_check(
            "custom_check",
            self._custom_health_check
        )
        
    async def cleanup_service(self):
        """Cleanup service-specific components."""
        logger.info("Cleaning up ExampleDataService")
        
    async def handle_data_event(self, event: Dict[str, Any]):
        """Handle incoming data events."""
        logger.info(f"Received event: {event}")
        
        # Process the event
        event_type = event.get("_metadata", {}).get("event_type")
        if event_type == "data_update":
            # Invalidate cache for updated data
            data_id = event.get("data_id")
            if data_id:
                await self.put_cache("query_results_cache", f"data_{data_id}", None)
                
    async def _custom_health_check(self) -> bool:
        """Custom health check for this service."""
        # Implement your custom health check logic
        return True
        
    async def process_data(self, data_id: str) -> Dict[str, Any]:
        """
        Example method showing how to use caching and circuit breakers.
        """
        # Check cache first
        cache_key = f"data_{data_id}"
        cached_result = await self.get_cache("query_results_cache", cache_key)
        if cached_result:
            logger.info(f"Cache hit for {data_id}")
            return cached_result
            
        # Use circuit breaker for external service call
        circuit_breaker = self.get_circuit_breaker("external_api")
        if circuit_breaker:
            try:
                # Make external call with circuit breaker
                result = await circuit_breaker(self._fetch_from_external_api)(data_id)
            except Exception as e:
                logger.error(f"External API call failed: {e}")
                raise HTTPException(status_code=503, detail="External service unavailable")
        else:
            # Fallback without circuit breaker
            result = await self._fetch_from_external_api(data_id)
            
        # Cache the result
        await self.put_cache("query_results_cache", cache_key, result, ttl=300)
        
        # Publish event about data processing
        await self.publish_event(
            "data-events",
            {
                "data_id": data_id,
                "status": "processed",
                "result_summary": len(result)
            },
            event_type="data_processed"
        )
        
        return result
        
    async def _fetch_from_external_api(self, data_id: str) -> Dict[str, Any]:
        """Simulate fetching data from external API."""
        # Simulate API call
        await asyncio.sleep(0.1)
        return {
            "id": data_id,
            "data": f"Processed data for {data_id}",
            "timestamp": asyncio.get_event_loop().time()
        }


def create_example_service():
    """Factory function to create the example service with FastAPI app."""
    
    # Define service metadata
    metadata = ServiceMetadata(
        name="example-data-service",
        version="1.0.0",
        description="Example DataIntelligence service",
        capabilities=["data_processing", "caching", "events"],
        dependencies=["ignite", "pulsar"],
        min_memory_mb=1024,
        min_cpu_cores=1.0,
        data_sources=["external_api"],
        data_outputs=["processed_data"]
    )
    
    # Create service configuration
    config = ServiceConfig(
        name="example-data-service",
        version="1.0.0",
        enable_caching=True,
        enable_events=True,
        enable_rate_limiting=True,
        rate_limit_requests=100,
        enable_circuit_breaker=True,
        circuit_breaker_failures=3,
        circuit_breaker_timeout=30
    )
    
    # Create the FastAPI app and service instance
    app, service = create_data_intelligence_app(
        service_metadata=metadata,
        service_config=config,
        include_health_endpoint=True,
        include_metrics_endpoint=True
    )
    
    # Add service-specific routes
    @app.get("/process/{data_id}")
    async def process_data_endpoint(data_id: str):
        """Process data by ID."""
        return await service.process_data(data_id)
        
    @app.post("/invalidate-cache/{data_id}")
    async def invalidate_cache(data_id: str):
        """Invalidate cache for specific data."""
        cache_key = f"data_{data_id}"
        await service.put_cache("query_results_cache", cache_key, None)
        return {"status": "cache invalidated", "data_id": data_id}
        
    return app, service


if __name__ == "__main__":
    import uvicorn
    
    # Create and run the service
    app, service = create_example_service()
    uvicorn.run(app, host="0.0.0.0", port=8000) 