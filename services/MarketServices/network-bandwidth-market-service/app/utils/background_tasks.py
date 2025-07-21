"""
Background Tasks for Network Bandwidth Market Service
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import List
import pulsar

from ..models import (
    CongestionEvent, CircuitEvent, BandwidthEvent,
    CongestionLevel, AllocationStatus, PathStatus
)
from ..services import (
    PathRegistryService, BandwidthManagerService,
    CircuitManagerService, PricingEngineService
)
from ..config import settings


logger = logging.getLogger(__name__)


class BackgroundTaskManager:
    """Manages background tasks for the service"""
    
    def __init__(
        self,
        path_registry: PathRegistryService,
        bandwidth_manager: BandwidthManagerService,
        circuit_manager: CircuitManagerService,
        pricing_engine: PricingEngineService
    ):
        self.path_registry = path_registry
        self.bandwidth_manager = bandwidth_manager
        self.circuit_manager = circuit_manager
        self.pricing_engine = pricing_engine
        self.pulsar_client = None
        self.producers = {}
        self.tasks = []
        
    async def start(self):
        """Start all background tasks"""
        try:
            # Initialize Pulsar client
            self.pulsar_client = pulsar.Client(settings.PULSAR_URL)
            
            # Create producers
            self.producers["congestion"] = self.pulsar_client.create_producer(
                settings.PULSAR_TOPIC_CONGESTION
            )
            self.producers["circuits"] = self.pulsar_client.create_producer(
                settings.PULSAR_TOPIC_CIRCUITS
            )
            self.producers["bandwidth"] = self.pulsar_client.create_producer(
                settings.PULSAR_TOPIC_BANDWIDTH
            )
            
            # Start background tasks
            self.tasks.append(
                asyncio.create_task(self.congestion_monitoring_task())
            )
            self.tasks.append(
                asyncio.create_task(self.circuit_health_monitoring_task())
            )
            self.tasks.append(
                asyncio.create_task(self.bandwidth_cleanup_task())
            )
            self.tasks.append(
                asyncio.create_task(self.path_optimization_task())
            )
            self.tasks.append(
                asyncio.create_task(self.settlement_processing_task())
            )
            
            logger.info("Background tasks started")
            
        except Exception as e:
            logger.error(f"Failed to start background tasks: {e}")
            raise
    
    async def stop(self):
        """Stop all background tasks"""
        # Cancel all tasks
        for task in self.tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close Pulsar producers
        for producer in self.producers.values():
            producer.close()
        
        # Close Pulsar client
        if self.pulsar_client:
            self.pulsar_client.close()
        
        logger.info("Background tasks stopped")
    
    async def congestion_monitoring_task(self):
        """Monitor network congestion and update pricing"""
        logger.info("Starting congestion monitoring task")
        
        while True:
            try:
                # Get all active paths
                active_paths = await self.path_registry.get_paths_by_status(
                    PathStatus.ACTIVE
                )
                
                for path in active_paths:
                    # Calculate current utilization
                    available = await self.bandwidth_manager.get_available_bandwidth(
                        path.path_id
                    )
                    
                    if available is not None:
                        utilization_percent = (
                            (path.max_bandwidth_mbps - available) / 
                            path.max_bandwidth_mbps * 100
                        )
                        
                        # Determine congestion level
                        if utilization_percent < 50:
                            congestion_level = CongestionLevel.NONE
                        elif utilization_percent < 70:
                            congestion_level = CongestionLevel.LOW
                        elif utilization_percent < 85:
                            congestion_level = CongestionLevel.MODERATE
                        elif utilization_percent < 95:
                            congestion_level = CongestionLevel.HIGH
                        else:
                            congestion_level = CongestionLevel.SEVERE
                        
                        # Update path status if congested
                        if congestion_level in [CongestionLevel.HIGH, CongestionLevel.SEVERE]:
                            await self.path_registry.update_path_status(
                                path.path_id,
                                PathStatus.CONGESTED,
                                available
                            )
                        
                        # Send congestion event
                        event = CongestionEvent(
                            event_id=f"cong_{datetime.utcnow().timestamp()}",
                            path_id=path.path_id,
                            timestamp=datetime.utcnow(),
                            congestion_level=congestion_level,
                            utilization_percent=utilization_percent,
                            affected_allocations=[],  # Would calculate in production
                            estimated_duration_minutes=None
                        )
                        
                        self.producers["congestion"].send(
                            event.json().encode('utf-8')
                        )
                        
                        # Update pricing based on congestion
                        await self.pricing_engine.update_congestion_pricing({
                            "path_id": path.path_id,
                            "utilization_percent": utilization_percent,
                            "congestion_level": congestion_level.value,
                            "available_bandwidth": available
                        })
                
                # Sleep for configured interval
                await asyncio.sleep(settings.CONGESTION_CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in congestion monitoring: {e}")
                await asyncio.sleep(settings.CONGESTION_CHECK_INTERVAL)
    
    async def circuit_health_monitoring_task(self):
        """Monitor circuit health and SLA compliance"""
        logger.info("Starting circuit health monitoring task")
        
        while True:
            try:
                # Get all active circuits
                # In production, would query active circuits
                
                await asyncio.sleep(settings.CIRCUIT_HEALTH_CHECK_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in circuit health monitoring: {e}")
                await asyncio.sleep(settings.CIRCUIT_HEALTH_CHECK_INTERVAL)
    
    async def bandwidth_cleanup_task(self):
        """Clean up expired bandwidth allocations"""
        logger.info("Starting bandwidth cleanup task")
        
        while True:
            try:
                now = datetime.utcnow()
                
                # In production, would query expired allocations
                # For now, just log
                logger.debug(f"Running bandwidth cleanup at {now}")
                
                # Also handle expired burst requests
                
                await asyncio.sleep(300)  # Run every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in bandwidth cleanup: {e}")
                await asyncio.sleep(300)
    
    async def path_optimization_task(self):
        """Analyze and optimize network paths"""
        logger.info("Starting path optimization task")
        
        while True:
            try:
                # Analyze path efficiency
                # Discover alternative routes
                # Generate optimization recommendations
                
                await asyncio.sleep(3600)  # Run hourly
                
            except Exception as e:
                logger.error(f"Error in path optimization: {e}")
                await asyncio.sleep(3600)
    
    async def settlement_processing_task(self):
        """Process bandwidth usage settlements"""
        logger.info("Starting settlement processing task")
        
        while True:
            try:
                # Process bandwidth usage reconciliation
                # Calculate billing
                # Process SLA credits
                # Execute contract settlements
                
                await asyncio.sleep(settings.SETTLEMENT_INTERVAL)
                
            except Exception as e:
                logger.error(f"Error in settlement processing: {e}")
                await asyncio.sleep(settings.SETTLEMENT_INTERVAL)


# Additional utility functions
async def release_expired_burst_capacity(
    bandwidth_manager: BandwidthManagerService,
    path_registry: PathRegistryService
):
    """Release bandwidth from expired burst requests"""
    # In production, would query expired bursts from cache
    # and release their bandwidth back to paths
    pass


async def check_sla_violations(
    circuit_manager: CircuitManagerService,
    pulsar_producer
):
    """Check for circuit SLA violations"""
    # Monitor circuit performance against SLA
    # Generate violation events
    # Calculate credits
    pass


async def predict_congestion(
    path_id: str,
    historical_data: List[float]
) -> float:
    """Predict future congestion using ML model"""
    # In production, would use trained ML model
    # For now, return simple moving average
    if not historical_data:
        return 0.0
    
    return sum(historical_data[-10:]) / min(len(historical_data), 10) 