"""Event processor for Resource Scaling Service"""

import logging
from typing import Optional

from platformq_shared import (
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus
)
from platformq_resource_common import (
    ResourceAnomalyEvent,
    ScalingDecision,
    ScalingAction
)

from .scaling_engine import ScalingEngine

logger = logging.getLogger(__name__)


class ScalingEventProcessor(EventProcessor):
    """Process resource anomaly events for scaling"""
    
    def __init__(
        self,
        service_name: str,
        pulsar_url: str,
        scaling_engine: ScalingEngine
    ):
        super().__init__(service_name, pulsar_url)
        self.scaling_engine = scaling_engine
    
    async def on_start(self):
        """Initialize event processor"""
        logger.info("Starting scaling event processor")
    
    async def on_stop(self):
        """Cleanup event processor"""
        logger.info("Stopping scaling event processor")
    
    @event_handler("persistent://public/default/resource-anomalies", dict)
    async def handle_resource_anomaly(self, event_data: dict, msg):
        """Handle resource anomaly events"""
        try:
            # Parse anomaly event
            anomaly = ResourceAnomalyEvent(**event_data)
            
            logger.info(
                f"Processing anomaly for {anomaly.service_name}: "
                f"{anomaly.anomaly_type} (severity: {anomaly.severity:.2f})"
            )
            
            # Only react to high severity anomalies
            if anomaly.severity < 0.7:
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    message="Anomaly severity below threshold"
                )
            
            # Get scaling policy
            policy = await self.scaling_engine.get_scaling_policy(anomaly.service_name)
            if not policy:
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    message="No scaling policy for service"
                )
            
            # Handle different anomaly types
            if anomaly.anomaly_type == 'high_cpu':
                await self._handle_high_cpu(anomaly, policy)
            elif anomaly.anomaly_type == 'high_memory':
                await self._handle_high_memory(anomaly, policy)
            elif anomaly.anomaly_type == 'high_error_rate':
                await self._handle_high_error_rate(anomaly, policy)
            elif anomaly.anomaly_type == 'slow_response':
                await self._handle_slow_response(anomaly, policy)
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                message=f"Processed {anomaly.anomaly_type} anomaly"
            )
            
        except Exception as e:
            logger.error(f"Error processing resource anomaly: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
    
    async def _handle_high_cpu(self, anomaly: ResourceAnomalyEvent, policy):
        """Handle high CPU anomaly"""
        # Trigger immediate scaling evaluation
        logger.info(f"High CPU detected for {anomaly.service_name}, triggering scaling evaluation")
        
        # Force a scaling evaluation
        await self.scaling_engine._evaluate_service_scaling(
            anomaly.service_name,
            anomaly.namespace
        )
    
    async def _handle_high_memory(self, anomaly: ResourceAnomalyEvent, policy):
        """Handle high memory anomaly"""
        # Similar to CPU, trigger scaling evaluation
        logger.info(f"High memory detected for {anomaly.service_name}, triggering scaling evaluation")
        
        await self.scaling_engine._evaluate_service_scaling(
            anomaly.service_name,
            anomaly.namespace
        )
    
    async def _handle_high_error_rate(self, anomaly: ResourceAnomalyEvent, policy):
        """Handle high error rate anomaly"""
        # High error rate might indicate overload
        logger.warning(
            f"High error rate detected for {anomaly.service_name}: "
            f"{anomaly.current_value:.2f} errors/sec"
        )
        
        # Consider scaling out to distribute load
        if policy.enable_predictive_scaling:
            await self.scaling_engine._evaluate_service_scaling(
                anomaly.service_name,
                anomaly.namespace
            )
    
    async def _handle_slow_response(self, anomaly: ResourceAnomalyEvent, policy):
        """Handle slow response time anomaly"""
        logger.warning(
            f"Slow response times detected for {anomaly.service_name}: "
            f"{anomaly.current_value:.0f}ms"
        )
        
        # Slow response might indicate need for more resources
        await self.scaling_engine._evaluate_service_scaling(
            anomaly.service_name,
            anomaly.namespace
        ) 