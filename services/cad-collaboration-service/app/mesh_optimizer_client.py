import asyncio
from typing import Dict, Optional, Any, List
import json
import uuid
import logging
from datetime import datetime
import httpx

from platformq_shared.events import MeshOptimizationRequest, MeshOptimizationResult
from platformq_shared.event_publisher import EventPublisher
import pulsar

logger = logging.getLogger(__name__)


class MeshOptimizerClient:
    """Client for requesting mesh optimization through Flink jobs"""
    
    def __init__(self,
                 pulsar_url: str = "pulsar://pulsar:6650",
                 tenant_id: str = "default",
                 result_timeout: float = 300.0):  # 5 minutes timeout
        self.pulsar_url = pulsar_url
        self.tenant_id = tenant_id
        self.result_timeout = result_timeout
        
        # Track pending optimization requests
        self.pending_requests: Dict[str, OptimizationRequest] = {}
        
        # Pulsar client for receiving results
        self.pulsar_client = None
        self.result_consumer = None
        self._consumer_task = None
        
    async def start(self):
        """Start the result consumer"""
        self.pulsar_client = pulsar.Client(self.pulsar_url)
        
        # Subscribe to optimization results
        self.result_consumer = self.pulsar_client.subscribe(
            topic=f"persistent://platformq/{self.tenant_id}/mesh-optimization-results",
            subscription_name="cad-collaboration-service-optimizer",
            schema=pulsar.schema.AvroSchema(MeshOptimizationResult)
        )
        
        # Start consumer task
        self._consumer_task = asyncio.create_task(self._consume_results())
        logger.info("Mesh optimizer client started")
        
    async def stop(self):
        """Stop the result consumer"""
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
                
        if self.result_consumer:
            self.result_consumer.close()
            
        if self.pulsar_client:
            self.pulsar_client.close()
            
        logger.info("Mesh optimizer client stopped")
        
    async def request_optimization(self,
                                 asset_id: str,
                                 mesh_data_uri: str,
                                 optimization_params: Dict[str, Any],
                                 callback: Optional[callable] = None) -> str:
        """Request mesh optimization through Flink"""
        request_id = str(uuid.uuid4())
        
        # Create optimization request event
        request = MeshOptimizationRequest(
            tenant_id=self.tenant_id,
            request_id=request_id,
            asset_id=asset_id,
            mesh_data_uri=mesh_data_uri,
            optimization_type=optimization_params.get("type", "DECIMATE"),
            optimization_level=optimization_params.get("level", "MEDIUM"),
            target_poly_count=optimization_params.get("target_poly_count"),
            target_file_size=optimization_params.get("target_file_size"),
            preserve_features=optimization_params.get("preserve_features", []),
            generate_lods=optimization_params.get("generate_lods", True),
            lod_levels=optimization_params.get("lod_levels"),
            output_format=optimization_params.get("output_format", "GLB"),
            priority=optimization_params.get("priority", "NORMAL"),
            callback_url=optimization_params.get("callback_url"),
            metadata=optimization_params.get("metadata", {}),
            requested_at=int(datetime.utcnow().timestamp() * 1000)
        )
        
        # Track pending request
        self.pending_requests[request_id] = OptimizationRequest(
            request_id=request_id,
            asset_id=asset_id,
            callback=callback,
            submitted_at=datetime.utcnow()
        )
        
        # Publish request to Pulsar
        producer = self.pulsar_client.create_producer(
            topic=f"persistent://platformq/{self.tenant_id}/mesh-optimization-requests",
            schema=pulsar.schema.AvroSchema(MeshOptimizationRequest)
        )
        
        try:
            producer.send(request)
            logger.info(f"Submitted optimization request {request_id} for asset {asset_id}")
        finally:
            producer.close()
            
        return request_id
        
    async def _consume_results(self):
        """Consume optimization results from Pulsar"""
        while True:
            try:
                msg = await asyncio.get_event_loop().run_in_executor(
                    None, self.result_consumer.receive
                )
                
                result: MeshOptimizationResult = msg.value()
                
                # Acknowledge message
                self.result_consumer.acknowledge(msg)
                
                # Process result
                await self._process_result(result)
                
            except Exception as e:
                logger.error(f"Error consuming optimization result: {e}")
                await asyncio.sleep(1.0)
                
    async def _process_result(self, result: MeshOptimizationResult):
        """Process an optimization result"""
        request_id = result.request_id
        
        if request_id not in self.pending_requests:
            logger.warning(f"Received result for unknown request {request_id}")
            return
            
        pending = self.pending_requests.pop(request_id)
        
        logger.info(f"Received optimization result for request {request_id}: {result.status}")
        
        # Execute callback if provided
        if pending.callback:
            try:
                await pending.callback(result)
            except Exception as e:
                logger.error(f"Error in optimization callback: {e}")
                
    async def request_lod_generation(self,
                                   asset_id: str,
                                   mesh_data_uri: str,
                                   lod_levels: List[float] = None) -> str:
        """Request LOD generation for a mesh"""
        lod_levels = lod_levels or [0.5, 0.25, 0.1]  # 50%, 25%, 10%
        
        return await self.request_optimization(
            asset_id=asset_id,
            mesh_data_uri=mesh_data_uri,
            optimization_params={
                "type": "DECIMATE",
                "level": "CUSTOM",
                "generate_lods": True,
                "lod_levels": lod_levels,
                "preserve_features": ["UV_SEAMS", "MATERIALS"]
            }
        )
        
    async def request_mesh_repair(self,
                                asset_id: str,
                                mesh_data_uri: str) -> str:
        """Request mesh repair (fix non-manifold, holes, etc.)"""
        return await self.request_optimization(
            asset_id=asset_id,
            mesh_data_uri=mesh_data_uri,
            optimization_params={
                "type": "REPAIR",
                "level": "HIGH",
                "preserve_features": ["UV_SEAMS", "VERTEX_COLORS", "MATERIALS"]
            }
        )
        
    async def get_optimization_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a pending optimization request"""
        if request_id in self.pending_requests:
            pending = self.pending_requests[request_id]
            elapsed = (datetime.utcnow() - pending.submitted_at).total_seconds()
            
            return {
                "request_id": request_id,
                "asset_id": pending.asset_id,
                "status": "PENDING",
                "elapsed_seconds": elapsed,
                "timeout_in": max(0, self.result_timeout - elapsed)
            }
            
        return None
        
    async def cancel_optimization(self, request_id: str) -> bool:
        """Cancel a pending optimization request"""
        if request_id in self.pending_requests:
            # Remove from pending
            self.pending_requests.pop(request_id)
            
            # TODO: Send cancellation event to Flink
            
            logger.info(f"Cancelled optimization request {request_id}")
            return True
            
        return False
        
    def cleanup_expired_requests(self):
        """Clean up expired optimization requests"""
        now = datetime.utcnow()
        expired = []
        
        for request_id, pending in self.pending_requests.items():
            if (now - pending.submitted_at).total_seconds() > self.result_timeout:
                expired.append(request_id)
                
        for request_id in expired:
            pending = self.pending_requests.pop(request_id)
            logger.warning(f"Optimization request {request_id} timed out")
            
            # Execute callback with timeout error if present
            if pending.callback:
                timeout_result = MeshOptimizationResult(
                    tenant_id=self.tenant_id,
                    request_id=request_id,
                    asset_id=pending.asset_id,
                    status="TIMEOUT",
                    error_details={
                        "error_code": "TIMEOUT",
                        "error_message": f"Request timed out after {self.result_timeout} seconds"
                    },
                    optimization_metrics={
                        "processing_time_ms": int(self.result_timeout * 1000)
                    },
                    completed_at=int(now.timestamp() * 1000)
                )
                asyncio.create_task(pending.callback(timeout_result))


class OptimizationRequest:
    """Tracks a pending optimization request"""
    
    def __init__(self, request_id: str, asset_id: str, 
                 callback: Optional[callable] = None,
                 submitted_at: Optional[datetime] = None):
        self.request_id = request_id
        self.asset_id = asset_id
        self.callback = callback
        self.submitted_at = submitted_at or datetime.utcnow() 