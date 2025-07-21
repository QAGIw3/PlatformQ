"""
Circuit Manager Service
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import uuid
import asyncio
from pyignite import Client

from ..models import (
    DedicatedCircuit, CircuitType, AllocationStatus,
    CircuitProvisionRequest, NetworkPath, NetworkNode
)
from ..config import settings
from .path_registry import PathRegistryService


logger = logging.getLogger(__name__)


class CircuitManagerService:
    """Service for managing dedicated network circuits"""
    
    def __init__(self, path_registry: PathRegistryService):
        self.path_registry = path_registry
        self.ignite_client = None
        self.circuit_cache = None
        
    async def initialize(self):
        """Initialize connections"""
        try:
            # Connect to Ignite
            self.ignite_client = Client()
            self.ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
            self.circuit_cache = self.ignite_client.get_or_create_cache(
                settings.IGNITE_CACHE_CIRCUITS
            )
            
            logger.info("Circuit Manager Service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Circuit Manager Service: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.ignite_client:
            self.ignite_client.close()
    
    async def provision_circuit(
        self,
        request: CircuitProvisionRequest,
        user_address: str
    ) -> Tuple[Optional[DedicatedCircuit], List[NetworkPath], Optional[str]]:
        """Provision a dedicated circuit"""
        try:
            # Validate circuit type and endpoints
            if request.circuit_type == CircuitType.POINT_TO_POINT:
                if len(request.endpoints) != 2:
                    return None, [], "Point-to-point circuit requires exactly 2 endpoints"
            elif len(request.endpoints) < 2:
                return None, [], "Circuit requires at least 2 endpoints"
            
            # Check user circuit limits
            user_circuits = await self._get_user_active_circuits(user_address)
            if len(user_circuits) >= settings.MAX_CIRCUITS_PER_USER:
                return None, [], "User circuit limit reached"
            
            # Find suitable paths for the circuit
            selected_paths = await self._find_circuit_paths(
                request.endpoints,
                request.circuit_type,
                request.bandwidth_mbps,
                request.latency_requirement_ms,
                request.redundancy
            )
            
            if not selected_paths:
                return None, [], "No suitable paths found for circuit requirements"
            
            # Calculate circuit cost
            monthly_cost = await self._calculate_circuit_cost(
                selected_paths,
                request.bandwidth_mbps,
                request.redundancy
            )
            
            # Create circuit
            circuit_id = f"circuit_{uuid.uuid4().hex[:8]}"
            start_date = request.start_date or datetime.utcnow()
            end_date = start_date + timedelta(days=request.duration_days)
            
            # Define SLA parameters
            sla_parameters = {
                "guaranteed_bandwidth_mbps": request.bandwidth_mbps,
                "max_latency_ms": request.latency_requirement_ms or 
                                 max(p.latency_ms for p in selected_paths) * 1.2,
                "min_availability": 0.999 if request.redundancy else 0.995,
                "max_packet_loss": 0.0001,
                "setup_time_seconds": settings.CIRCUIT_SETUP_TIME
            }
            
            circuit = DedicatedCircuit(
                circuit_id=circuit_id,
                user_address=user_address,
                circuit_type=request.circuit_type,
                endpoints=request.endpoints,
                bandwidth_mbps=request.bandwidth_mbps,
                guaranteed_latency_ms=sla_parameters["max_latency_ms"],
                redundancy_enabled=request.redundancy,
                redundant_paths=[p.path_id for p in selected_paths[1:]] if request.redundancy else None,
                start_date=start_date,
                end_date=end_date,
                monthly_cost=monthly_cost,
                sla_parameters=sla_parameters,
                status=AllocationStatus.PENDING,
                created_at=datetime.utcnow()
            )
            
            # Store circuit
            self.circuit_cache.put(circuit_id, circuit.dict())
            
            # Reserve bandwidth on selected paths
            for path in selected_paths:
                success = await self._reserve_path_bandwidth(
                    path.path_id,
                    request.bandwidth_mbps,
                    start_date,
                    end_date
                )
                if not success:
                    # Rollback if any reservation fails
                    await self._rollback_circuit_provision(circuit_id, selected_paths)
                    return None, [], "Failed to reserve bandwidth on paths"
            
            # Update circuit status
            circuit.status = AllocationStatus.ACTIVE
            self.circuit_cache.put(circuit_id, circuit.dict())
            
            logger.info(f"Provisioned circuit: {circuit_id}")
            return circuit, selected_paths, None
            
        except Exception as e:
            logger.error(f"Failed to provision circuit: {e}")
            return None, [], str(e)
    
    async def modify_circuit(
        self,
        circuit_id: str,
        user_address: str,
        new_bandwidth_mbps: Optional[int] = None,
        extend_duration_days: Optional[int] = None
    ) -> Tuple[bool, Optional[str]]:
        """Modify an existing circuit"""
        try:
            circuit_data = self.circuit_cache.get(circuit_id)
            if not circuit_data:
                return False, "Circuit not found"
            
            circuit = DedicatedCircuit(**circuit_data)
            
            # Verify ownership
            if circuit.user_address != user_address:
                return False, "Unauthorized"
            
            if circuit.status != AllocationStatus.ACTIVE:
                return False, f"Circuit is {circuit.status}"
            
            # Handle bandwidth modification
            if new_bandwidth_mbps:
                if new_bandwidth_mbps < circuit.bandwidth_mbps:
                    # Downgrade is usually allowed
                    circuit.bandwidth_mbps = new_bandwidth_mbps
                else:
                    # Check if upgrade is possible
                    can_upgrade = await self._check_circuit_upgrade_feasibility(
                        circuit,
                        new_bandwidth_mbps
                    )
                    if not can_upgrade:
                        return False, "Cannot upgrade circuit bandwidth - insufficient capacity"
                    circuit.bandwidth_mbps = new_bandwidth_mbps
                
                # Recalculate cost
                circuit.monthly_cost = await self._recalculate_circuit_cost(circuit)
            
            # Handle duration extension
            if extend_duration_days:
                circuit.end_date = circuit.end_date + timedelta(days=extend_duration_days)
            
            # Update circuit
            self.circuit_cache.put(circuit_id, circuit.dict())
            
            return True, None
            
        except Exception as e:
            logger.error(f"Failed to modify circuit: {e}")
            return False, str(e)
    
    async def decommission_circuit(
        self,
        circuit_id: str,
        user_address: str
    ) -> bool:
        """Decommission a circuit"""
        try:
            circuit_data = self.circuit_cache.get(circuit_id)
            if not circuit_data:
                return False
            
            circuit = DedicatedCircuit(**circuit_data)
            
            # Verify ownership
            if circuit.user_address != user_address:
                return False
            
            # Update status
            circuit.status = AllocationStatus.TERMINATED
            circuit.end_date = datetime.utcnow()
            
            # Release bandwidth on paths
            paths = [circuit.path_id] if hasattr(circuit, 'path_id') else []
            if circuit.redundant_paths:
                paths.extend(circuit.redundant_paths)
            
            for path_id in paths:
                await self._release_path_bandwidth(
                    path_id,
                    circuit.bandwidth_mbps
                )
            
            # Update circuit
            self.circuit_cache.put(circuit_id, circuit.dict())
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to decommission circuit: {e}")
            return False
    
    async def get_circuit(
        self,
        circuit_id: str
    ) -> Optional[DedicatedCircuit]:
        """Get circuit by ID"""
        try:
            circuit_data = self.circuit_cache.get(circuit_id)
            if circuit_data:
                return DedicatedCircuit(**circuit_data)
            return None
        except Exception as e:
            logger.error(f"Failed to get circuit: {e}")
            return None
    
    async def monitor_circuit_health(
        self,
        circuit_id: str
    ) -> Dict[str, any]:
        """Monitor circuit health and SLA compliance"""
        try:
            circuit = await self.get_circuit(circuit_id)
            if not circuit:
                return {}
            
            health_status = {
                "circuit_id": circuit_id,
                "status": circuit.status.value,
                "sla_compliance": True,
                "metrics": {}
            }
            
            # Get paths for the circuit
            paths = []
            if hasattr(circuit, 'path_id'):
                path = await self.path_registry.get_path(circuit.path_id)
                if path:
                    paths.append(path)
            
            if circuit.redundant_paths:
                for path_id in circuit.redundant_paths:
                    path = await self.path_registry.get_path(path_id)
                    if path:
                        paths.append(path)
            
            # Check each path
            for path in paths:
                path_health = {
                    "status": path.status.value,
                    "latency_ms": path.latency_ms,
                    "available_bandwidth_mbps": path.available_bandwidth_mbps,
                    "reliability_score": path.reliability_score
                }
                
                # Check SLA violations
                if path.latency_ms > circuit.guaranteed_latency_ms:
                    health_status["sla_compliance"] = False
                    path_health["sla_violation"] = "latency_exceeded"
                
                health_status["metrics"][path.path_id] = path_health
            
            return health_status
            
        except Exception as e:
            logger.error(f"Failed to monitor circuit health: {e}")
            return {}
    
    async def _find_circuit_paths(
        self,
        endpoints: List[NetworkNode],
        circuit_type: CircuitType,
        bandwidth_mbps: int,
        latency_requirement_ms: Optional[float],
        redundancy: bool
    ) -> List[NetworkPath]:
        """Find suitable paths for circuit requirements"""
        selected_paths = []
        
        if circuit_type == CircuitType.POINT_TO_POINT:
            # Find paths between two endpoints
            source = endpoints[0].node_id
            destination = endpoints[1].node_id
            
            paths = await self.path_registry.search_paths({
                "source": source,
                "destination": destination,
                "min_bandwidth_mbps": bandwidth_mbps,
                "max_latency_ms": latency_requirement_ms
            })
            
            if paths:
                selected_paths.append(paths[0])
                
                # Find redundant path if needed
                if redundancy and len(paths) > 1:
                    selected_paths.append(paths[1])
                elif redundancy:
                    # Try to find alternative path
                    alt_paths = await self.path_registry.find_alternative_paths(
                        source,
                        destination,
                        [paths[0].path_id]
                    )
                    if alt_paths:
                        selected_paths.append(alt_paths[0])
        
        elif circuit_type == CircuitType.MESH:
            # Find paths between all endpoint pairs
            for i in range(len(endpoints)):
                for j in range(i + 1, len(endpoints)):
                    paths = await self.path_registry.search_paths({
                        "source": endpoints[i].node_id,
                        "destination": endpoints[j].node_id,
                        "min_bandwidth_mbps": bandwidth_mbps,
                        "max_latency_ms": latency_requirement_ms
                    })
                    if paths:
                        selected_paths.append(paths[0])
        
        elif circuit_type == CircuitType.HUB_SPOKE:
            # First endpoint is hub
            hub = endpoints[0].node_id
            for spoke in endpoints[1:]:
                paths = await self.path_registry.search_paths({
                    "source": hub,
                    "destination": spoke.node_id,
                    "min_bandwidth_mbps": bandwidth_mbps,
                    "max_latency_ms": latency_requirement_ms
                })
                if paths:
                    selected_paths.append(paths[0])
        
        return selected_paths
    
    async def _reserve_path_bandwidth(
        self,
        path_id: str,
        bandwidth_mbps: int,
        start_date: datetime,
        end_date: datetime
    ) -> bool:
        """Reserve bandwidth on a path for circuit"""
        try:
            path = await self.path_registry.get_path(path_id)
            if not path:
                return False
            
            # Check if bandwidth is available
            if path.available_bandwidth_mbps < bandwidth_mbps:
                return False
            
            # Update available bandwidth
            await self.path_registry.update_path_status(
                path_id,
                path.status,
                path.available_bandwidth_mbps - bandwidth_mbps
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to reserve path bandwidth: {e}")
            return False
    
    async def _release_path_bandwidth(
        self,
        path_id: str,
        bandwidth_mbps: int
    ):
        """Release bandwidth on a path"""
        try:
            path = await self.path_registry.get_path(path_id)
            if path:
                await self.path_registry.update_path_status(
                    path_id,
                    path.status,
                    path.available_bandwidth_mbps + bandwidth_mbps
                )
        except Exception as e:
            logger.error(f"Failed to release path bandwidth: {e}")
    
    async def _rollback_circuit_provision(
        self,
        circuit_id: str,
        paths: List[NetworkPath]
    ):
        """Rollback a failed circuit provision"""
        try:
            # Remove circuit from cache
            self.circuit_cache.remove(circuit_id)
            
            # Release any reserved bandwidth
            # (Implementation would track which paths were reserved)
            
        except Exception as e:
            logger.error(f"Failed to rollback circuit provision: {e}")
    
    async def _get_user_active_circuits(
        self,
        user_address: str
    ) -> List[DedicatedCircuit]:
        """Get active circuits for a user"""
        circuits = []
        now = datetime.utcnow()
        
        for key in self.circuit_cache.scan():
            circuit_data = self.circuit_cache.get(key)
            if circuit_data:
                circuit = DedicatedCircuit(**circuit_data)
                if (circuit.user_address == user_address and
                    circuit.status == AllocationStatus.ACTIVE and
                    circuit.start_date <= now <= circuit.end_date):
                    circuits.append(circuit)
        
        return circuits
    
    async def _calculate_circuit_cost(
        self,
        paths: List[NetworkPath],
        bandwidth_mbps: int,
        redundancy: bool
    ) -> float:
        """Calculate monthly cost for circuit"""
        base_rate = settings.BASE_BANDWIDTH_RATE * 720  # Hours in month
        
        # Circuit premium (dedicated resources)
        circuit_multiplier = 10.0
        
        # Redundancy premium
        redundancy_multiplier = 1.5 if redundancy else 1.0
        
        # Path quality factor
        quality_factor = min(p.reliability_score for p in paths)
        
        monthly_cost = (
            base_rate * bandwidth_mbps * circuit_multiplier *
            redundancy_multiplier * quality_factor * len(paths)
        )
        
        return monthly_cost
    
    async def _recalculate_circuit_cost(
        self,
        circuit: DedicatedCircuit
    ) -> float:
        """Recalculate circuit cost after modification"""
        base_rate = settings.BASE_BANDWIDTH_RATE * 720
        circuit_multiplier = 10.0
        redundancy_multiplier = 1.5 if circuit.redundancy_enabled else 1.0
        
        return (
            base_rate * circuit.bandwidth_mbps * 
            circuit_multiplier * redundancy_multiplier
        )
    
    async def _check_circuit_upgrade_feasibility(
        self,
        circuit: DedicatedCircuit,
        new_bandwidth_mbps: int
    ) -> bool:
        """Check if circuit can be upgraded to new bandwidth"""
        # Check primary path
        paths_to_check = []
        if hasattr(circuit, 'path_id'):
            paths_to_check.append(circuit.path_id)
        if circuit.redundant_paths:
            paths_to_check.extend(circuit.redundant_paths)
        
        for path_id in paths_to_check:
            path = await self.path_registry.get_path(path_id)
            if not path:
                return False
            
            bandwidth_increase = new_bandwidth_mbps - circuit.bandwidth_mbps
            if path.available_bandwidth_mbps < bandwidth_increase:
                return False
        
        return True 