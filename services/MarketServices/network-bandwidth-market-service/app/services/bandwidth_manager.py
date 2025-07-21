"""
Bandwidth Manager Service
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import uuid
from pyignite import Client
from web3 import Web3
import asyncio

from ..models import (
    BandwidthAllocation, BandwidthClass, AllocationStatus,
    BandwidthAllocationRequest, QoSParameters, BurstRequest,
    BurstCapacityRequest, NetworkPath
)
from ..config import settings
from .path_registry import PathRegistryService


logger = logging.getLogger(__name__)


class BandwidthManagerService:
    """Service for managing bandwidth allocations and burst requests"""
    
    def __init__(self, path_registry: PathRegistryService):
        self.path_registry = path_registry
        self.ignite_client = None
        self.allocation_cache = None
        self.burst_cache = None
        self.w3 = None
        self.contract = None
        
    async def initialize(self):
        """Initialize connections"""
        try:
            # Connect to Ignite
            self.ignite_client = Client()
            self.ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
            self.allocation_cache = self.ignite_client.get_or_create_cache(
                settings.IGNITE_CACHE_BANDWIDTH
            )
            self.burst_cache = self.ignite_client.get_or_create_cache(
                "bandwidth_burst_requests"
            )
            
            # Initialize Web3 if blockchain is configured
            if settings.BLOCKCHAIN_RPC_URL and settings.NETWORK_BANDWIDTH_CONTRACT:
                self.w3 = Web3(Web3.HTTPProvider(settings.BLOCKCHAIN_RPC_URL))
                # Contract initialization would happen here
                
            logger.info("Bandwidth Manager Service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Bandwidth Manager Service: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.ignite_client:
            self.ignite_client.close()
    
    async def allocate_bandwidth(
        self,
        request: BandwidthAllocationRequest,
        user_address: str
    ) -> Tuple[Optional[BandwidthAllocation], Optional[str]]:
        """Allocate bandwidth on a path"""
        try:
            # Validate path exists and is active
            path = await self.path_registry.get_path(request.path_id)
            if not path:
                return None, "Path not found"
            
            if path.status != "active":
                return None, f"Path is {path.status}"
            
            # Check available bandwidth
            current_allocations = await self._get_active_allocations_for_path(
                request.path_id
            )
            
            used_bandwidth = sum(
                alloc.bandwidth_mbps for alloc in current_allocations
            )
            available = path.max_bandwidth_mbps - used_bandwidth
            
            if request.bandwidth_mbps > available:
                return None, f"Insufficient bandwidth. Available: {available} Mbps"
            
            # Check user limits
            user_allocations = await self._get_user_active_allocations(user_address)
            if len(user_allocations) >= settings.MAX_ALLOCATIONS_PER_USER:
                return None, "User allocation limit reached"
            
            # Calculate pricing
            price_per_hour = await self._calculate_allocation_price(
                request.bandwidth_mbps,
                request.qos_class,
                path
            )
            total_cost = price_per_hour * request.duration_hours
            
            # Create allocation
            allocation_id = f"alloc_{uuid.uuid4().hex[:8]}"
            start_time = request.start_time or datetime.utcnow()
            end_time = start_time + timedelta(hours=request.duration_hours)
            
            # Generate QoS parameters based on class
            qos_params = self._generate_qos_parameters(
                request.qos_class,
                path
            )
            
            allocation = BandwidthAllocation(
                allocation_id=allocation_id,
                user_address=user_address,
                path_id=request.path_id,
                bandwidth_mbps=request.bandwidth_mbps,
                qos_class=request.qos_class,
                qos_parameters=qos_params,
                start_time=start_time,
                end_time=end_time,
                status=AllocationStatus.PENDING,
                price_per_hour=price_per_hour,
                total_cost=total_cost,
                burst_allowed=request.burst_allowed,
                burst_limit_mbps=request.burst_limit_mbps,
                created_at=datetime.utcnow()
            )
            
            # Store allocation
            self.allocation_cache.put(allocation_id, allocation.dict())
            
            # TODO: Blockchain transaction for tokenization
            if self.contract:
                # Would mint bandwidth token here
                pass
            
            # Update allocation status
            allocation.status = AllocationStatus.ACTIVE
            self.allocation_cache.put(allocation_id, allocation.dict())
            
            # Update path available bandwidth
            await self.path_registry.update_path_status(
                request.path_id,
                path.status,
                available - request.bandwidth_mbps
            )
            
            logger.info(f"Allocated bandwidth: {allocation_id}")
            return allocation, None
            
        except Exception as e:
            logger.error(f"Failed to allocate bandwidth: {e}")
            return None, str(e)
    
    async def release_bandwidth(
        self,
        allocation_id: str,
        user_address: str
    ) -> bool:
        """Release bandwidth allocation"""
        try:
            allocation_data = self.allocation_cache.get(allocation_id)
            if not allocation_data:
                return False
            
            allocation = BandwidthAllocation(**allocation_data)
            
            # Verify ownership
            if allocation.user_address != user_address:
                return False
            
            # Update status
            allocation.status = AllocationStatus.TERMINATED
            allocation.end_time = datetime.utcnow()
            
            # Calculate refund if applicable
            if allocation.end_time < allocation.end_time:
                # Pro-rated refund logic would go here
                pass
            
            # Update cache
            self.allocation_cache.put(allocation_id, allocation.dict())
            
            # Update path available bandwidth
            path = await self.path_registry.get_path(allocation.path_id)
            if path:
                await self.path_registry.update_path_status(
                    allocation.path_id,
                    path.status,
                    path.available_bandwidth_mbps + allocation.bandwidth_mbps
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to release bandwidth: {e}")
            return False
    
    async def request_burst(
        self,
        request: BurstCapacityRequest,
        user_address: str
    ) -> Tuple[Optional[BurstRequest], bool, Optional[str]]:
        """Request burst bandwidth capacity"""
        try:
            # Validate allocation
            allocation_data = self.allocation_cache.get(request.allocation_id)
            if not allocation_data:
                return None, False, "Allocation not found"
            
            allocation = BandwidthAllocation(**allocation_data)
            
            # Verify ownership and burst allowed
            if allocation.user_address != user_address:
                return None, False, "Unauthorized"
            
            if not allocation.burst_allowed:
                return None, False, "Burst not allowed for this allocation"
            
            # Check burst limits
            if allocation.burst_limit_mbps:
                total_burst = allocation.bandwidth_mbps + request.additional_bandwidth_mbps
                if total_burst > allocation.burst_limit_mbps:
                    return None, False, f"Exceeds burst limit of {allocation.burst_limit_mbps} Mbps"
            
            # Check user burst request limits
            recent_bursts = await self._get_user_recent_burst_requests(
                user_address,
                hours=1
            )
            if len(recent_bursts) >= settings.MAX_BURST_REQUESTS_PER_HOUR:
                return None, False, "Hourly burst request limit reached"
            
            # Check path capacity
            path = await self.path_registry.get_path(allocation.path_id)
            if not path:
                return None, False, "Path not found"
            
            # Calculate burst pricing
            burst_price = await self._calculate_burst_price(
                request.additional_bandwidth_mbps,
                request.duration_seconds,
                request.urgency_factor,
                allocation.qos_class
            )
            
            # Create burst request
            burst_id = f"burst_{uuid.uuid4().hex[:8]}"
            burst = BurstRequest(
                burst_id=burst_id,
                allocation_id=request.allocation_id,
                requested_bandwidth_mbps=request.additional_bandwidth_mbps,
                duration_seconds=request.duration_seconds,
                urgency_factor=request.urgency_factor,
                approved=False,
                price=burst_price,
                created_at=datetime.utcnow()
            )
            
            # Check if burst can be approved
            if path.available_bandwidth_mbps >= request.additional_bandwidth_mbps:
                burst.approved = True
                burst.actual_bandwidth_mbps = request.additional_bandwidth_mbps
                burst.start_time = datetime.utcnow()
                burst.end_time = burst.start_time + timedelta(
                    seconds=request.duration_seconds
                )
                
                # Update path available bandwidth
                await self.path_registry.update_path_status(
                    allocation.path_id,
                    path.status,
                    path.available_bandwidth_mbps - request.additional_bandwidth_mbps
                )
            
            # Store burst request
            self.burst_cache.put(burst_id, burst.dict())
            
            return burst, burst.approved, None
            
        except Exception as e:
            logger.error(f"Failed to request burst: {e}")
            return None, False, str(e)
    
    async def get_allocation(
        self,
        allocation_id: str
    ) -> Optional[BandwidthAllocation]:
        """Get allocation by ID"""
        try:
            allocation_data = self.allocation_cache.get(allocation_id)
            if allocation_data:
                return BandwidthAllocation(**allocation_data)
            return None
        except Exception as e:
            logger.error(f"Failed to get allocation: {e}")
            return None
    
    async def get_available_bandwidth(
        self,
        path_id: str
    ) -> Optional[int]:
        """Get available bandwidth for a path"""
        try:
            path = await self.path_registry.get_path(path_id)
            if not path:
                return None
            
            allocations = await self._get_active_allocations_for_path(path_id)
            used_bandwidth = sum(alloc.bandwidth_mbps for alloc in allocations)
            
            # Include active bursts
            active_bursts = await self._get_active_bursts_for_path(path_id)
            burst_bandwidth = sum(
                burst.actual_bandwidth_mbps or 0 for burst in active_bursts
            )
            
            return path.max_bandwidth_mbps - used_bandwidth - burst_bandwidth
            
        except Exception as e:
            logger.error(f"Failed to get available bandwidth: {e}")
            return None
    
    async def _get_active_allocations_for_path(
        self,
        path_id: str
    ) -> List[BandwidthAllocation]:
        """Get active allocations for a path"""
        allocations = []
        now = datetime.utcnow()
        
        # In production, this would use a proper query
        # For now, scan through allocations
        for key in self.allocation_cache.scan():
            allocation_data = self.allocation_cache.get(key)
            if allocation_data:
                allocation = BandwidthAllocation(**allocation_data)
                if (allocation.path_id == path_id and
                    allocation.status == AllocationStatus.ACTIVE and
                    allocation.start_time <= now <= allocation.end_time):
                    allocations.append(allocation)
        
        return allocations
    
    async def _get_user_active_allocations(
        self,
        user_address: str
    ) -> List[BandwidthAllocation]:
        """Get active allocations for a user"""
        allocations = []
        now = datetime.utcnow()
        
        for key in self.allocation_cache.scan():
            allocation_data = self.allocation_cache.get(key)
            if allocation_data:
                allocation = BandwidthAllocation(**allocation_data)
                if (allocation.user_address == user_address and
                    allocation.status == AllocationStatus.ACTIVE and
                    allocation.start_time <= now <= allocation.end_time):
                    allocations.append(allocation)
        
        return allocations
    
    async def _get_user_recent_burst_requests(
        self,
        user_address: str,
        hours: int
    ) -> List[BurstRequest]:
        """Get recent burst requests for a user"""
        bursts = []
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        for key in self.burst_cache.scan():
            burst_data = self.burst_cache.get(key)
            if burst_data:
                burst = BurstRequest(**burst_data)
                
                # Get allocation to check user
                allocation_data = self.allocation_cache.get(burst.allocation_id)
                if allocation_data:
                    allocation = BandwidthAllocation(**allocation_data)
                    if (allocation.user_address == user_address and
                        burst.created_at >= cutoff_time):
                        bursts.append(burst)
        
        return bursts
    
    async def _get_active_bursts_for_path(
        self,
        path_id: str
    ) -> List[BurstRequest]:
        """Get active burst requests for a path"""
        bursts = []
        now = datetime.utcnow()
        
        for key in self.burst_cache.scan():
            burst_data = self.burst_cache.get(key)
            if burst_data:
                burst = BurstRequest(**burst_data)
                if (burst.approved and 
                    burst.start_time and burst.end_time and
                    burst.start_time <= now <= burst.end_time):
                    
                    # Get allocation to check path
                    allocation_data = self.allocation_cache.get(burst.allocation_id)
                    if allocation_data:
                        allocation = BandwidthAllocation(**allocation_data)
                        if allocation.path_id == path_id:
                            bursts.append(burst)
        
        return bursts
    
    async def _calculate_allocation_price(
        self,
        bandwidth_mbps: int,
        qos_class: BandwidthClass,
        path: NetworkPath
    ) -> float:
        """Calculate bandwidth allocation price"""
        base_price = settings.BASE_BANDWIDTH_RATE
        qos_multiplier = settings.QOS_CLASS_MULTIPLIERS.get(qos_class.value, 1.0)
        
        # Path quality factor
        quality_factor = path.reliability_score
        
        # Time of day multiplier
        hour = datetime.utcnow().hour
        if 9 <= hour < 17:  # Peak hours
            tod_multiplier = settings.TIME_OF_DAY_MULTIPLIERS["peak"]
        elif 17 <= hour < 24:  # Standard hours
            tod_multiplier = settings.TIME_OF_DAY_MULTIPLIERS["standard"]
        else:  # Off-peak
            tod_multiplier = settings.TIME_OF_DAY_MULTIPLIERS["off_peak"]
        
        price_per_mbps_hour = (
            base_price * qos_multiplier * quality_factor * tod_multiplier
        )
        
        return price_per_mbps_hour * bandwidth_mbps
    
    async def _calculate_burst_price(
        self,
        bandwidth_mbps: int,
        duration_seconds: int,
        urgency_factor: float,
        qos_class: BandwidthClass
    ) -> float:
        """Calculate burst bandwidth price"""
        base_price = settings.BASE_BANDWIDTH_RATE
        burst_multiplier = settings.BURST_RATE_MULTIPLIER
        qos_multiplier = settings.QOS_CLASS_MULTIPLIERS.get(qos_class.value, 1.0)
        
        # Convert to hours
        duration_hours = duration_seconds / 3600
        
        price = (
            base_price * bandwidth_mbps * duration_hours *
            burst_multiplier * urgency_factor * qos_multiplier
        )
        
        return price
    
    def _generate_qos_parameters(
        self,
        qos_class: BandwidthClass,
        path: NetworkPath
    ) -> QoSParameters:
        """Generate QoS parameters based on class and path"""
        base_latency = path.latency_ms
        
        # QoS class parameters
        qos_configs = {
            BandwidthClass.BEST_EFFORT: {
                "latency_factor": 2.0,
                "jitter_factor": 5.0,
                "loss_rate": 0.01,
                "priority": 0
            },
            BandwidthClass.BRONZE: {
                "latency_factor": 1.5,
                "jitter_factor": 3.0,
                "loss_rate": 0.005,
                "priority": 50
            },
            BandwidthClass.SILVER: {
                "latency_factor": 1.2,
                "jitter_factor": 2.0,
                "loss_rate": 0.001,
                "priority": 100
            },
            BandwidthClass.GOLD: {
                "latency_factor": 1.1,
                "jitter_factor": 1.5,
                "loss_rate": 0.0001,
                "priority": 150
            },
            BandwidthClass.PLATINUM: {
                "latency_factor": 1.0,
                "jitter_factor": 1.0,
                "loss_rate": 0.00001,
                "priority": 255
            }
        }
        
        config = qos_configs.get(qos_class, qos_configs[BandwidthClass.BEST_EFFORT])
        
        return QoSParameters(
            bandwidth_mbps=0,  # Will be set by allocation
            latency_ms=base_latency * config["latency_factor"],
            jitter_ms=base_latency * 0.1 * config["jitter_factor"],
            packet_loss_rate=config["loss_rate"],
            priority=config["priority"]
        ) 