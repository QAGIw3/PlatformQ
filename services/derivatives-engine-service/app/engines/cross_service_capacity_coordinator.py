"""
Cross-Service Capacity Coordinator

Coordinates compute capacity allocation across all platform services including
MLOps, Simulation, Digital Assets, etc.
"""

from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import httpx

from app.integrations import IgniteCache, PulsarEventPublisher
from app.integrations.mlops_client import MLOpsClient
from app.engines.partner_capacity_manager import PartnerCapacityManager, CapacityTier
from app.engines.compute_futures_engine import ComputeFuturesEngine

logger = logging.getLogger(__name__)


class ServiceType(Enum):
    """Platform services that consume compute resources"""
    MLOPS = "mlops"
    SIMULATION = "simulation"
    DIGITAL_ASSET = "digital_asset"
    FEDERATED_LEARNING = "federated_learning"
    GRAPH_INTELLIGENCE = "graph_intelligence"
    QUANTUM_OPTIMIZATION = "quantum_optimization"
    NEUROMORPHIC = "neuromorphic"
    REALTIME_ANALYTICS = "realtime_analytics"
    DATA_PROCESSING = "data_processing"


@dataclass
class ServiceCapacityRequest:
    """Capacity request from a service"""
    request_id: str
    service_type: ServiceType
    tenant_id: str
    resource_type: str
    quantity: Decimal
    duration: timedelta
    start_time: datetime
    priority: int  # 0-10, higher is more important
    flexibility_hours: int  # How many hours the start time can be shifted
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def end_time(self) -> datetime:
        return self.start_time + self.duration


@dataclass
class CapacityAllocation:
    """Allocated capacity for a service request"""
    allocation_id: str
    request_id: str
    service_type: ServiceType
    resource_type: str
    quantity: Decimal
    tier: CapacityTier
    provider: Optional[str]
    start_time: datetime
    duration: timedelta
    cost: Decimal
    status: str  # "reserved", "active", "completed", "failed"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CapacityForecast:
    """Forecast of capacity demand"""
    service_type: ServiceType
    resource_type: str
    time_window: datetime
    expected_demand: Decimal
    confidence: float
    peak_demand: Decimal
    average_demand: Decimal


class CrossServiceCapacityCoordinator:
    """Coordinates capacity allocation across all platform services"""
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        partner_manager: PartnerCapacityManager,
        futures_engine: ComputeFuturesEngine
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.partner_manager = partner_manager
        self.futures_engine = futures_engine
        
        # Service clients
        self.service_clients: Dict[ServiceType, httpx.AsyncClient] = {}
        self._init_service_clients()
        
        # Specialized clients
        self.mlops_client = MLOpsClient()
        
        # Request tracking
        self.pending_requests: List[ServiceCapacityRequest] = []
        self.allocations: Dict[str, CapacityAllocation] = {}
        
        # Forecasting data
        self.historical_usage: Dict[str, List[Dict]] = {}
        self.forecasts: Dict[str, CapacityForecast] = {}
        
        # Background tasks
        self._coordinator_task = None
        self._forecasting_task = None
        self._optimizer_task = None
        
    def _init_service_clients(self):
        """Initialize HTTP clients for each service"""
        service_urls = {
            ServiceType.MLOPS: "http://mlops-service:8000",
            ServiceType.SIMULATION: "http://simulation-service:8000",
            ServiceType.DIGITAL_ASSET: "http://digital-asset-service:8000",
            ServiceType.FEDERATED_LEARNING: "http://federated-learning-service:8000",
            ServiceType.GRAPH_INTELLIGENCE: "http://graph-intelligence-service:8000",
            ServiceType.QUANTUM_OPTIMIZATION: "http://quantum-optimization-service:8000",
            ServiceType.NEUROMORPHIC: "http://neuromorphic-service:8000",
            ServiceType.REALTIME_ANALYTICS: "http://realtime-analytics-service:8000",
            ServiceType.DATA_PROCESSING: "http://analytics-service:8000"
        }
        
        for service_type, url in service_urls.items():
            self.service_clients[service_type] = httpx.AsyncClient(
                base_url=url,
                timeout=30.0
            )
            
    async def start(self):
        """Start the coordinator"""
        self._coordinator_task = asyncio.create_task(self._coordination_loop())
        self._forecasting_task = asyncio.create_task(self._forecasting_loop())
        self._optimizer_task = asyncio.create_task(self._optimization_loop())
        
        # Load historical data
        await self._load_historical_usage()
        
        logger.info("Cross-service capacity coordinator started")
        
    async def stop(self):
        """Stop the coordinator"""
        if self._coordinator_task:
            self._coordinator_task.cancel()
        if self._forecasting_task:
            self._forecasting_task.cancel()
        if self._optimizer_task:
            self._optimizer_task.cancel()
            
        # Close service clients
        for client in self.service_clients.values():
            await client.aclose()
        await self.mlops_client.close()
        
    async def request_capacity(
        self,
        request: ServiceCapacityRequest
    ) -> Optional[str]:
        """Request capacity for a service"""
        # Validate request
        if not await self._validate_request(request):
            return None
            
        # Try immediate allocation
        allocation = await self._try_immediate_allocation(request)
        
        if allocation:
            self.allocations[allocation.allocation_id] = allocation
            await self._store_allocation(allocation)
            await self._notify_service(request.service_type, allocation)
            return allocation.allocation_id
        else:
            # Queue for later allocation
            self.pending_requests.append(request)
            await self._store_pending_request(request)
            
            # Try to optimize with flexibility
            if request.flexibility_hours > 0:
                asyncio.create_task(self._optimize_flexible_request(request))
                
            return request.request_id
            
    async def get_capacity_forecast(
        self,
        service_type: Optional[ServiceType] = None,
        resource_type: Optional[str] = None,
        days_ahead: int = 7
    ) -> List[CapacityForecast]:
        """Get capacity demand forecast"""
        forecasts = []
        
        # Filter by service type and resource type
        for key, forecast in self.forecasts.items():
            if service_type and forecast.service_type != service_type:
                continue
            if resource_type and forecast.resource_type != resource_type:
                continue
                
            # Check if forecast is within time window
            if forecast.time_window <= datetime.utcnow() + timedelta(days=days_ahead):
                forecasts.append(forecast)
                
        return forecasts
        
    async def optimize_capacity_allocation(self):
        """Optimize capacity allocation across all services"""
        # Get all pending requests
        requests_by_priority = sorted(
            self.pending_requests,
            key=lambda r: (r.priority, -r.flexibility_hours),
            reverse=True
        )
        
        # Get available capacity forecast
        available_capacity = await self._get_available_capacity_forecast()
        
        # Optimize allocation
        optimal_allocations = await self._compute_optimal_allocation(
            requests_by_priority,
            available_capacity
        )
        
        # Execute allocations
        for allocation in optimal_allocations:
            self.allocations[allocation.allocation_id] = allocation
            await self._store_allocation(allocation)
            
            # Remove from pending
            self.pending_requests = [
                r for r in self.pending_requests
                if r.request_id != allocation.request_id
            ]
            
            # Notify service
            await self._notify_service(allocation.service_type, allocation)
            
        logger.info(f"Optimized allocation completed: {len(optimal_allocations)} allocations made")
        
    # Private methods
    async def _validate_request(self, request: ServiceCapacityRequest) -> bool:
        """Validate capacity request"""
        # Check service limits
        service_usage = await self._get_service_usage(
            request.service_type,
            request.tenant_id
        )
        
        # Basic validation
        if request.quantity <= 0:
            logger.error(f"Invalid quantity in request {request.request_id}")
            return False
            
        if request.duration.total_seconds() <= 0:
            logger.error(f"Invalid duration in request {request.request_id}")
            return False
            
        # Service-specific validation
        if request.service_type == ServiceType.MLOPS:
            # Check if model training request is valid
            if request.quantity > Decimal("8"):  # Max 8 GPUs per training job
                logger.warning(f"MLOps request exceeds GPU limit: {request.quantity}")
                return False
                
        return True
        
    async def _try_immediate_allocation(
        self,
        request: ServiceCapacityRequest
    ) -> Optional[CapacityAllocation]:
        """Try to allocate capacity immediately"""
        # First try platform-owned resources
        platform_capacity = await self._check_platform_capacity(
            request.resource_type,
            request.quantity,
            request.start_time,
            request.duration
        )
        
        if platform_capacity:
            return CapacityAllocation(
                allocation_id=f"ALLOC_{datetime.utcnow().timestamp()}",
                request_id=request.request_id,
                service_type=request.service_type,
                resource_type=request.resource_type,
                quantity=request.quantity,
                tier=CapacityTier.PLATFORM_OWNED,
                provider=None,
                start_time=request.start_time,
                duration=request.duration,
                cost=platform_capacity["cost"],
                status="reserved"
            )
            
        # Try partner inventory
        partner_allocation = await self.partner_manager.allocate_from_inventory(
            request.resource_type,
            "us-east-1",  # TODO: Get region from request
            request.quantity
        )
        
        if partner_allocation:
            cost = partner_allocation["wholesale_price"] * request.quantity * Decimal(str(request.duration.total_seconds() / 3600))
            
            return CapacityAllocation(
                allocation_id=f"ALLOC_{datetime.utcnow().timestamp()}",
                request_id=request.request_id,
                service_type=request.service_type,
                resource_type=request.resource_type,
                quantity=request.quantity,
                tier=CapacityTier.PARTNER_WHOLESALE,
                provider=partner_allocation["provider"].value,
                start_time=request.start_time,
                duration=request.duration,
                cost=cost,
                status="reserved"
            )
            
        # Try futures market
        futures_price = await self._check_futures_market(
            request.resource_type,
            request.quantity,
            request.start_time
        )
        
        if futures_price:
            # Create futures contract
            contract = await self.futures_engine.create_futures_contract(
                creator_id=f"platform_{request.service_type.value}",
                resource_type=request.resource_type,
                quantity=request.quantity,
                delivery_start=request.start_time,
                duration_hours=int(request.duration.total_seconds() / 3600),
                contract_months=1,
                location_zone="us-east-1"
            )
            
            if contract:
                return CapacityAllocation(
                    allocation_id=f"ALLOC_{datetime.utcnow().timestamp()}",
                    request_id=request.request_id,
                    service_type=request.service_type,
                    resource_type=request.resource_type,
                    quantity=request.quantity,
                    tier=CapacityTier.SPOT_MARKET,
                    provider=None,
                    start_time=request.start_time,
                    duration=request.duration,
                    cost=futures_price * request.quantity,
                    status="reserved",
                    metadata={"futures_contract_id": contract["id"]}
                )
                
        return None
        
    async def _optimize_flexible_request(
        self,
        request: ServiceCapacityRequest
    ):
        """Optimize allocation for flexible requests"""
        best_allocation = None
        best_cost = Decimal("999999")
        
        # Try different time slots within flexibility window
        for hour_offset in range(request.flexibility_hours):
            adjusted_start = request.start_time + timedelta(hours=hour_offset)
            
            # Check cost at this time
            cost = await self._estimate_cost_at_time(
                request.resource_type,
                request.quantity,
                adjusted_start,
                request.duration
            )
            
            if cost < best_cost:
                best_cost = cost
                best_allocation = CapacityAllocation(
                    allocation_id=f"ALLOC_{datetime.utcnow().timestamp()}",
                    request_id=request.request_id,
                    service_type=request.service_type,
                    resource_type=request.resource_type,
                    quantity=request.quantity,
                    tier=CapacityTier.SPOT_MARKET,
                    provider=None,
                    start_time=adjusted_start,
                    duration=request.duration,
                    cost=cost,
                    status="reserved"
                )
                
        if best_allocation and best_cost < Decimal("999999"):
            # Execute allocation
            self.allocations[best_allocation.allocation_id] = best_allocation
            await self._store_allocation(best_allocation)
            
            # Remove from pending
            self.pending_requests = [
                r for r in self.pending_requests
                if r.request_id != request.request_id
            ]
            
            # Notify service
            await self._notify_service(request.service_type, best_allocation)
            
            logger.info(f"Optimized flexible request {request.request_id} with {hour_offset}h shift, saved ${best_cost}")
            
    async def _notify_service(
        self,
        service_type: ServiceType,
        allocation: CapacityAllocation
    ):
        """Notify service about allocation"""
        client = self.service_clients.get(service_type)
        if not client:
            return
            
        try:
            # Service-specific notification
            if service_type == ServiceType.MLOPS:
                await self.mlops_client.notify_compute_reserved(
                    allocation.metadata.get("tenant_id", "platform"),
                    allocation.allocation_id,
                    allocation.resource_type,
                    int(allocation.quantity),
                    allocation.start_time,
                    int(allocation.duration.total_seconds() / 3600)
                )
            else:
                # Generic notification
                await client.post(
                    "/api/v1/capacity/allocation-confirmed",
                    json={
                        "allocation_id": allocation.allocation_id,
                        "request_id": allocation.request_id,
                        "resource_type": allocation.resource_type,
                        "quantity": str(allocation.quantity),
                        "start_time": allocation.start_time.isoformat(),
                        "duration_hours": allocation.duration.total_seconds() / 3600,
                        "cost": str(allocation.cost)
                    }
                )
                
            # Publish event
            await self.pulsar.publish(
                "persistent://platformq/capacity/allocation-confirmed",
                {
                    "allocation_id": allocation.allocation_id,
                    "service_type": service_type.value,
                    "resource_type": allocation.resource_type,
                    "quantity": str(allocation.quantity),
                    "tier": allocation.tier.value,
                    "provider": allocation.provider,
                    "start_time": allocation.start_time.isoformat(),
                    "cost": str(allocation.cost)
                }
            )
            
        except Exception as e:
            logger.error(f"Error notifying service {service_type}: {e}")
            
    async def _coordination_loop(self):
        """Main coordination loop"""
        while True:
            try:
                # Process pending requests
                await self._process_pending_requests()
                
                # Update allocation statuses
                await self._update_allocation_statuses()
                
                # Check for capacity issues
                await self._check_capacity_health()
                
                await asyncio.sleep(60)  # Run every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in coordination loop: {e}")
                await asyncio.sleep(30)
                
    async def _forecasting_loop(self):
        """Forecast capacity demand"""
        while True:
            try:
                # Get scheduled jobs from services
                for service_type in ServiceType:
                    if service_type == ServiceType.MLOPS:
                        # Get ML training schedule
                        jobs = await self.mlops_client.get_scheduled_training_jobs(
                            "all",  # All tenants
                            days_ahead=7
                        )
                        
                        for job in jobs:
                            await self._update_forecast_from_job(
                                service_type,
                                job
                            )
                            
                    # Add other service-specific forecasting
                    
                # Generate aggregated forecasts
                await self._generate_aggregate_forecasts()
                
                # Publish forecasts
                await self._publish_forecasts()
                
                await asyncio.sleep(3600)  # Run hourly
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in forecasting loop: {e}")
                await asyncio.sleep(300)
                
    async def _optimization_loop(self):
        """Optimize capacity purchases based on forecasts"""
        while True:
            try:
                # Get current and forecasted demand
                demand_forecast = await self._aggregate_demand_forecast()
                
                # Get current capacity
                current_capacity = await self._get_total_available_capacity()
                
                # Identify gaps
                capacity_gaps = await self._identify_capacity_gaps(
                    demand_forecast,
                    current_capacity
                )
                
                # Purchase additional capacity if needed
                for gap in capacity_gaps:
                    if gap["severity"] > 0.8:  # High severity
                        await self._purchase_capacity_for_gap(gap)
                        
                # Optimize existing allocations
                await self.optimize_capacity_allocation()
                
                await asyncio.sleep(1800)  # Run every 30 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in optimization loop: {e}")
                await asyncio.sleep(600)
                
    async def _check_platform_capacity(
        self,
        resource_type: str,
        quantity: Decimal,
        start_time: datetime,
        duration: timedelta
    ) -> Optional[Dict[str, Any]]:
        """Check platform-owned capacity availability"""
        # In practice, check internal resource manager
        # For now, mock response
        return None
        
    async def _check_futures_market(
        self,
        resource_type: str,
        quantity: Decimal,
        delivery_time: datetime
    ) -> Optional[Decimal]:
        """Check futures market prices"""
        try:
            market = await self.futures_engine.get_day_ahead_market(
                delivery_time,
                resource_type
            )
            
            if market:
                hour = delivery_time.hour
                return market.cleared_prices.get(hour)
                
        except Exception as e:
            logger.error(f"Error checking futures market: {e}")
            
        return None
        
    async def _estimate_cost_at_time(
        self,
        resource_type: str,
        quantity: Decimal,
        start_time: datetime,
        duration: timedelta
    ) -> Decimal:
        """Estimate cost for capacity at specific time"""
        # Check multiple sources and return best price
        costs = []
        
        # Partner wholesale
        provider, wholesale_price = await self.partner_manager.get_best_price(
            resource_type,
            "us-east-1",
            quantity,
            duration
        )
        if wholesale_price:
            costs.append(wholesale_price * quantity * Decimal(str(duration.total_seconds() / 3600)))
            
        # Futures market
        futures_price = await self._check_futures_market(
            resource_type,
            quantity,
            start_time
        )
        if futures_price:
            costs.append(futures_price * quantity * Decimal(str(duration.total_seconds() / 3600)))
            
        return min(costs) if costs else Decimal("999999")
        
    async def _get_service_usage(
        self,
        service_type: ServiceType,
        tenant_id: str
    ) -> Dict[str, Any]:
        """Get current usage for a service"""
        # Aggregate current allocations
        usage = {
            "active_allocations": 0,
            "total_quantity": Decimal("0"),
            "total_cost": Decimal("0")
        }
        
        for allocation in self.allocations.values():
            if (allocation.service_type == service_type and
                allocation.metadata.get("tenant_id") == tenant_id and
                allocation.status in ["reserved", "active"]):
                usage["active_allocations"] += 1
                usage["total_quantity"] += allocation.quantity
                usage["total_cost"] += allocation.cost
                
        return usage
        
    async def _store_allocation(self, allocation: CapacityAllocation):
        """Store allocation in cache"""
        await self.ignite.set(
            f"capacity_allocation:{allocation.allocation_id}",
            allocation.__dict__
        )
        
    async def _store_pending_request(self, request: ServiceCapacityRequest):
        """Store pending request in cache"""
        await self.ignite.set(
            f"capacity_request:{request.request_id}",
            request.__dict__
        )
        
    async def _load_historical_usage(self):
        """Load historical usage data"""
        # In practice, load from database
        pass
        
    async def _process_pending_requests(self):
        """Process pending capacity requests"""
        for request in list(self.pending_requests):
            # Check if we can allocate now
            allocation = await self._try_immediate_allocation(request)
            
            if allocation:
                self.allocations[allocation.allocation_id] = allocation
                await self._store_allocation(allocation)
                await self._notify_service(request.service_type, allocation)
                
                # Remove from pending
                self.pending_requests.remove(request)
                
    async def _update_allocation_statuses(self):
        """Update status of active allocations"""
        current_time = datetime.utcnow()
        
        for allocation in self.allocations.values():
            if allocation.status == "reserved" and allocation.start_time <= current_time:
                allocation.status = "active"
                await self._store_allocation(allocation)
                
            elif allocation.status == "active" and allocation.start_time + allocation.duration <= current_time:
                allocation.status = "completed"
                await self._store_allocation(allocation)
                
                # Record usage for billing
                await self._record_usage(allocation)
                
    async def _check_capacity_health(self):
        """Check overall capacity health"""
        # Calculate utilization
        total_allocated = sum(
            a.quantity for a in self.allocations.values()
            if a.status in ["reserved", "active"]
        )
        
        # Get total available
        available = await self.partner_manager.get_available_inventory("gpu")
        total_available = sum(inv.available_capacity for inv in available)
        
        utilization = float(total_allocated / (total_allocated + total_available)) if total_available > 0 else 1.0
        
        if utilization > 0.9:
            logger.warning(f"High capacity utilization: {utilization * 100:.1f}%")
            
            # Trigger capacity expansion
            await self.pulsar.publish(
                "persistent://platformq/capacity/high-utilization-alert",
                {
                    "utilization": utilization,
                    "total_allocated": str(total_allocated),
                    "total_available": str(total_available),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
    async def _update_forecast_from_job(
        self,
        service_type: ServiceType,
        job: Dict[str, Any]
    ):
        """Update forecast based on scheduled job"""
        # Extract resource requirements
        resource_type = job.get("gpu_type", "gpu")
        quantity = Decimal(str(job.get("gpu_count", 1)))
        start_time = datetime.fromisoformat(job["scheduled_time"])
        duration = timedelta(hours=job.get("estimated_duration_hours", 4))
        
        # Create or update forecast
        key = f"{service_type.value}:{resource_type}:{start_time.date()}"
        
        if key not in self.forecasts:
            self.forecasts[key] = CapacityForecast(
                service_type=service_type,
                resource_type=resource_type,
                time_window=start_time.replace(hour=0, minute=0, second=0),
                expected_demand=quantity,
                confidence=0.9,
                peak_demand=quantity,
                average_demand=quantity
            )
        else:
            forecast = self.forecasts[key]
            forecast.expected_demand += quantity
            forecast.peak_demand = max(forecast.peak_demand, forecast.expected_demand)
            
    async def _generate_aggregate_forecasts(self):
        """Generate aggregated capacity forecasts"""
        # Aggregate by resource type and time window
        aggregated = {}
        
        for forecast in self.forecasts.values():
            key = f"{forecast.resource_type}:{forecast.time_window.date()}"
            
            if key not in aggregated:
                aggregated[key] = {
                    "resource_type": forecast.resource_type,
                    "date": forecast.time_window.date(),
                    "total_demand": Decimal("0"),
                    "services": []
                }
                
            aggregated[key]["total_demand"] += forecast.expected_demand
            aggregated[key]["services"].append(forecast.service_type.value)
            
        # Store aggregated forecasts
        for key, data in aggregated.items():
            await self.ignite.set(f"capacity_forecast:{key}", data)
            
    async def _publish_forecasts(self):
        """Publish capacity forecasts"""
        forecasts_data = []
        
        for forecast in self.forecasts.values():
            forecasts_data.append({
                "service_type": forecast.service_type.value,
                "resource_type": forecast.resource_type,
                "date": forecast.time_window.date().isoformat(),
                "expected_demand": str(forecast.expected_demand),
                "confidence": forecast.confidence,
                "peak_demand": str(forecast.peak_demand)
            })
            
        await self.pulsar.publish(
            "persistent://platformq/capacity/forecast-update",
            {
                "timestamp": datetime.utcnow().isoformat(),
                "forecasts": forecasts_data
            }
        )
        
    async def _aggregate_demand_forecast(self) -> List[Dict[str, Any]]:
        """Aggregate demand forecast across all services"""
        # Group forecasts by date and resource type
        aggregated = {}
        
        for forecast in self.forecasts.values():
            key = (forecast.time_window.date(), forecast.resource_type)
            
            if key not in aggregated:
                aggregated[key] = {
                    "date": forecast.time_window.date(),
                    "resource_type": forecast.resource_type,
                    "total_demand": Decimal("0"),
                    "peak_demand": Decimal("0")
                }
                
            aggregated[key]["total_demand"] += forecast.expected_demand
            aggregated[key]["peak_demand"] = max(
                aggregated[key]["peak_demand"],
                forecast.peak_demand
            )
            
        return list(aggregated.values())
        
    async def _get_total_available_capacity(self) -> Dict[str, Decimal]:
        """Get total available capacity by resource type"""
        capacity = {}
        
        # Get partner inventory
        for resource_type in ["gpu", "cpu", "memory", "storage"]:
            inventory = await self.partner_manager.get_available_inventory(resource_type)
            total = sum(inv.available_capacity for inv in inventory)
            capacity[resource_type] = total
            
        return capacity
        
    async def _identify_capacity_gaps(
        self,
        demand_forecast: List[Dict[str, Any]],
        current_capacity: Dict[str, Decimal]
    ) -> List[Dict[str, Any]]:
        """Identify capacity gaps"""
        gaps = []
        
        for forecast in demand_forecast:
            resource_type = forecast["resource_type"]
            demand = forecast["total_demand"]
            available = current_capacity.get(resource_type, Decimal("0"))
            
            if demand > available:
                gap = demand - available
                severity = float(gap / demand) if demand > 0 else 0
                
                gaps.append({
                    "date": forecast["date"],
                    "resource_type": resource_type,
                    "gap_size": gap,
                    "severity": severity,
                    "demand": demand,
                    "available": available
                })
                
        return gaps
        
    async def _purchase_capacity_for_gap(self, gap: Dict[str, Any]):
        """Purchase additional capacity to fill gap"""
        logger.info(f"Purchasing capacity to fill gap: {gap}")
        
        # Determine best source
        provider, price = await self.partner_manager.get_best_price(
            gap["resource_type"],
            "us-east-1",  # TODO: Make region dynamic
            gap["gap_size"],
            timedelta(days=1)
        )
        
        if provider:
            # Purchase capacity
            order = await self.partner_manager.purchase_capacity(
                provider,
                gap["resource_type"],
                "us-east-1",
                gap["gap_size"],
                timedelta(days=1),
                gap["date"]
            )
            
            if order.status == "confirmed":
                logger.info(f"Successfully purchased {gap['gap_size']} {gap['resource_type']} from {provider.value}")
            else:
                logger.error(f"Failed to purchase capacity: {order}")
                
    async def _record_usage(self, allocation: CapacityAllocation):
        """Record usage for billing"""
        usage_record = {
            "allocation_id": allocation.allocation_id,
            "service_type": allocation.service_type.value,
            "tenant_id": allocation.metadata.get("tenant_id", "platform"),
            "resource_type": allocation.resource_type,
            "quantity": str(allocation.quantity),
            "duration_hours": allocation.duration.total_seconds() / 3600,
            "cost": str(allocation.cost),
            "tier": allocation.tier.value,
            "provider": allocation.provider,
            "start_time": allocation.start_time.isoformat(),
            "end_time": (allocation.start_time + allocation.duration).isoformat()
        }
        
        # Store in usage history
        await self.ignite.set(
            f"capacity_usage:{allocation.allocation_id}",
            usage_record
        )
        
        # Publish for billing
        await self.pulsar.publish(
            "persistent://platformq/billing/capacity-usage",
            usage_record
        ) 