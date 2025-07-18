"""
Physical Settlement Engine for Compute Futures

Handles the actual delivery of compute resources when contracts expire
"""

from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import httpx
import json
from collections import defaultdict
import uuid

# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


class SettlementStatus(Enum):
    """Status of physical settlement"""
    PENDING = "pending"
    PROVISIONING = "provisioning"
    ALLOCATED = "allocated"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    DISPUTED = "disputed"


class ResourceStatus(Enum):
    """Status of compute resources"""
    AVAILABLE = "available"
    RESERVED = "reserved"
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    FAILED = "failed"


@dataclass
class ComputeResource:
    """Represents a compute resource"""
    resource_id: str
    provider_id: str
    resource_type: str  # GPU, CPU, etc
    specifications: Dict[str, Any]
    location: str
    status: ResourceStatus
    allocated_to: Optional[str] = None
    allocation_start: Optional[datetime] = None
    allocation_end: Optional[datetime] = None
    access_details: Dict[str, Any] = field(default_factory=dict)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    cost_per_hour: Decimal = Decimal("0")
    
    @property
    def is_available(self) -> bool:
        return self.status == ResourceStatus.AVAILABLE
        
    @property
    def utilization_rate(self) -> Decimal:
        """Calculate resource utilization"""
        if "cpu_usage" in self.performance_metrics:
            return Decimal(str(self.performance_metrics["cpu_usage"]))
        return Decimal("0")


@dataclass
class Settlement:
    """Physical settlement record"""
    settlement_id: str
    contract_id: str
    buyer_id: str
    seller_id: str
    resource_type: str
    quantity: Decimal
    delivery_time: datetime
    duration: timedelta
    status: SettlementStatus
    resources: List[ComputeResource] = field(default_factory=list)
    
    # Settlement details
    settlement_price: Decimal = Decimal("0")
    actual_cost: Decimal = Decimal("0")
    
    # Performance tracking
    sla_requirements: Dict[str, Any] = field(default_factory=dict)
    sla_violations: List[Dict[str, Any]] = field(default_factory=list)
    performance_score: Decimal = Decimal("100")  # 0-100
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    provisioning_started: Optional[datetime] = None
    provisioning_completed: Optional[datetime] = None
    activation_time: Optional[datetime] = None
    completion_time: Optional[datetime] = None
    
    # Dispute handling
    dispute_reason: Optional[str] = None
    dispute_resolution: Optional[str] = None
    
    @property
    def is_overdue(self) -> bool:
        """Check if settlement is overdue"""
        return (self.status == SettlementStatus.PENDING and 
                datetime.utcnow() > self.delivery_time)
                
    @property
    def time_to_delivery(self) -> timedelta:
        """Time remaining to delivery"""
        return self.delivery_time - datetime.utcnow()


@dataclass
class ResourceProvider:
    """Compute resource provider"""
    provider_id: str
    name: str
    provider_type: str  # platform, partner, p2p
    api_endpoint: str
    api_credentials: Dict[str, str]
    
    # Capacity
    total_resources: Dict[str, int] = field(default_factory=dict)
    available_resources: Dict[str, int] = field(default_factory=dict)
    
    # Pricing
    pricing: Dict[str, Decimal] = field(default_factory=dict)
    
    # Performance
    avg_provisioning_time: timedelta = timedelta(minutes=5)
    success_rate: Decimal = Decimal("0.99")
    sla_compliance: Decimal = Decimal("0.95")
    
    # Status
    is_active: bool = True
    last_health_check: datetime = field(default_factory=datetime.utcnow)


class PhysicalSettlementEngine:
    """Handles physical delivery of compute resources"""
    
    def __init__(self, ignite_cache=None, pulsar_publisher=None,
                 provisioning_service_url: str = "http://provisioning-service:8000"):
        self.cache = ignite_cache
        self.pulsar = pulsar_publisher
        self.provisioning_url = provisioning_service_url
        
        # Active settlements
        self.settlements: Dict[str, Settlement] = {}
        
        # Resource inventory
        self.resources: Dict[str, ComputeResource] = {}
        self.providers: Dict[str, ResourceProvider] = {}
        
        # Resource allocation tracking
        self.allocations: Dict[str, Set[str]] = defaultdict(set)  # user -> resource_ids
        
        # Performance tracking
        self.performance_history: Dict[str, List[Dict]] = defaultdict(list)
        
        # HTTP client for external APIs
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Background tasks
        self._running = False
        self._tasks = []
        
    async def start(self):
        """Start settlement engine"""
        self._running = True
        
        # Load providers and resources
        await self._load_providers()
        await self._inventory_resources()
        
        # Start background tasks
        self._tasks.append(asyncio.create_task(self._settlement_processor()))
        self._tasks.append(asyncio.create_task(self._resource_monitor()))
        self._tasks.append(asyncio.create_task(self._sla_monitor()))
        self._tasks.append(asyncio.create_task(self._cleanup_loop()))
        
        logger.info("Physical settlement engine started")
        
    async def stop(self):
        """Stop settlement engine"""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
            
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.http_client.aclose()
        
    async def initiate_settlement(
        self,
        contract_id: str,
        buyer_id: str,
        seller_id: str,
        resource_type: str,
        quantity: Decimal,
        specifications: Dict[str, Any],
        delivery_time: datetime,
        duration: timedelta,
        settlement_price: Decimal,
        sla_requirements: Optional[Dict[str, Any]] = None
    ) -> Settlement:
        """Initiate physical settlement for expired contract"""
        
        settlement_id = f"settlement_{contract_id}_{datetime.utcnow().timestamp()}"
        
        settlement = Settlement(
            settlement_id=settlement_id,
            contract_id=contract_id,
            buyer_id=buyer_id,
            seller_id=seller_id,
            resource_type=resource_type,
            quantity=quantity,
            delivery_time=delivery_time,
            duration=duration,
            status=SettlementStatus.PENDING,
            settlement_price=settlement_price,
            sla_requirements=sla_requirements or {}
        )
        
        self.settlements[settlement_id] = settlement
        
        # Store in cache
        if self.cache:
            await self.cache.set(f"settlement:{settlement_id}", settlement)
            
        # Emit event
        await self._emit_settlement_event("initiated", settlement)
        
        # Start provisioning if delivery time is near
        if settlement.time_to_delivery <= timedelta(hours=1):
            asyncio.create_task(self._provision_resources(settlement))
            
        return settlement
        
    async def _provision_resources(self, settlement: Settlement):
        """Provision compute resources for settlement"""
        
        try:
            settlement.status = SettlementStatus.PROVISIONING
            settlement.provisioning_started = datetime.utcnow()
            
            # Find available resources
            required_resources = await self._find_matching_resources(
                settlement.resource_type,
                settlement.quantity,
                settlement.sla_requirements
            )
            
            if not required_resources:
                # Try to provision from partners
                required_resources = await self._provision_from_partners(
                    settlement.resource_type,
                    settlement.quantity,
                    settlement.sla_requirements
                )
                
            if not required_resources:
                raise Exception("Insufficient resources available")
                
            # Allocate resources
            allocated_resources = []
            
            for resource in required_resources:
                # Reserve resource
                resource.status = ResourceStatus.RESERVED
                resource.allocated_to = settlement.buyer_id
                resource.allocation_start = settlement.delivery_time
                resource.allocation_end = settlement.delivery_time + settlement.duration
                
                # Call provisioning service
                access_details = await self._call_provisioning_service(
                    resource, settlement.buyer_id
                )
                
                resource.access_details = access_details
                resource.status = ResourceStatus.PROVISIONING
                
                allocated_resources.append(resource)
                self.allocations[settlement.buyer_id].add(resource.resource_id)
                
            settlement.resources = allocated_resources
            settlement.status = SettlementStatus.ALLOCATED
            settlement.provisioning_completed = datetime.utcnow()
            
            # Calculate actual cost
            settlement.actual_cost = sum(
                r.cost_per_hour * (settlement.duration.total_seconds() / 3600)
                for r in allocated_resources
            )
            
            # Emit success event
            await self._emit_settlement_event("provisioned", settlement)
            
        except Exception as e:
            logger.error(f"Failed to provision resources for {settlement.settlement_id}: {e}")
            settlement.status = SettlementStatus.FAILED
            await self._handle_settlement_failure(settlement, str(e))
            
    async def _find_matching_resources(
        self,
        resource_type: str,
        quantity: Decimal,
        requirements: Dict[str, Any]
    ) -> List[ComputeResource]:
        """Find available resources matching requirements"""
        
        matching_resources = []
        
        for resource in self.resources.values():
            if (resource.resource_type == resource_type and
                resource.is_available and
                self._meets_requirements(resource, requirements)):
                
                matching_resources.append(resource)
                
                if len(matching_resources) >= int(quantity):
                    break
                    
        return matching_resources[:int(quantity)]
        
    def _meets_requirements(self, resource: ComputeResource, requirements: Dict[str, Any]) -> bool:
        """Check if resource meets requirements"""
        
        # Check location
        if "location" in requirements:
            if resource.location != requirements["location"]:
                return False
                
        # Check specifications
        for key, required_value in requirements.get("specifications", {}).items():
            if key not in resource.specifications:
                return False
                
            resource_value = resource.specifications[key]
            
            # Handle different comparison types
            if isinstance(required_value, dict):
                if "min" in required_value and resource_value < required_value["min"]:
                    return False
                if "max" in required_value and resource_value > required_value["max"]:
                    return False
            elif resource_value != required_value:
                return False
                
        return True
        
    async def _provision_from_partners(
        self,
        resource_type: str,
        quantity: Decimal,
        requirements: Dict[str, Any]
    ) -> List[ComputeResource]:
        """Provision resources from partner providers"""
        
        provisioned_resources = []
        
        # Sort providers by price and availability
        suitable_providers = [
            p for p in self.providers.values()
            if p.is_active and resource_type in p.available_resources
        ]
        
        suitable_providers.sort(key=lambda p: p.pricing.get(resource_type, Decimal("999999")))
        
        for provider in suitable_providers:
            if provider.available_resources.get(resource_type, 0) >= int(quantity):
                # Call provider API to provision
                resources = await self._call_provider_api(
                    provider, resource_type, int(quantity), requirements
                )
                
                if resources:
                    provisioned_resources.extend(resources)
                    break
                    
        return provisioned_resources
        
    async def _call_provisioning_service(
        self,
        resource: ComputeResource,
        user_id: str
    ) -> Dict[str, Any]:
        """Call provisioning service to allocate resource"""
        
        try:
            response = await self.http_client.post(
                f"{self.provisioning_url}/api/v1/compute/provision",
                json={
                    "user_id": user_id,
                    "resource_type": resource.resource_type,
                    "specifications": resource.specifications,
                    "location": resource.location,
                    "duration_hours": 1  # Will be extended as needed
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("access_details", {})
            else:
                raise Exception(f"Provisioning failed: {response.text}")
                
        except Exception as e:
            logger.error(f"Error calling provisioning service: {e}")
            raise
            
    async def _call_provider_api(
        self,
        provider: ResourceProvider,
        resource_type: str,
        quantity: int,
        requirements: Dict[str, Any]
    ) -> List[ComputeResource]:
        """Call external provider API to provision resources"""
        
        # This would integrate with different provider APIs
        # For now, simulate the provisioning
        
        resources = []
        
        for i in range(quantity):
            resource = ComputeResource(
                resource_id=f"{provider.provider_id}_{resource_type}_{uuid.uuid4()}",
                provider_id=provider.provider_id,
                resource_type=resource_type,
                specifications=requirements.get("specifications", {}),
                location=requirements.get("location", "us-east-1"),
                status=ResourceStatus.AVAILABLE,
                cost_per_hour=provider.pricing.get(resource_type, Decimal("10"))
            )
            
            resources.append(resource)
            self.resources[resource.resource_id] = resource
            
        # Update provider availability
        provider.available_resources[resource_type] -= quantity
        
        return resources
        
    async def activate_settlement(self, settlement_id: str):
        """Activate resources for settlement"""
        
        settlement = self.settlements.get(settlement_id)
        if not settlement:
            return
            
        if settlement.status != SettlementStatus.ALLOCATED:
            return
            
        # Activate all resources
        for resource in settlement.resources:
            resource.status = ResourceStatus.ACTIVE
            
        settlement.status = SettlementStatus.ACTIVE
        settlement.activation_time = datetime.utcnow()
        
        # Start monitoring
        asyncio.create_task(self._monitor_settlement_performance(settlement))
        
        # Emit activation event
        await self._emit_settlement_event("activated", settlement)
        
    async def complete_settlement(self, settlement_id: str):
        """Complete settlement and release resources"""
        
        settlement = self.settlements.get(settlement_id)
        if not settlement:
            return
            
        # Terminate resources
        for resource in settlement.resources:
            resource.status = ResourceStatus.TERMINATING
            
            # Call termination API
            await self._terminate_resource(resource)
            
            resource.status = ResourceStatus.TERMINATED
            resource.allocated_to = None
            
            # Remove from allocations
            self.allocations[settlement.buyer_id].discard(resource.resource_id)
            
        settlement.status = SettlementStatus.COMPLETED
        settlement.completion_time = datetime.utcnow()
        
        # Calculate final performance score
        settlement.performance_score = self._calculate_performance_score(settlement)
        
        # Handle any refunds/penalties
        await self._process_settlement_adjustments(settlement)
        
        # Emit completion event
        await self._emit_settlement_event("completed", settlement)
        
    async def report_sla_violation(
        self,
        settlement_id: str,
        violation_type: str,
        details: Dict[str, Any]
    ):
        """Report SLA violation for settlement"""
        
        settlement = self.settlements.get(settlement_id)
        if not settlement:
            return
            
        violation = {
            "type": violation_type,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details,
            "impact": self._calculate_violation_impact(violation_type, details)
        }
        
        settlement.sla_violations.append(violation)
        
        # Update performance score
        settlement.performance_score *= Decimal("0.95")  # 5% penalty per violation
        
        # Check if critical violation
        if violation["impact"] == "critical":
            await self._handle_critical_violation(settlement, violation)
            
    async def initiate_dispute(
        self,
        settlement_id: str,
        disputing_party: str,
        reason: str,
        evidence: Optional[Dict[str, Any]] = None
    ):
        """Initiate dispute for settlement"""
        
        settlement = self.settlements.get(settlement_id)
        if not settlement:
            return
            
        settlement.status = SettlementStatus.DISPUTED
        settlement.dispute_reason = reason
        
        # Pause any active resources
        for resource in settlement.resources:
            if resource.status == ResourceStatus.ACTIVE:
                resource.status = ResourceStatus.RESERVED
                
        # Emit dispute event
        await self._emit_settlement_event("disputed", settlement, {
            "disputing_party": disputing_party,
            "reason": reason,
            "evidence": evidence
        })
        
        # Start dispute resolution process
        asyncio.create_task(self._resolve_dispute(settlement, disputing_party, evidence))
        
    async def _monitor_settlement_performance(self, settlement: Settlement):
        """Monitor performance of active settlement"""
        
        while settlement.status == SettlementStatus.ACTIVE and self._running:
            try:
                # Collect performance metrics
                metrics = await self._collect_performance_metrics(settlement)
                
                # Check SLA compliance
                violations = self._check_sla_compliance(metrics, settlement.sla_requirements)
                
                for violation in violations:
                    await self.report_sla_violation(
                        settlement.settlement_id,
                        violation["type"],
                        violation["details"]
                    )
                    
                # Store metrics
                self.performance_history[settlement.settlement_id].append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "metrics": metrics,
                    "violations": violations
                })
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error monitoring settlement {settlement.settlement_id}: {e}")
                await asyncio.sleep(300)  # Retry after 5 minutes
                
    async def _collect_performance_metrics(self, settlement: Settlement) -> Dict[str, Any]:
        """Collect performance metrics for resources"""
        
        metrics = {
            "resources": [],
            "aggregate": {
                "avg_cpu_usage": Decimal("0"),
                "avg_memory_usage": Decimal("0"),
                "avg_network_latency": Decimal("0"),
                "uptime_percentage": Decimal("100")
            }
        }
        
        for resource in settlement.resources:
            # In production, this would query actual monitoring systems
            resource_metrics = {
                "resource_id": resource.resource_id,
                "cpu_usage": Decimal("75"),  # Simulated
                "memory_usage": Decimal("60"),
                "network_latency": Decimal("10"),  # ms
                "is_available": resource.status == ResourceStatus.ACTIVE
            }
            
            metrics["resources"].append(resource_metrics)
            
        # Calculate aggregates
        if metrics["resources"]:
            num_resources = len(metrics["resources"])
            metrics["aggregate"]["avg_cpu_usage"] = sum(
                m["cpu_usage"] for m in metrics["resources"]
            ) / num_resources
            
            metrics["aggregate"]["avg_memory_usage"] = sum(
                m["memory_usage"] for m in metrics["resources"]
            ) / num_resources
            
            metrics["aggregate"]["avg_network_latency"] = sum(
                m["network_latency"] for m in metrics["resources"]
            ) / num_resources
            
            available_count = sum(1 for m in metrics["resources"] if m["is_available"])
            metrics["aggregate"]["uptime_percentage"] = (
                Decimal(str(available_count)) / Decimal(str(num_resources)) * 100
            )
            
        return metrics
        
    def _check_sla_compliance(
        self,
        metrics: Dict[str, Any],
        sla_requirements: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Check if metrics meet SLA requirements"""
        
        violations = []
        
        # Check uptime
        if "min_uptime" in sla_requirements:
            if metrics["aggregate"]["uptime_percentage"] < sla_requirements["min_uptime"]:
                violations.append({
                    "type": "uptime",
                    "details": {
                        "required": sla_requirements["min_uptime"],
                        "actual": metrics["aggregate"]["uptime_percentage"]
                    }
                })
                
        # Check latency
        if "max_latency" in sla_requirements:
            if metrics["aggregate"]["avg_network_latency"] > sla_requirements["max_latency"]:
                violations.append({
                    "type": "latency",
                    "details": {
                        "required": sla_requirements["max_latency"],
                        "actual": metrics["aggregate"]["avg_network_latency"]
                    }
                })
                
        # Check performance
        if "max_cpu_usage" in sla_requirements:
            if metrics["aggregate"]["avg_cpu_usage"] > sla_requirements["max_cpu_usage"]:
                violations.append({
                    "type": "cpu_overload",
                    "details": {
                        "required": sla_requirements["max_cpu_usage"],
                        "actual": metrics["aggregate"]["avg_cpu_usage"]
                    }
                })
                
        return violations
        
    def _calculate_violation_impact(self, violation_type: str, details: Dict[str, Any]) -> str:
        """Calculate impact level of SLA violation"""
        
        if violation_type == "uptime":
            uptime = details.get("actual", 100)
            if uptime < 90:
                return "critical"
            elif uptime < 95:
                return "major"
            else:
                return "minor"
                
        elif violation_type == "latency":
            latency_ratio = details.get("actual", 0) / details.get("required", 1)
            if latency_ratio > 2:
                return "critical"
            elif latency_ratio > 1.5:
                return "major"
            else:
                return "minor"
                
        return "minor"
        
    def _calculate_performance_score(self, settlement: Settlement) -> Decimal:
        """Calculate final performance score"""
        
        base_score = Decimal("100")
        
        # Deduct for violations
        violation_penalty = len(settlement.sla_violations) * Decimal("5")
        
        # Deduct for critical violations
        critical_violations = sum(
            1 for v in settlement.sla_violations
            if self._calculate_violation_impact(v["type"], v["details"]) == "critical"
        )
        critical_penalty = critical_violations * Decimal("10")
        
        final_score = max(Decimal("0"), base_score - violation_penalty - critical_penalty)
        
        return final_score
        
    async def _process_settlement_adjustments(self, settlement: Settlement):
        """Process refunds/penalties based on performance"""
        
        if settlement.performance_score < Decimal("90"):
            # Calculate refund percentage
            refund_percentage = (Decimal("90") - settlement.performance_score) / Decimal("100")
            refund_amount = settlement.settlement_price * refund_percentage
            
            # In production, this would trigger actual refund
            logger.info(
                f"Processing refund of {refund_amount} for settlement {settlement.settlement_id}"
            )
            
            await self._emit_settlement_event("refund_processed", settlement, {
                "refund_amount": str(refund_amount),
                "reason": "SLA violations"
            })
            
    async def _handle_settlement_failure(self, settlement: Settlement, reason: str):
        """Handle failed settlement"""
        
        # Try failover providers
        failover_success = await self._attempt_failover(settlement)
        
        if not failover_success:
            # Trigger refund
            await self._emit_settlement_event("failed", settlement, {
                "reason": reason,
                "refund_amount": str(settlement.settlement_price)
            })
            
    async def _attempt_failover(self, settlement: Settlement) -> bool:
        """Attempt to provision from failover providers"""
        
        # Get failover providers
        failover_providers = [
            p for p in self.providers.values()
            if p.provider_id != settlement.resources[0].provider_id if settlement.resources else True
        ]
        
        for provider in failover_providers:
            try:
                resources = await self._provision_from_partners(
                    settlement.resource_type,
                    settlement.quantity,
                    settlement.sla_requirements
                )
                
                if resources:
                    settlement.resources = resources
                    settlement.status = SettlementStatus.ALLOCATED
                    return True
                    
            except Exception as e:
                logger.error(f"Failover attempt failed with {provider.name}: {e}")
                
        return False
        
    async def _terminate_resource(self, resource: ComputeResource):
        """Terminate a compute resource"""
        
        # In production, this would call actual termination APIs
        logger.info(f"Terminating resource {resource.resource_id}")
        
    async def _handle_critical_violation(self, settlement: Settlement, violation: Dict[str, Any]):
        """Handle critical SLA violation"""
        
        # Immediate notification
        await self._emit_settlement_event("critical_violation", settlement, violation)
        
        # Consider immediate termination and refund
        if settlement.performance_score < Decimal("50"):
            await self.complete_settlement(settlement.settlement_id)
            
    async def _resolve_dispute(
        self,
        settlement: Settlement,
        disputing_party: str,
        evidence: Optional[Dict[str, Any]]
    ):
        """Resolve settlement dispute"""
        
        # In production, this would involve more sophisticated arbitration
        # For now, simple resolution based on performance metrics
        
        if settlement.performance_score < Decimal("80"):
            # Rule in favor of buyer
            settlement.dispute_resolution = "Resolved in favor of buyer - performance below threshold"
            refund_amount = settlement.settlement_price * Decimal("0.5")
            
            await self._emit_settlement_event("dispute_resolved", settlement, {
                "winner": settlement.buyer_id,
                "refund_amount": str(refund_amount)
            })
        else:
            # Rule in favor of seller
            settlement.dispute_resolution = "Resolved in favor of seller - performance acceptable"
            
            await self._emit_settlement_event("dispute_resolved", settlement, {
                "winner": settlement.seller_id,
                "refund_amount": "0"
            })
            
        # Resume settlement
        settlement.status = SettlementStatus.ACTIVE
        
        for resource in settlement.resources:
            if resource.status == ResourceStatus.RESERVED:
                resource.status = ResourceStatus.ACTIVE
                
    async def _settlement_processor(self):
        """Background task to process pending settlements"""
        
        while self._running:
            try:
                # Process pending settlements
                for settlement in list(self.settlements.values()):
                    if (settlement.status == SettlementStatus.PENDING and
                        settlement.time_to_delivery <= timedelta(hours=1)):
                        
                        asyncio.create_task(self._provision_resources(settlement))
                        
                    elif (settlement.status == SettlementStatus.ALLOCATED and
                          datetime.utcnow() >= settlement.delivery_time):
                        
                        await self.activate_settlement(settlement.settlement_id)
                        
                    elif (settlement.status == SettlementStatus.ACTIVE and
                          datetime.utcnow() >= settlement.delivery_time + settlement.duration):
                        
                        await self.complete_settlement(settlement.settlement_id)
                        
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in settlement processor: {e}")
                await asyncio.sleep(60)
                
    async def _resource_monitor(self):
        """Monitor resource health and availability"""
        
        while self._running:
            try:
                # Check resource health
                for resource in self.resources.values():
                    if resource.status == ResourceStatus.ACTIVE:
                        # In production, would check actual health
                        pass
                        
                # Update provider availability
                for provider in self.providers.values():
                    if datetime.utcnow() - provider.last_health_check > timedelta(minutes=5):
                        await self._check_provider_health(provider)
                        
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in resource monitor: {e}")
                await asyncio.sleep(300)
                
    async def _sla_monitor(self):
        """Monitor SLA compliance across all settlements"""
        
        while self._running:
            try:
                active_settlements = [
                    s for s in self.settlements.values()
                    if s.status == SettlementStatus.ACTIVE
                ]
                
                for settlement in active_settlements:
                    # Already monitored individually, this is for aggregate metrics
                    pass
                    
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in SLA monitor: {e}")
                await asyncio.sleep(600)
                
    async def _cleanup_loop(self):
        """Clean up completed settlements"""
        
        while self._running:
            try:
                cutoff = datetime.utcnow() - timedelta(days=7)
                
                # Remove old completed settlements
                to_remove = []
                for sid, settlement in self.settlements.items():
                    if (settlement.status == SettlementStatus.COMPLETED and
                        settlement.completion_time and
                        settlement.completion_time < cutoff):
                        
                        to_remove.append(sid)
                        
                for sid in to_remove:
                    del self.settlements[sid]
                    
                    # Clean from cache
                    if self.cache:
                        await self.cache.delete(f"settlement:{sid}")
                        
                await asyncio.sleep(3600)  # Clean every hour
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(3600)
                
    async def _load_providers(self):
        """Load resource providers"""
        
        # In production, load from configuration
        # For now, create some sample providers
        
        platform_provider = ResourceProvider(
            provider_id="platform_internal",
            name="Platform Internal Resources",
            provider_type="platform",
            api_endpoint="internal",
            api_credentials={},
            total_resources={"GPU": 100, "CPU": 1000},
            available_resources={"GPU": 50, "CPU": 500},
            pricing={"GPU": Decimal("50"), "CPU": Decimal("5")}
        )
        
        self.providers[platform_provider.provider_id] = platform_provider
        
    async def _inventory_resources(self):
        """Inventory available resources"""
        
        # In production, would query actual resource pools
        # For now, create some sample resources
        
        for i in range(10):
            resource = ComputeResource(
                resource_id=f"gpu_{i}",
                provider_id="platform_internal",
                resource_type="GPU",
                specifications={
                    "model": "NVIDIA A100",
                    "memory_gb": 80,
                    "compute_capability": 8.0
                },
                location="us-east-1",
                status=ResourceStatus.AVAILABLE,
                cost_per_hour=Decimal("50")
            )
            
            self.resources[resource.resource_id] = resource
            
    async def _check_provider_health(self, provider: ResourceProvider):
        """Check health of resource provider"""
        
        # In production, would actually health check the provider
        provider.last_health_check = datetime.utcnow()
        
    async def _emit_settlement_event(self, event_type: str, settlement: Settlement,
                                   additional_data: Optional[Dict[str, Any]] = None):
        """Emit settlement event"""
        
        if self.pulsar:
            event_data = {
                "event_type": event_type,
                "settlement_id": settlement.settlement_id,
                "contract_id": settlement.contract_id,
                "status": settlement.status.value,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if additional_data:
                event_data.update(additional_data)
                
            await self.pulsar.publish(f"settlements.{event_type}", event_data)
            
    def get_settlement_summary(self) -> Dict[str, Any]:
        """Get summary of all settlements"""
        
        status_counts = defaultdict(int)
        for settlement in self.settlements.values():
            status_counts[settlement.status.value] += 1
            
        return {
            "total_settlements": len(self.settlements),
            "status_breakdown": dict(status_counts),
            "active_resources": sum(
                1 for r in self.resources.values()
                if r.status == ResourceStatus.ACTIVE
            ),
            "total_resources": len(self.resources),
            "providers": len(self.providers),
            "performance_stats": {
                "avg_performance_score": sum(
                    s.performance_score for s in self.settlements.values()
                ) / len(self.settlements) if self.settlements else Decimal("0"),
                "total_violations": sum(
                    len(s.sla_violations) for s in self.settlements.values()
                )
            }
        } 