"""
GPU Resource Manager

Manages GPU allocation and scheduling for model training and inference
"""

import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio
from enum import Enum
import httpx
from collections import defaultdict
import nvidia_ml_py as nvml

logger = logging.getLogger(__name__)


class GPUType(Enum):
    NVIDIA_T4 = "nvidia-t4"
    NVIDIA_V100 = "nvidia-v100"
    NVIDIA_A100 = "nvidia-a100"
    NVIDIA_A100_80GB = "nvidia-a100-80gb"
    NVIDIA_H100 = "nvidia-h100"
    NVIDIA_RTX_3090 = "nvidia-rtx-3090"
    NVIDIA_RTX_4090 = "nvidia-rtx-4090"


class AllocationPriority(Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class GPUNode:
    node_id: str
    hostname: str
    gpu_type: GPUType
    gpu_count: int
    gpu_memory_mb: int
    available_gpus: Set[int]
    allocated_gpus: Dict[int, str]  # GPU index -> allocation_id
    node_labels: Dict[str, str]
    last_heartbeat: datetime
    is_healthy: bool


@dataclass
class GPUAllocation:
    allocation_id: str
    tenant_id: str
    user_id: str
    purpose: str  # training, inference, experiment
    model_name: str
    gpu_type: GPUType
    gpu_count: int
    memory_per_gpu_mb: int
    node_id: str
    gpu_indices: List[int]
    priority: AllocationPriority
    created_at: datetime
    expires_at: Optional[datetime]
    actual_usage: Dict[str, Any]
    is_active: bool


@dataclass
class GPURequest:
    request_id: str
    tenant_id: str
    user_id: str
    model_name: str
    purpose: str
    gpu_type: Optional[GPUType]
    gpu_count: int
    memory_per_gpu_mb: int
    duration_hours: Optional[int]
    priority: AllocationPriority
    constraints: Dict[str, Any]


class GPUResourceManager:
    """Manages GPU resource allocation and scheduling"""
    
    def __init__(self,
                 kubernetes_api_url: Optional[str] = None,
                 monitoring_interval: int = 30):
        self.kubernetes_api_url = kubernetes_api_url
        self.monitoring_interval = monitoring_interval
        
        # GPU inventory
        self.gpu_nodes: Dict[str, GPUNode] = {}
        self.allocations: Dict[str, GPUAllocation] = {}
        self.pending_requests: List[GPURequest] = []
        
        # Resource limits per tenant
        self.tenant_limits: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "max_gpus": 10,
            "max_memory_gb": 320,
            "max_concurrent_training": 5,
            "max_concurrent_inference": 20
        })
        
        # Usage tracking
        self.usage_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # Monitoring
        self._monitoring_task: Optional[asyncio.Task] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        
        # Initialize NVML for GPU monitoring
        try:
            nvml.nvmlInit()
            self.nvml_initialized = True
        except:
            logger.warning("NVML initialization failed - GPU monitoring limited")
            self.nvml_initialized = False
            
    async def start(self):
        """Start GPU resource manager"""
        # Discover available GPUs
        await self._discover_gpu_nodes()
        
        # Start monitoring
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        
        logger.info("GPU Resource Manager started")
        
    async def stop(self):
        """Stop GPU resource manager"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._scheduler_task:
            self._scheduler_task.cancel()
            
        if self.nvml_initialized:
            nvml.nvmlShutdown()
            
        logger.info("GPU Resource Manager stopped")
        
    async def request_gpus(self, request: GPURequest) -> Optional[str]:
        """Request GPU allocation"""
        # Validate request
        if not await self._validate_request(request):
            return None
            
        # Check if resources available immediately
        allocation = await self._try_allocate(request)
        
        if allocation:
            self.allocations[allocation.allocation_id] = allocation
            await self._update_node_allocation(allocation)
            logger.info(f"Allocated GPUs immediately: {allocation.allocation_id}")
            return allocation.allocation_id
        else:
            # Queue request
            self.pending_requests.append(request)
            self.pending_requests.sort(key=lambda r: r.priority.value, reverse=True)
            logger.info(f"Queued GPU request: {request.request_id}")
            return None
            
    async def release_gpus(self, allocation_id: str):
        """Release GPU allocation"""
        if allocation_id not in self.allocations:
            logger.warning(f"Unknown allocation: {allocation_id}")
            return
            
        allocation = self.allocations[allocation_id]
        allocation.is_active = False
        
        # Update node availability
        node = self.gpu_nodes.get(allocation.node_id)
        if node:
            for gpu_idx in allocation.gpu_indices:
                node.available_gpus.add(gpu_idx)
                del node.allocated_gpus[gpu_idx]
                
        # Record usage
        self._record_usage(allocation)
        
        logger.info(f"Released GPU allocation: {allocation_id}")
        
    async def get_allocation_status(self, allocation_id: str) -> Optional[Dict[str, Any]]:
        """Get status of GPU allocation"""
        allocation = self.allocations.get(allocation_id)
        if not allocation:
            return None
            
        status = {
            "allocation_id": allocation_id,
            "status": "active" if allocation.is_active else "terminated",
            "gpu_type": allocation.gpu_type.value,
            "gpu_count": allocation.gpu_count,
            "node": allocation.node_id,
            "gpu_indices": allocation.gpu_indices,
            "created_at": allocation.created_at.isoformat(),
            "expires_at": allocation.expires_at.isoformat() if allocation.expires_at else None
        }
        
        # Add current GPU metrics if available
        if allocation.is_active and self.nvml_initialized:
            metrics = await self._get_gpu_metrics(allocation)
            status["current_metrics"] = metrics
            
        return status
        
    async def get_available_resources(self) -> Dict[str, Any]:
        """Get currently available GPU resources"""
        resources = defaultdict(lambda: {"count": 0, "memory_gb": 0, "nodes": []})
        
        for node in self.gpu_nodes.values():
            if node.is_healthy and node.available_gpus:
                gpu_type = node.gpu_type.value
                resources[gpu_type]["count"] += len(node.available_gpus)
                resources[gpu_type]["memory_gb"] += len(node.available_gpus) * (node.gpu_memory_mb / 1024)
                resources[gpu_type]["nodes"].append(node.node_id)
                
        return dict(resources)
        
    async def get_tenant_usage(self, tenant_id: str) -> Dict[str, Any]:
        """Get GPU usage for a tenant"""
        current_usage = {
            "active_allocations": 0,
            "total_gpus": 0,
            "total_memory_gb": 0,
            "by_purpose": defaultdict(int),
            "by_gpu_type": defaultdict(int)
        }
        
        for allocation in self.allocations.values():
            if allocation.tenant_id == tenant_id and allocation.is_active:
                current_usage["active_allocations"] += 1
                current_usage["total_gpus"] += allocation.gpu_count
                current_usage["total_memory_gb"] += allocation.gpu_count * (allocation.memory_per_gpu_mb / 1024)
                current_usage["by_purpose"][allocation.purpose] += allocation.gpu_count
                current_usage["by_gpu_type"][allocation.gpu_type.value] += allocation.gpu_count
                
        # Add limits
        current_usage["limits"] = self.tenant_limits[tenant_id]
        
        # Add usage history
        current_usage["history"] = self.usage_history.get(tenant_id, [])[-100:]  # Last 100 records
        
        return current_usage
        
    async def _discover_gpu_nodes(self):
        """Discover available GPU nodes"""
        if self.kubernetes_api_url:
            # Discover from Kubernetes
            await self._discover_k8s_gpu_nodes()
        else:
            # Discover local GPUs
            await self._discover_local_gpus()
            
    async def _discover_local_gpus(self):
        """Discover GPUs on local machine"""
        if not self.nvml_initialized:
            return
            
        device_count = nvml.nvmlDeviceGetCount()
        if device_count == 0:
            logger.warning("No GPUs found on local machine")
            return
            
        node_id = "local"
        gpu_indices = set(range(device_count))
        
        # Get GPU info from first device
        handle = nvml.nvmlDeviceGetHandleByIndex(0)
        name = nvml.nvmlDeviceGetName(handle)
        memory_info = nvml.nvmlDeviceGetMemoryInfo(handle)
        
        # Map GPU name to type
        gpu_type = self._map_gpu_name_to_type(name.decode('utf-8'))
        
        node = GPUNode(
            node_id=node_id,
            hostname="localhost",
            gpu_type=gpu_type,
            gpu_count=device_count,
            gpu_memory_mb=memory_info.total // (1024 * 1024),
            available_gpus=gpu_indices,
            allocated_gpus={},
            node_labels={"location": "local"},
            last_heartbeat=datetime.utcnow(),
            is_healthy=True
        )
        
        self.gpu_nodes[node_id] = node
        logger.info(f"Discovered {device_count} local GPUs of type {gpu_type.value}")
        
    async def _discover_k8s_gpu_nodes(self):
        """Discover GPU nodes from Kubernetes"""
        # Implementation would query Kubernetes API for nodes with GPU resources
        pass
        
    def _map_gpu_name_to_type(self, name: str) -> GPUType:
        """Map GPU name string to GPUType enum"""
        name_lower = name.lower()
        
        if "t4" in name_lower:
            return GPUType.NVIDIA_T4
        elif "v100" in name_lower:
            return GPUType.NVIDIA_V100
        elif "a100" in name_lower and "80gb" in name_lower:
            return GPUType.NVIDIA_A100_80GB
        elif "a100" in name_lower:
            return GPUType.NVIDIA_A100
        elif "h100" in name_lower:
            return GPUType.NVIDIA_H100
        elif "3090" in name_lower:
            return GPUType.NVIDIA_RTX_3090
        elif "4090" in name_lower:
            return GPUType.NVIDIA_RTX_4090
        else:
            return GPUType.NVIDIA_T4  # Default
            
    async def _validate_request(self, request: GPURequest) -> bool:
        """Validate GPU request against tenant limits"""
        usage = await self.get_tenant_usage(request.tenant_id)
        limits = self.tenant_limits[request.tenant_id]
        
        # Check GPU count
        if usage["total_gpus"] + request.gpu_count > limits["max_gpus"]:
            logger.warning(f"Request exceeds GPU limit for tenant {request.tenant_id}")
            return False
            
        # Check memory
        requested_memory_gb = request.gpu_count * (request.memory_per_gpu_mb / 1024)
        if usage["total_memory_gb"] + requested_memory_gb > limits["max_memory_gb"]:
            logger.warning(f"Request exceeds memory limit for tenant {request.tenant_id}")
            return False
            
        # Check concurrent jobs
        if request.purpose == "training":
            if usage["by_purpose"]["training"] >= limits["max_concurrent_training"]:
                logger.warning(f"Request exceeds concurrent training limit for tenant {request.tenant_id}")
                return False
        elif request.purpose == "inference":
            if usage["by_purpose"]["inference"] >= limits["max_concurrent_inference"]:
                logger.warning(f"Request exceeds concurrent inference limit for tenant {request.tenant_id}")
                return False
                
        return True
        
    async def _try_allocate(self, request: GPURequest) -> Optional[GPUAllocation]:
        """Try to allocate GPUs for request"""
        suitable_nodes = []
        
        for node in self.gpu_nodes.values():
            if not node.is_healthy:
                continue
                
            # Check GPU type if specified
            if request.gpu_type and node.gpu_type != request.gpu_type:
                continue
                
            # Check available GPU count
            if len(node.available_gpus) >= request.gpu_count:
                suitable_nodes.append(node)
                
        if not suitable_nodes:
            return None
            
        # Select best node (simple strategy - most available GPUs)
        best_node = max(suitable_nodes, key=lambda n: len(n.available_gpus))
        
        # Allocate GPUs
        allocated_gpus = list(best_node.available_gpus)[:request.gpu_count]
        
        allocation = GPUAllocation(
            allocation_id=f"gpu_alloc_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{request.request_id}",
            tenant_id=request.tenant_id,
            user_id=request.user_id,
            purpose=request.purpose,
            model_name=request.model_name,
            gpu_type=best_node.gpu_type,
            gpu_count=request.gpu_count,
            memory_per_gpu_mb=request.memory_per_gpu_mb,
            node_id=best_node.node_id,
            gpu_indices=allocated_gpus,
            priority=request.priority,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=request.duration_hours) if request.duration_hours else None,
            actual_usage={},
            is_active=True
        )
        
        return allocation
        
    async def _update_node_allocation(self, allocation: GPUAllocation):
        """Update node with allocation info"""
        node = self.gpu_nodes.get(allocation.node_id)
        if node:
            for gpu_idx in allocation.gpu_indices:
                node.available_gpus.remove(gpu_idx)
                node.allocated_gpus[gpu_idx] = allocation.allocation_id
                
    async def _monitoring_loop(self):
        """Monitor GPU usage and health"""
        while True:
            try:
                await asyncio.sleep(self.monitoring_interval)
                
                # Monitor each active allocation
                for allocation in list(self.allocations.values()):
                    if allocation.is_active:
                        await self._monitor_allocation(allocation)
                        
                # Check node health
                for node in self.gpu_nodes.values():
                    await self._check_node_health(node)
                    
                # Clean up expired allocations
                await self._cleanup_expired_allocations()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                
    async def _monitor_allocation(self, allocation: GPUAllocation):
        """Monitor a specific allocation"""
        if not self.nvml_initialized:
            return
            
        try:
            metrics = await self._get_gpu_metrics(allocation)
            allocation.actual_usage = metrics
            
            # Check for issues
            for gpu_metrics in metrics.get("gpus", []):
                if gpu_metrics.get("temperature", 0) > 85:
                    logger.warning(f"High GPU temperature in allocation {allocation.allocation_id}: {gpu_metrics['temperature']}C")
                    
                if gpu_metrics.get("memory_used_percent", 0) > 95:
                    logger.warning(f"High GPU memory usage in allocation {allocation.allocation_id}: {gpu_metrics['memory_used_percent']}%")
                    
        except Exception as e:
            logger.error(f"Error monitoring allocation {allocation.allocation_id}: {e}")
            
    async def _get_gpu_metrics(self, allocation: GPUAllocation) -> Dict[str, Any]:
        """Get current GPU metrics for allocation"""
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "gpus": []
        }
        
        for gpu_idx in allocation.gpu_indices:
            try:
                handle = nvml.nvmlDeviceGetHandleByIndex(gpu_idx)
                
                # Get utilization
                util = nvml.nvmlDeviceGetUtilizationRates(handle)
                
                # Get memory info
                mem_info = nvml.nvmlDeviceGetMemoryInfo(handle)
                
                # Get temperature
                temp = nvml.nvmlDeviceGetTemperature(handle, nvml.NVML_TEMPERATURE_GPU)
                
                # Get power usage
                power = nvml.nvmlDeviceGetPowerUsage(handle) / 1000  # Convert to watts
                
                gpu_metrics = {
                    "index": gpu_idx,
                    "utilization": util.gpu,
                    "memory_used_mb": mem_info.used // (1024 * 1024),
                    "memory_total_mb": mem_info.total // (1024 * 1024),
                    "memory_used_percent": (mem_info.used / mem_info.total) * 100,
                    "temperature": temp,
                    "power_watts": power
                }
                
                metrics["gpus"].append(gpu_metrics)
                
            except Exception as e:
                logger.error(f"Error getting metrics for GPU {gpu_idx}: {e}")
                
        return metrics
        
    async def _check_node_health(self, node: GPUNode):
        """Check health of GPU node"""
        # Check heartbeat
        if datetime.utcnow() - node.last_heartbeat > timedelta(minutes=5):
            node.is_healthy = False
            logger.warning(f"Node {node.node_id} marked unhealthy - no heartbeat")
            
    async def _cleanup_expired_allocations(self):
        """Clean up expired allocations"""
        for allocation_id, allocation in list(self.allocations.items()):
            if allocation.expires_at and datetime.utcnow() > allocation.expires_at:
                await self.release_gpus(allocation_id)
                
    async def _scheduler_loop(self):
        """Schedule pending GPU requests"""
        while True:
            try:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                if not self.pending_requests:
                    continue
                    
                # Process pending requests by priority
                for request in list(self.pending_requests):
                    allocation = await self._try_allocate(request)
                    
                    if allocation:
                        self.allocations[allocation.allocation_id] = allocation
                        await self._update_node_allocation(allocation)
                        self.pending_requests.remove(request)
                        
                        logger.info(f"Scheduled pending request: {request.request_id} -> {allocation.allocation_id}")
                        
                        # Notify requester
                        await self._notify_allocation_ready(allocation)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                
    def _record_usage(self, allocation: GPUAllocation):
        """Record GPU usage for billing/analytics"""
        duration_hours = (datetime.utcnow() - allocation.created_at).total_seconds() / 3600
        
        usage_record = {
            "allocation_id": allocation.allocation_id,
            "tenant_id": allocation.tenant_id,
            "user_id": allocation.user_id,
            "model_name": allocation.model_name,
            "purpose": allocation.purpose,
            "gpu_type": allocation.gpu_type.value,
            "gpu_count": allocation.gpu_count,
            "duration_hours": duration_hours,
            "gpu_hours": duration_hours * allocation.gpu_count,
            "peak_utilization": max(
                (gpu.get("utilization", 0) for gpu in allocation.actual_usage.get("gpus", [])),
                default=0
            ),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.usage_history[allocation.tenant_id].append(usage_record)
        
    async def _notify_allocation_ready(self, allocation: GPUAllocation):
        """Notify that GPU allocation is ready"""
        # In production, send notification to user
        logger.info(f"GPU allocation ready: {allocation.allocation_id} for user {allocation.user_id}") 