"""
Distributed orchestration for cross-cluster workflows.

Provides coordination, state management, and fault tolerance for distributed processing.
"""

import uuid
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable, Set, Tuple
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict
import json
import hashlib

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class NodeState(str, Enum):
    """Distributed node states"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    FAILED = "failed"
    DRAINING = "draining"
    MAINTENANCE = "maintenance"


class TaskState(str, Enum):
    """Distributed task states"""
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class PartitionStrategy(str, Enum):
    """Data partitioning strategies"""
    HASH = "hash"
    RANGE = "range"
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    CUSTOM = "custom"


class ConsistencyLevel(str, Enum):
    """Consistency levels for distributed operations"""
    EVENTUAL = "eventual"
    STRONG = "strong"
    BOUNDED_STALENESS = "bounded_staleness"
    SESSION = "session"
    CONSISTENT_PREFIX = "consistent_prefix"


@dataclass
class ClusterNode:
    """Distributed cluster node"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    host: str = ""
    port: int = 0
    
    # State
    state: NodeState = NodeState.ACTIVE
    
    # Capacity
    cpu_cores: int = 0
    memory_gb: int = 0
    disk_gb: int = 0
    
    # Current usage
    cpu_usage_percent: float = 0.0
    memory_usage_gb: float = 0.0
    disk_usage_gb: float = 0.0
    
    # Health
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    health_score: float = 1.0
    
    # Metadata
    region: Optional[str] = None
    zone: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    
    def is_healthy(self) -> bool:
        """Check if node is healthy"""
        if self.state != NodeState.ACTIVE:
            return False
            
        # Check heartbeat
        heartbeat_age = (datetime.utcnow() - self.last_heartbeat).total_seconds()
        if heartbeat_age > 60:  # 1 minute timeout
            return False
            
        return self.health_score > 0.5
        
    def get_available_capacity(self) -> Dict[str, float]:
        """Get available capacity"""
        return {
            "cpu_cores": self.cpu_cores * (1 - self.cpu_usage_percent / 100),
            "memory_gb": self.memory_gb - self.memory_usage_gb,
            "disk_gb": self.disk_gb - self.disk_usage_gb
        }


@dataclass
class DistributedTask:
    """Distributed processing task"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    task_type: str = ""
    
    # State
    state: TaskState = TaskState.PENDING
    
    # Assignment
    assigned_node_id: Optional[str] = None
    assigned_at: Optional[datetime] = None
    
    # Execution
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Data
    input_data: Dict[str, Any] = field(default_factory=dict)
    output_data: Optional[Dict[str, Any]] = None
    
    # Requirements
    required_cpu: float = 1.0
    required_memory_gb: float = 1.0
    required_disk_gb: float = 0.0
    
    # Configuration
    timeout_seconds: int = 3600
    max_retries: int = 3
    retry_count: int = 0
    
    # Metadata
    priority: int = 0
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def is_retriable(self) -> bool:
        """Check if task can be retried"""
        return self.retry_count < self.max_retries
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "task_type": self.task_type,
            "state": self.state.value,
            "assigned_node_id": self.assigned_node_id,
            "assigned_at": self.assigned_at.isoformat() if self.assigned_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "required_cpu": self.required_cpu,
            "required_memory_gb": self.required_memory_gb,
            "priority": self.priority,
            "retry_count": self.retry_count
        }


@dataclass
class TaskGroup:
    """Group of related tasks"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    
    # Tasks
    task_ids: List[str] = field(default_factory=list)
    
    # Dependencies
    dependencies: Dict[str, List[str]] = field(default_factory=dict)  # task_id -> [dependency_ids]
    
    # Configuration
    partition_strategy: PartitionStrategy = PartitionStrategy.HASH
    partition_key: Optional[str] = None
    
    # State
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def add_task(self, task_id: str, dependencies: Optional[List[str]] = None):
        """Add task to group"""
        if task_id not in self.task_ids:
            self.task_ids.append(task_id)
            
        if dependencies:
            self.dependencies[task_id] = dependencies


@dataclass
class DistributedLock:
    """Distributed lock for coordination"""
    resource_id: str
    owner_id: str
    acquired_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(minutes=5))
    
    def is_expired(self) -> bool:
        """Check if lock is expired"""
        return datetime.utcnow() > self.expires_at


class BaseTaskExecutor(ABC):
    """Base class for task executors"""
    
    @abstractmethod
    async def execute(
        self,
        task: DistributedTask,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute task"""
        pass
        
    @abstractmethod
    def can_handle(self, task_type: str) -> bool:
        """Check if executor can handle task type"""
        pass


class SparkTaskExecutor(BaseTaskExecutor):
    """Executor for Spark tasks"""
    
    async def execute(
        self,
        task: DistributedTask,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute Spark task"""
        # Submit to Spark cluster
        logger.info(f"Executing Spark task: {task.name}")
        
        # Simulate execution
        await asyncio.sleep(1)
        
        return {
            "status": "completed",
            "records_processed": 10000
        }
        
    def can_handle(self, task_type: str) -> bool:
        return task_type == "spark"


class FlinkTaskExecutor(BaseTaskExecutor):
    """Executor for Flink tasks"""
    
    async def execute(
        self,
        task: DistributedTask,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute Flink task"""
        # Submit to Flink cluster
        logger.info(f"Executing Flink task: {task.name}")
        
        # Simulate execution
        await asyncio.sleep(1)
        
        return {
            "status": "completed",
            "events_processed": 50000
        }
        
    def can_handle(self, task_type: str) -> bool:
        return task_type == "flink"


class DistributedOrchestrator:
    """
    Distributed workflow orchestrator.
    
    Features:
    - Multi-cluster coordination
    - Task scheduling and assignment
    - Resource management
    - Fault tolerance
    - State synchronization
    - Distributed locking
    """
    
    def __init__(
        self,
        node_id: str,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.node_id = node_id
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Cluster state
        self._nodes: Dict[str, ClusterNode] = {}
        self._local_node: Optional[ClusterNode] = None
        
        # Task management
        self._tasks: Dict[str, DistributedTask] = {}
        self._task_groups: Dict[str, TaskGroup] = {}
        self._task_queue: asyncio.Queue = asyncio.Queue()
        
        # Executors
        self._executors: List[BaseTaskExecutor] = [
            SparkTaskExecutor(),
            FlinkTaskExecutor()
        ]
        
        # Distributed locks
        self._locks: Dict[str, DistributedLock] = {}
        
        # Background tasks
        self._scheduler_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        
        # State
        self._is_leader = False
        self._leader_node_id: Optional[str] = None
        
    async def start(self):
        """Start orchestrator"""
        # Register local node
        self._local_node = ClusterNode(
            id=self.node_id,
            name=f"node-{self.node_id}",
            host="localhost",  # Would get from config
            port=8080
        )
        self._nodes[self.node_id] = self._local_node
        
        # Start background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        
        # Participate in leader election
        await self._leader_election()
        
        logger.info(f"Distributed orchestrator started on node {self.node_id}")
        
    async def stop(self):
        """Stop orchestrator"""
        # Cancel background tasks
        for task in [self._heartbeat_task, self._scheduler_task, self._monitor_task]:
            if task and not task.done():
                task.cancel()
                
        logger.info(f"Distributed orchestrator stopped on node {self.node_id}")
        
    def register_node(self, node: ClusterNode):
        """Register cluster node"""
        self._nodes[node.id] = node
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="cluster.node.registered",
                source="distributed_orchestrator",
                data={
                    "node_id": node.id,
                    "node_name": node.name,
                    "state": node.state.value
                }
            ))
            
        logger.info(f"Registered node: {node.name}")
        
    async def submit_task(
        self,
        task: DistributedTask,
        group_id: Optional[str] = None
    ) -> str:
        """Submit task for execution"""
        # Store task
        self._tasks[task.id] = task
        
        # Add to group if specified
        if group_id:
            group = self._task_groups.get(group_id)
            if group:
                group.add_task(task.id)
                
        # Queue for scheduling
        await self._task_queue.put(task.id)
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="task.submitted",
                source="distributed_orchestrator",
                data={
                    "task_id": task.id,
                    "task_type": task.task_type,
                    "priority": task.priority
                }
            ))
            
        logger.info(f"Submitted task: {task.name}")
        return task.id
        
    def create_task_group(
        self,
        name: str,
        partition_strategy: PartitionStrategy = PartitionStrategy.HASH,
        partition_key: Optional[str] = None
    ) -> TaskGroup:
        """Create task group"""
        group = TaskGroup(
            name=name,
            partition_strategy=partition_strategy,
            partition_key=partition_key
        )
        
        self._task_groups[group.id] = group
        
        logger.info(f"Created task group: {name}")
        return group
        
    async def _scheduler_loop(self):
        """Task scheduling loop"""
        logger.info("Task scheduler started")
        
        while True:
            try:
                # Get pending task
                task_id = await self._task_queue.get()
                task = self._tasks.get(task_id)
                
                if not task:
                    continue
                    
                # Only leader schedules tasks
                if not self._is_leader:
                    # Re-queue for leader
                    await self._task_queue.put(task_id)
                    await asyncio.sleep(1)
                    continue
                    
                # Find suitable node
                node = self._find_suitable_node(task)
                
                if node:
                    # Assign task
                    await self._assign_task(task, node)
                else:
                    # Re-queue task
                    await self._task_queue.put(task_id)
                    logger.warning(f"No suitable node for task {task.name}")
                    await asyncio.sleep(5)
                    
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(1)
                
    def _find_suitable_node(self, task: DistributedTask) -> Optional[ClusterNode]:
        """Find suitable node for task"""
        suitable_nodes = []
        
        for node in self._nodes.values():
            if not node.is_healthy():
                continue
                
            # Check capacity
            capacity = node.get_available_capacity()
            if (capacity["cpu_cores"] >= task.required_cpu and
                capacity["memory_gb"] >= task.required_memory_gb and
                capacity["disk_gb"] >= task.required_disk_gb):
                suitable_nodes.append(node)
                
        if not suitable_nodes:
            return None
            
        # Select node with most available resources
        return max(suitable_nodes, key=lambda n: n.get_available_capacity()["cpu_cores"])
        
    async def _assign_task(self, task: DistributedTask, node: ClusterNode):
        """Assign task to node"""
        task.state = TaskState.ASSIGNED
        task.assigned_node_id = node.id
        task.assigned_at = datetime.utcnow()
        
        # Update node usage (estimated)
        node.cpu_usage_percent += (task.required_cpu / node.cpu_cores) * 100
        node.memory_usage_gb += task.required_memory_gb
        
        # If assigned to local node, execute
        if node.id == self.node_id:
            asyncio.create_task(self._execute_task(task))
        else:
            # Send to remote node
            await self._send_task_to_node(task, node)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="task.assigned",
                source="distributed_orchestrator",
                data={
                    "task_id": task.id,
                    "node_id": node.id
                }
            ))
            
        logger.info(f"Assigned task {task.name} to node {node.name}")
        
    async def _execute_task(self, task: DistributedTask):
        """Execute task locally"""
        task.state = TaskState.RUNNING
        task.started_at = datetime.utcnow()
        
        try:
            # Get executor
            executor = None
            for exec in self._executors:
                if exec.can_handle(task.task_type):
                    executor = exec
                    break
                    
            if not executor:
                raise ValueError(f"No executor for task type: {task.task_type}")
                
            # Execute with timeout
            context = {
                "node_id": self.node_id,
                "task_group_id": self._get_task_group_id(task.id)
            }
            
            result = await asyncio.wait_for(
                executor.execute(task, context),
                timeout=task.timeout_seconds
            )
            
            # Update task
            task.state = TaskState.COMPLETED
            task.output_data = result
            task.completed_at = datetime.utcnow()
            
            # Update node usage
            if self._local_node:
                self._local_node.cpu_usage_percent -= (task.required_cpu / self._local_node.cpu_cores) * 100
                self._local_node.memory_usage_gb -= task.required_memory_gb
                
            # Publish event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="task.completed",
                    source="distributed_orchestrator",
                    data={
                        "task_id": task.id,
                        "duration_seconds": (task.completed_at - task.started_at).total_seconds()
                    }
                ))
                
            logger.info(f"Task completed: {task.name}")
            
        except asyncio.TimeoutError:
            await self._handle_task_failure(task, "Task timeout")
        except Exception as e:
            await self._handle_task_failure(task, str(e))
            
    async def _handle_task_failure(self, task: DistributedTask, error: str):
        """Handle task failure"""
        task.state = TaskState.FAILED
        task.completed_at = datetime.utcnow()
        
        logger.error(f"Task failed: {task.name}, error: {error}")
        
        # Check if retriable
        if task.is_retriable():
            task.retry_count += 1
            task.state = TaskState.RETRYING
            
            # Re-queue with delay
            await asyncio.sleep(5 * task.retry_count)  # Exponential backoff
            await self._task_queue.put(task.id)
            
            logger.info(f"Retrying task {task.name}, attempt {task.retry_count}")
        else:
            # Publish failure event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="task.failed",
                    source="distributed_orchestrator",
                    data={
                        "task_id": task.id,
                        "error": error,
                        "retry_count": task.retry_count
                    }
                ))
                
    async def _send_task_to_node(self, task: DistributedTask, node: ClusterNode):
        """Send task to remote node"""
        # This would use actual RPC/messaging
        logger.info(f"Sending task {task.name} to remote node {node.name}")
        
    def _get_task_group_id(self, task_id: str) -> Optional[str]:
        """Get task group ID for task"""
        for group_id, group in self._task_groups.items():
            if task_id in group.task_ids:
                return group_id
        return None
        
    async def acquire_lock(
        self,
        resource_id: str,
        owner_id: Optional[str] = None,
        ttl_seconds: int = 300
    ) -> bool:
        """Acquire distributed lock"""
        owner_id = owner_id or self.node_id
        
        # Check if lock exists and is valid
        existing_lock = self._locks.get(resource_id)
        if existing_lock and not existing_lock.is_expired():
            return existing_lock.owner_id == owner_id
            
        # Create new lock
        lock = DistributedLock(
            resource_id=resource_id,
            owner_id=owner_id,
            expires_at=datetime.utcnow() + timedelta(seconds=ttl_seconds)
        )
        
        # Store in distributed cache if available
        if self.cache:
            cache_key = f"distributed_lock:{resource_id}"
            # Use cache's atomic operations
            success = self.cache.set_if_not_exists(
                cache_key,
                lock.__dict__,
                ttl=ttl_seconds
            )
            
            if success:
                self._locks[resource_id] = lock
                return True
            return False
        else:
            # Simple local implementation
            self._locks[resource_id] = lock
            return True
            
    def release_lock(self, resource_id: str, owner_id: Optional[str] = None):
        """Release distributed lock"""
        owner_id = owner_id or self.node_id
        
        lock = self._locks.get(resource_id)
        if lock and lock.owner_id == owner_id:
            del self._locks[resource_id]
            
            # Remove from cache
            if self.cache:
                cache_key = f"distributed_lock:{resource_id}"
                self.cache.delete(cache_key)
                
            logger.info(f"Released lock on {resource_id}")
            
    async def _heartbeat_loop(self):
        """Node heartbeat loop"""
        logger.info("Heartbeat loop started")
        
        while True:
            try:
                if self._local_node:
                    # Update heartbeat
                    self._local_node.last_heartbeat = datetime.utcnow()
                    
                    # Publish to other nodes
                    if self.event_bus:
                        self.event_bus.publish(Event(
                            type="cluster.heartbeat",
                            source="distributed_orchestrator",
                            data={
                                "node_id": self.node_id,
                                "state": self._local_node.state.value,
                                "health_score": self._local_node.health_score
                            }
                        ))
                        
                await asyncio.sleep(10)  # Heartbeat every 10 seconds
                
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                await asyncio.sleep(10)
                
    async def _monitor_loop(self):
        """Cluster monitoring loop"""
        logger.info("Monitor loop started")
        
        while True:
            try:
                # Check node health
                for node in list(self._nodes.values()):
                    if node.id == self.node_id:
                        continue
                        
                    if not node.is_healthy():
                        logger.warning(f"Node {node.name} is unhealthy")
                        
                        # Handle node failure
                        await self._handle_node_failure(node)
                        
                # Check stuck tasks
                for task in self._tasks.values():
                    if task.state == TaskState.RUNNING and task.started_at:
                        runtime = (datetime.utcnow() - task.started_at).total_seconds()
                        if runtime > task.timeout_seconds:
                            logger.warning(f"Task {task.name} exceeded timeout")
                            await self._handle_task_failure(task, "Timeout")
                            
                await asyncio.sleep(30)  # Monitor every 30 seconds
                
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(30)
                
    async def _handle_node_failure(self, node: ClusterNode):
        """Handle node failure"""
        node.state = NodeState.FAILED
        
        # Reassign tasks from failed node
        tasks_to_reassign = [
            task for task in self._tasks.values()
            if task.assigned_node_id == node.id and
            task.state in [TaskState.ASSIGNED, TaskState.RUNNING]
        ]
        
        for task in tasks_to_reassign:
            logger.info(f"Reassigning task {task.name} from failed node {node.name}")
            task.state = TaskState.PENDING
            task.assigned_node_id = None
            await self._task_queue.put(task.id)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="cluster.node.failed",
                source="distributed_orchestrator",
                data={
                    "node_id": node.id,
                    "reassigned_tasks": len(tasks_to_reassign)
                }
            ))
            
    async def _leader_election(self):
        """Participate in leader election"""
        # Simple implementation: lowest node ID wins
        active_nodes = [n for n in self._nodes.values() if n.is_healthy()]
        
        if not active_nodes:
            return
            
        leader_node = min(active_nodes, key=lambda n: n.id)
        
        self._leader_node_id = leader_node.id
        self._is_leader = (leader_node.id == self.node_id)
        
        if self._is_leader:
            logger.info(f"Node {self.node_id} elected as leader")
            
            # Publish event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="cluster.leader.elected",
                    source="distributed_orchestrator",
                    data={"leader_node_id": self.node_id}
                ))
                
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get cluster status"""
        return {
            "nodes": {
                node_id: {
                    "name": node.name,
                    "state": node.state.value,
                    "health_score": node.health_score,
                    "cpu_usage": node.cpu_usage_percent,
                    "memory_usage_gb": node.memory_usage_gb,
                    "is_healthy": node.is_healthy()
                }
                for node_id, node in self._nodes.items()
            },
            "tasks": {
                "total": len(self._tasks),
                "by_state": {
                    state.value: len([t for t in self._tasks.values() if t.state == state])
                    for state in TaskState
                },
                "queued": self._task_queue.qsize()
            },
            "leader": {
                "node_id": self._leader_node_id,
                "is_current_node": self._is_leader
            }
        }
        
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task status"""
        task = self._tasks.get(task_id)
        if task:
            return task.to_dict()
        return None 