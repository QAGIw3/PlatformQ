"""
Apache Ignite Compute Manager for Distributed ZKP Generation
"""

import asyncio
import json
import uuid
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import threading

from pyignite import AsyncClient
from pyignite.datatypes import String, LongObject

from app.config import settings


class ComputeTask:
    """Represents a compute task for proof generation"""
    
    def __init__(
        self,
        task_id: str,
        task_type: str,
        params: Dict[str, Any],
        priority: int = 5
    ):
        self.task_id = task_id
        self.task_type = task_type
        self.params = params
        self.priority = priority
        self.status = "pending"
        self.created_at = datetime.now(timezone.utc)
        self.started_at = None
        self.completed_at = None
        self.result = None
        self.error = None
        self.worker_id = None


class ComputeManager:
    """
    Manages distributed compute tasks on Apache Ignite grid
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 10800,
        worker_threads: int = 4,
        timeout: int = 300
    ):
        self.host = host
        self.port = port
        self.worker_threads = worker_threads
        self.timeout = timeout
        self.client: Optional[AsyncClient] = None
        self.connected = False
        
        # Task management
        self.active_tasks: Dict[str, ComputeTask] = {}
        self.task_lock = threading.Lock()
        
        # Worker pool for local computation
        self.executor = ThreadPoolExecutor(max_workers=worker_threads)
        
        # Cache names
        self.TASK_QUEUE_CACHE = "zkp_task_queue"
        self.TASK_RESULTS_CACHE = "zkp_task_results"
        self.WORKER_STATUS_CACHE = "zkp_worker_status"
        
        # Registered compute functions
        self.compute_functions: Dict[str, Callable] = {}
        
        # Monitoring
        self.total_tasks = 0
        self.completed_tasks = 0
        self.failed_tasks = 0
    
    async def connect(self):
        """Connect to Apache Ignite"""
        if self.connected:
            return
            
        try:
            self.client = AsyncClient()
            await self.client.connect(self.host, self.port)
            
            # Create caches
            await self.client.get_or_create_cache(self.TASK_QUEUE_CACHE)
            await self.client.get_or_create_cache(self.TASK_RESULTS_CACHE)
            await self.client.get_or_create_cache(self.WORKER_STATUS_CACHE)
            
            self.connected = True
            
            # Register this node as a worker
            await self._register_worker()
            
        except Exception as e:
            print(f"Failed to connect to Apache Ignite: {str(e)}")
            self.connected = False
            raise
    
    async def disconnect(self):
        """Disconnect from Apache Ignite"""
        if self.client and self.connected:
            # Unregister worker
            await self._unregister_worker()
            
            await self.client.close()
            self.connected = False
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
    
    async def health_check(self) -> bool:
        """Check if compute grid is healthy"""
        if not self.connected:
            return False
            
        try:
            # Check cache access
            cache = await self.client.get_cache(self.TASK_QUEUE_CACHE)
            await cache.get("health_check")
            return True
        except Exception:
            return False
    
    async def register_tasks(self):
        """Register compute tasks that can be executed"""
        # Import task implementations
        from app.core.proof_tasks import (
            generate_bbs_signature_task,
            verify_bbs_proof_task,
            generate_selective_disclosure_task,
            generate_range_proof_task,
            generate_predicate_proof_task,
            generate_set_membership_proof_task
        )
        
        # Register functions
        self.compute_functions["bbs_signature"] = generate_bbs_signature_task
        self.compute_functions["bbs_verify"] = verify_bbs_proof_task
        self.compute_functions["selective_disclosure"] = generate_selective_disclosure_task
        self.compute_functions["range_proof"] = generate_range_proof_task
        self.compute_functions["predicate_proof"] = generate_predicate_proof_task
        self.compute_functions["set_membership"] = generate_set_membership_proof_task
        
        print(f"Registered {len(self.compute_functions)} compute tasks")
    
    async def submit_task(
        self,
        task_type: str,
        params: Dict[str, Any],
        priority: int = 5
    ) -> str:
        """Submit a task for distributed execution"""
        if not self.connected:
            raise RuntimeError("Not connected to compute grid")
        
        # Create task
        task_id = str(uuid.uuid4())
        task = ComputeTask(
            task_id=task_id,
            task_type=task_type,
            params=params,
            priority=priority
        )
        
        # Store in active tasks
        with self.task_lock:
            self.active_tasks[task_id] = task
            self.total_tasks += 1
        
        # Submit to grid
        await self._submit_to_grid(task)
        
        return task_id
    
    async def submit_batch(
        self,
        tasks: List[Dict[str, Any]],
        priority: int = 5
    ) -> List[str]:
        """Submit multiple tasks for batch processing"""
        task_ids = []
        
        for task_spec in tasks:
            task_id = await self.submit_task(
                task_type=task_spec["type"],
                params=task_spec["params"],
                priority=priority
            )
            task_ids.append(task_id)
        
        return task_ids
    
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a task"""
        # Check active tasks
        with self.task_lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                return {
                    "task_id": task.task_id,
                    "status": task.status,
                    "created_at": task.created_at,
                    "started_at": task.started_at,
                    "completed_at": task.completed_at,
                    "worker_id": task.worker_id
                }
        
        # Check results cache
        if self.connected:
            results_cache = await self.client.get_cache(self.TASK_RESULTS_CACHE)
            result = await results_cache.get(task_id)
            if result:
                return json.loads(result)
        
        return None
    
    async def get_task_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get result of a completed task"""
        # Check active tasks
        with self.task_lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                if task.status == "completed":
                    return {"result": task.result}
                elif task.status == "failed":
                    return {"error": task.error}
                else:
                    return {"status": task.status}
        
        # Check results cache
        if self.connected:
            results_cache = await self.client.get_cache(self.TASK_RESULTS_CACHE)
            result = await results_cache.get(task_id)
            if result:
                return json.loads(result)
        
        return None
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task"""
        with self.task_lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                if task.status == "pending":
                    task.status = "cancelled"
                    # Remove from queue
                    if self.connected:
                        queue_cache = await self.client.get_cache(self.TASK_QUEUE_CACHE)
                        await queue_cache.remove(task_id)
                    return True
        
        return False
    
    async def _submit_to_grid(self, task: ComputeTask):
        """Submit task to the compute grid"""
        # Serialize task
        task_data = {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "params": task.params,
            "priority": task.priority,
            "created_at": task.created_at.isoformat()
        }
        
        # Add to queue
        queue_cache = await self.client.get_cache(self.TASK_QUEUE_CACHE)
        await queue_cache.put(task.task_id, json.dumps(task_data))
        
        # Start processing in background
        asyncio.create_task(self._process_task(task))
    
    async def _process_task(self, task: ComputeTask):
        """Process a task (can be distributed across grid)"""
        try:
            # Update status
            task.status = "running"
            task.started_at = datetime.now(timezone.utc)
            task.worker_id = self._get_worker_id()
            
            # Get compute function
            if task.task_type not in self.compute_functions:
                raise ValueError(f"Unknown task type: {task.task_type}")
            
            compute_func = self.compute_functions[task.task_type]
            
            # Execute in thread pool (for CPU-intensive operations)
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                self.executor,
                compute_func,
                task.params
            )
            
            # Update task
            task.status = "completed"
            task.completed_at = datetime.now(timezone.utc)
            task.result = result
            
            # Store result
            await self._store_result(task)
            
            # Update metrics
            with self.task_lock:
                self.completed_tasks += 1
                
        except Exception as e:
            # Handle failure
            task.status = "failed"
            task.completed_at = datetime.now(timezone.utc)
            task.error = str(e)
            
            # Store error
            await self._store_result(task)
            
            # Update metrics
            with self.task_lock:
                self.failed_tasks += 1
        
        finally:
            # Clean up from active tasks after delay
            await asyncio.sleep(60)  # Keep for 1 minute for status queries
            with self.task_lock:
                if task.task_id in self.active_tasks:
                    del self.active_tasks[task.task_id]
    
    async def _store_result(self, task: ComputeTask):
        """Store task result in cache"""
        if not self.connected:
            return
        
        result_data = {
            "task_id": task.task_id,
            "status": task.status,
            "created_at": task.created_at.isoformat(),
            "started_at": task.started_at.isoformat() if task.started_at else None,
            "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            "worker_id": task.worker_id
        }
        
        if task.status == "completed":
            result_data["result"] = task.result
        elif task.status == "failed":
            result_data["error"] = task.error
        
        # Store in results cache
        results_cache = await self.client.get_cache(self.TASK_RESULTS_CACHE)
        await results_cache.put(
            task.task_id,
            json.dumps(result_data),
            ttl=3600 * 1000  # TTL 1 hour
        )
        
        # Remove from queue
        queue_cache = await self.client.get_cache(self.TASK_QUEUE_CACHE)
        await queue_cache.remove(task.task_id)
    
    async def _register_worker(self):
        """Register this node as a worker"""
        if not self.connected:
            return
        
        worker_id = self._get_worker_id()
        worker_data = {
            "worker_id": worker_id,
            "threads": self.worker_threads,
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "status": "active"
        }
        
        worker_cache = await self.client.get_cache(self.WORKER_STATUS_CACHE)
        await worker_cache.put(worker_id, json.dumps(worker_data))
    
    async def _unregister_worker(self):
        """Unregister this node as a worker"""
        if not self.connected:
            return
        
        worker_id = self._get_worker_id()
        worker_cache = await self.client.get_cache(self.WORKER_STATUS_CACHE)
        await worker_cache.remove(worker_id)
    
    def _get_worker_id(self) -> str:
        """Get unique worker ID"""
        import socket
        import os
        hostname = socket.gethostname()
        pid = os.getpid()
        return f"{hostname}-{pid}"
    
    async def monitor_jobs(self):
        """Background task to monitor job queue"""
        while self.connected:
            try:
                # Update worker heartbeat
                await self._update_worker_heartbeat()
                
                # Check for stale tasks
                await self._check_stale_tasks()
                
                # Sleep before next check
                await asyncio.sleep(30)
                
            except Exception as e:
                print(f"Error in job monitor: {str(e)}")
                await asyncio.sleep(60)
    
    async def _update_worker_heartbeat(self):
        """Update worker heartbeat"""
        if not self.connected:
            return
        
        worker_id = self._get_worker_id()
        worker_cache = await self.client.get_cache(self.WORKER_STATUS_CACHE)
        
        worker_data = await worker_cache.get(worker_id)
        if worker_data:
            data = json.loads(worker_data)
            data["last_heartbeat"] = datetime.now(timezone.utc).isoformat()
            data["active_tasks"] = len([t for t in self.active_tasks.values() if t.status == "running"])
            await worker_cache.put(worker_id, json.dumps(data))
    
    async def _check_stale_tasks(self):
        """Check for tasks that have been running too long"""
        current_time = datetime.now(timezone.utc)
        
        with self.task_lock:
            for task in list(self.active_tasks.values()):
                if task.status == "running" and task.started_at:
                    elapsed = (current_time - task.started_at).total_seconds()
                    if elapsed > self.timeout:
                        # Mark as failed
                        task.status = "failed"
                        task.error = "Task timeout"
                        task.completed_at = current_time
                        asyncio.create_task(self._store_result(task))
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get compute grid statistics"""
        stats = {
            "connected": self.connected,
            "worker_threads": self.worker_threads,
            "active_tasks": len(self.active_tasks),
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "success_rate": self.completed_tasks / self.total_tasks if self.total_tasks > 0 else 0
        }
        
        # Get worker information
        if self.connected:
            worker_cache = await self.client.get_cache(self.WORKER_STATUS_CACHE)
            # This would need proper cache iteration API
            stats["active_workers"] = 1  # Placeholder
        
        # Task breakdown
        with self.task_lock:
            task_types = {}
            for task in self.active_tasks.values():
                task_types[task.task_type] = task_types.get(task.task_type, 0) + 1
            stats["active_task_types"] = task_types
        
        return stats
    
    async def shutdown(self):
        """Graceful shutdown"""
        # Cancel all pending tasks
        with self.task_lock:
            for task in self.active_tasks.values():
                if task.status == "pending":
                    task.status = "cancelled"
        
        # Wait for running tasks to complete (with timeout)
        timeout = 30
        start_time = datetime.now(timezone.utc)
        
        while True:
            with self.task_lock:
                running_tasks = [t for t in self.active_tasks.values() if t.status == "running"]
                if not running_tasks:
                    break
            
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            if elapsed > timeout:
                print(f"Shutdown timeout - {len(running_tasks)} tasks still running")
                break
            
            await asyncio.sleep(1)
    
    def get_active_jobs_count(self) -> int:
        """Get count of active jobs"""
        with self.task_lock:
            return len([t for t in self.active_tasks.values() if t.status in ["pending", "running"]]) 