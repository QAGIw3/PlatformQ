"""
Distributed ZKP Compute Service using Apache Ignite
Enables parallel zero-knowledge proof generation across compute grid
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
import json
import hashlib
from datetime import datetime

from pyignite import Client as IgniteClient
from pyignite.cache import Cache

logger = logging.getLogger(__name__)


@dataclass
class ProofTask:
    """Represents a ZKP generation task"""
    task_id: str
    proof_type: str
    input_data: Dict[str, Any]
    circuit_name: str
    priority: int = 0
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


@dataclass
class ProofResult:
    """Result of ZKP generation"""
    task_id: str
    proof_data: Dict[str, Any]
    success: bool
    error: Optional[str] = None
    compute_time_ms: float = 0.0
    node_id: Optional[str] = None


class IgniteZKPCompute:
    """
    Distributed ZKP computation using Apache Ignite
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.client = ignite_client
        self._task_cache: Optional[Cache] = None
        self._result_cache: Optional[Cache] = None
        self._circuit_cache: Optional[Cache] = None
        self._worker_pool = None
        
    async def initialize(self):
        """Initialize compute service"""
        # Create caches
        self._task_cache = await self._get_or_create_cache("zkp_tasks")
        self._result_cache = await self._get_or_create_cache("zkp_results")
        self._circuit_cache = await self._get_or_create_cache("zkp_circuits")
        
        # Initialize worker pool
        self._worker_pool = ZKPWorkerPool(self.client)
        await self._worker_pool.initialize()
        
        logger.info("Ignite ZKP compute service initialized")
        
    async def _get_or_create_cache(self, cache_name: str) -> Cache:
        """Get or create cache"""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.client.get_or_create_cache, cache_name
        )
        
    async def submit_proof_task(
        self,
        proof_type: str,
        input_data: Dict[str, Any],
        circuit_name: str,
        priority: int = 0
    ) -> str:
        """Submit a proof generation task"""
        # Generate task ID
        task_id = self._generate_task_id(proof_type, input_data)
        
        # Create task
        task = ProofTask(
            task_id=task_id,
            proof_type=proof_type,
            input_data=input_data,
            circuit_name=circuit_name,
            priority=priority
        )
        
        # Store task
        await self._store_task(task)
        
        # Submit to worker pool
        await self._worker_pool.submit_task(task)
        
        logger.info(f"Submitted ZKP task {task_id} for {proof_type}")
        return task_id
        
    async def submit_batch_tasks(
        self,
        tasks: List[Dict[str, Any]]
    ) -> List[str]:
        """Submit multiple proof tasks for parallel processing"""
        task_ids = []
        
        # Create tasks
        proof_tasks = []
        for task_data in tasks:
            task_id = self._generate_task_id(
                task_data["proof_type"],
                task_data["input_data"]
            )
            
            task = ProofTask(
                task_id=task_id,
                proof_type=task_data["proof_type"],
                input_data=task_data["input_data"],
                circuit_name=task_data["circuit_name"],
                priority=task_data.get("priority", 0)
            )
            
            proof_tasks.append(task)
            task_ids.append(task_id)
            
        # Store all tasks
        await self._store_tasks_batch(proof_tasks)
        
        # Submit to worker pool for parallel processing
        await self._worker_pool.submit_batch(proof_tasks)
        
        logger.info(f"Submitted {len(task_ids)} ZKP tasks for parallel processing")
        return task_ids
        
    async def get_proof_result(
        self,
        task_id: str,
        timeout_seconds: int = 300
    ) -> Optional[ProofResult]:
        """Get proof generation result"""
        start_time = asyncio.get_event_loop().time()
        
        while True:
            # Check result cache
            result = await self._get_result(task_id)
            if result:
                return result
                
            # Check timeout
            if asyncio.get_event_loop().time() - start_time > timeout_seconds:
                logger.warning(f"Timeout waiting for proof result {task_id}")
                return None
                
            # Wait before retry
            await asyncio.sleep(0.5)
            
    async def get_batch_results(
        self,
        task_ids: List[str],
        timeout_seconds: int = 300
    ) -> Dict[str, ProofResult]:
        """Get results for multiple proof tasks"""
        results = {}
        
        # Create tasks for parallel result fetching
        fetch_tasks = []
        for task_id in task_ids:
            task = self.get_proof_result(task_id, timeout_seconds)
            fetch_tasks.append(task)
            
        # Wait for all results
        task_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        
        # Map results
        for task_id, result in zip(task_ids, task_results):
            if isinstance(result, ProofResult):
                results[task_id] = result
            else:
                logger.error(f"Failed to get result for task {task_id}: {result}")
                
        return results
        
    async def execute_map_reduce_proof(
        self,
        proof_type: str,
        data_chunks: List[Dict[str, Any]],
        circuit_name: str,
        reduce_func: Callable
    ) -> Dict[str, Any]:
        """Execute map-reduce style proof generation"""
        # Map phase - generate proofs for each chunk
        map_tasks = []
        for chunk in data_chunks:
            task_data = {
                "proof_type": proof_type,
                "input_data": chunk,
                "circuit_name": circuit_name
            }
            map_tasks.append(task_data)
            
        # Submit all map tasks
        task_ids = await self.submit_batch_tasks(map_tasks)
        
        # Get all results
        results = await self.get_batch_results(task_ids)
        
        # Reduce phase - combine proofs
        proof_values = [r.proof_data for r in results.values() if r.success]
        
        if not proof_values:
            raise Exception("No successful proofs generated in map phase")
            
        # Execute reduce function
        final_proof = await asyncio.get_event_loop().run_in_executor(
            None, reduce_func, proof_values
        )
        
        return final_proof
        
    def _generate_task_id(self, proof_type: str, input_data: Dict[str, Any]) -> str:
        """Generate unique task ID"""
        data_str = f"{proof_type}:{json.dumps(input_data, sort_keys=True)}"
        return hashlib.sha256(data_str.encode()).hexdigest()[:16]
        
    async def _store_task(self, task: ProofTask):
        """Store task in cache"""
        await asyncio.get_event_loop().run_in_executor(
            None,
            self._task_cache.put,
            task.task_id,
            json.dumps({
                "task_id": task.task_id,
                "proof_type": task.proof_type,
                "input_data": task.input_data,
                "circuit_name": task.circuit_name,
                "priority": task.priority,
                "created_at": task.created_at.isoformat()
            })
        )
        
    async def _store_tasks_batch(self, tasks: List[ProofTask]):
        """Store multiple tasks"""
        task_data = {}
        for task in tasks:
            task_data[task.task_id] = json.dumps({
                "task_id": task.task_id,
                "proof_type": task.proof_type,
                "input_data": task.input_data,
                "circuit_name": task.circuit_name,
                "priority": task.priority,
                "created_at": task.created_at.isoformat()
            })
            
        await asyncio.get_event_loop().run_in_executor(
            None,
            self._task_cache.put_all,
            task_data
        )
        
    async def _get_result(self, task_id: str) -> Optional[ProofResult]:
        """Get result from cache"""
        result_data = await asyncio.get_event_loop().run_in_executor(
            None,
            self._result_cache.get,
            task_id
        )
        
        if result_data:
            data = json.loads(result_data) if isinstance(result_data, str) else result_data
            return ProofResult(
                task_id=data["task_id"],
                proof_data=data["proof_data"],
                success=data["success"],
                error=data.get("error"),
                compute_time_ms=data.get("compute_time_ms", 0),
                node_id=data.get("node_id")
            )
            
        return None
        
    async def register_circuit(self, circuit_name: str, circuit_data: Dict[str, Any]):
        """Register a ZKP circuit for distributed execution"""
        await asyncio.get_event_loop().run_in_executor(
            None,
            self._circuit_cache.put,
            circuit_name,
            json.dumps(circuit_data)
        )
        logger.info(f"Registered ZKP circuit: {circuit_name}")
        
    async def get_compute_stats(self) -> Dict[str, Any]:
        """Get compute grid statistics"""
        stats = await self._worker_pool.get_stats()
        
        # Add cache stats
        task_count = await asyncio.get_event_loop().run_in_executor(
            None, self._task_cache.get_size
        )
        result_count = await asyncio.get_event_loop().run_in_executor(
            None, self._result_cache.get_size
        )
        
        stats.update({
            "pending_tasks": task_count,
            "completed_tasks": result_count
        })
        
        return stats


class ZKPWorkerPool:
    """
    Worker pool for distributed ZKP computation
    """
    
    def __init__(self, ignite_client: IgniteClient):
        self.client = ignite_client
        self._workers = []
        self._task_queue = asyncio.Queue()
        self._active_tasks = {}
        
    async def initialize(self, num_workers: int = 4):
        """Initialize worker pool"""
        # Start workers
        for i in range(num_workers):
            worker = ZKPWorker(i, self.client, self._task_queue)
            self._workers.append(worker)
            asyncio.create_task(worker.run())
            
        logger.info(f"Started {num_workers} ZKP workers")
        
    async def submit_task(self, task: ProofTask):
        """Submit task to worker pool"""
        await self._task_queue.put(task)
        self._active_tasks[task.task_id] = task
        
    async def submit_batch(self, tasks: List[ProofTask]):
        """Submit multiple tasks"""
        # Sort by priority
        sorted_tasks = sorted(tasks, key=lambda t: t.priority, reverse=True)
        
        for task in sorted_tasks:
            await self.submit_task(task)
            
    async def get_stats(self) -> Dict[str, Any]:
        """Get worker pool statistics"""
        return {
            "num_workers": len(self._workers),
            "queue_size": self._task_queue.qsize(),
            "active_tasks": len(self._active_tasks)
        }


class ZKPWorker:
    """
    Worker for processing ZKP tasks
    """
    
    def __init__(self, worker_id: int, ignite_client: IgniteClient, task_queue: asyncio.Queue):
        self.worker_id = worker_id
        self.client = ignite_client
        self.task_queue = task_queue
        self._result_cache = None
        
    async def run(self):
        """Main worker loop"""
        # Get result cache
        self._result_cache = await asyncio.get_event_loop().run_in_executor(
            None, self.client.get_or_create_cache, "zkp_results"
        )
        
        while True:
            try:
                # Get task from queue
                task = await self.task_queue.get()
                
                # Process task
                result = await self._process_task(task)
                
                # Store result
                await self._store_result(result)
                
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")
                
    async def _process_task(self, task: ProofTask) -> ProofResult:
        """Process a ZKP task"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Simulate proof generation (replace with actual ZKP library calls)
            # In production, this would call arkworks-rs or similar
            proof_data = await self._generate_proof(
                task.proof_type,
                task.input_data,
                task.circuit_name
            )
            
            compute_time = (asyncio.get_event_loop().time() - start_time) * 1000
            
            return ProofResult(
                task_id=task.task_id,
                proof_data=proof_data,
                success=True,
                compute_time_ms=compute_time,
                node_id=f"worker_{self.worker_id}"
            )
            
        except Exception as e:
            compute_time = (asyncio.get_event_loop().time() - start_time) * 1000
            
            return ProofResult(
                task_id=task.task_id,
                proof_data={},
                success=False,
                error=str(e),
                compute_time_ms=compute_time,
                node_id=f"worker_{self.worker_id}"
            )
            
    async def _generate_proof(
        self,
        proof_type: str,
        input_data: Dict[str, Any],
        circuit_name: str
    ) -> Dict[str, Any]:
        """Generate actual ZKP (placeholder)"""
        # In production, integrate with arkworks-rs or bellman
        await asyncio.sleep(0.1)  # Simulate computation
        
        return {
            "proof": f"proof_{proof_type}_{circuit_name}",
            "public_inputs": list(input_data.keys()),
            "verification_key": f"vk_{circuit_name}"
        }
        
    async def _store_result(self, result: ProofResult):
        """Store result in cache"""
        result_data = {
            "task_id": result.task_id,
            "proof_data": result.proof_data,
            "success": result.success,
            "error": result.error,
            "compute_time_ms": result.compute_time_ms,
            "node_id": result.node_id
        }
        
        await asyncio.get_event_loop().run_in_executor(
            None,
            self._result_cache.put,
            result.task_id,
            json.dumps(result_data)
        ) 