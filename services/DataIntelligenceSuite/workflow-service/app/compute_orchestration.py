"""
Workflow Compute Orchestration

Integrates Airflow workflows with compute derivatives for resource provisioning.
Handles automatic compute allocation for DAG tasks and complex workflows.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import json

import httpx
from platformq_shared.cache import CacheManager
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class WorkflowResourceType(Enum):
    DATA_PROCESSING = "data_processing"
    ML_TRAINING = "ml_training"
    SIMULATION = "simulation"
    ETL_PIPELINE = "etl_pipeline"
    BATCH_ANALYTICS = "batch_analytics"
    STREAM_PROCESSING = "stream_processing"


class TaskResourceProfile(Enum):
    MINIMAL = "minimal"  # < 1 GPU hour
    LIGHT = "light"  # 1-5 GPU hours
    MODERATE = "moderate"  # 5-20 GPU hours
    HEAVY = "heavy"  # 20-100 GPU hours
    INTENSIVE = "intensive"  # > 100 GPU hours


@dataclass
class WorkflowResourceEstimate:
    """Resource estimate for workflow execution"""
    workflow_id: str
    dag_id: str
    total_tasks: int
    task_estimates: Dict[str, float]  # task_id -> gpu_hours
    total_gpu_hours: float
    total_cpu_hours: float
    total_memory_gb_hours: float
    estimated_duration: timedelta
    critical_path_tasks: List[str]
    parallelism_factor: float  # How much can be parallelized
    
    def get_resource_profile(self) -> TaskResourceProfile:
        """Determine overall resource profile"""
        if self.total_gpu_hours < 1:
            return TaskResourceProfile.MINIMAL
        elif self.total_gpu_hours < 5:
            return TaskResourceProfile.LIGHT
        elif self.total_gpu_hours < 20:
            return TaskResourceProfile.MODERATE
        elif self.total_gpu_hours < 100:
            return TaskResourceProfile.HEAVY
        else:
            return TaskResourceProfile.INTENSIVE


@dataclass
class WorkflowAllocationResult:
    """Result of compute allocation for workflow"""
    success: bool
    workflow_id: str
    dag_id: str
    allocation_strategy: str
    task_allocations: Dict[str, Dict[str, Any]]  # task_id -> allocation details
    total_cost: Decimal
    contracts: Dict[str, List[str]]
    estimated_start_time: datetime
    estimated_completion_time: datetime
    optimization_notes: List[str]


class WorkflowComputeOrchestrator:
    """Orchestrates compute resources for Airflow workflows"""
    
    def __init__(
        self,
        derivatives_engine_url: str = "http://derivatives-engine-service:8000",
        airflow_api_url: str = "http://airflow-webserver:8080",
        cache_manager: Optional[CacheManager] = None,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.derivatives_url = derivatives_engine_url
        self.airflow_url = airflow_api_url
        self.cache = cache_manager or CacheManager()
        self.event_publisher = event_publisher
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def estimate_workflow_resources(
        self,
        dag_id: str,
        workflow_config: Dict[str, Any]
    ) -> WorkflowResourceEstimate:
        """Estimate resources needed for workflow execution"""
        try:
            # Get DAG structure from Airflow
            dag_response = await self.http_client.get(
                f"{self.airflow_url}/api/v1/dags/{dag_id}/tasks"
            )
            
            if dag_response.status_code != 200:
                raise Exception(f"Failed to get DAG tasks: {dag_response.text}")
                
            tasks = dag_response.json().get("tasks", [])
            
            # Estimate resources per task
            task_estimates = {}
            total_gpu_hours = 0.0
            total_cpu_hours = 0.0
            total_memory_gb_hours = 0.0
            
            for task in tasks:
                task_id = task["task_id"]
                task_type = task.get("task_type", "PythonOperator")
                
                # Estimate based on task type and config
                estimate = self._estimate_task_resources(task_type, task, workflow_config)
                task_estimates[task_id] = estimate["gpu_hours"]
                
                total_gpu_hours += estimate["gpu_hours"]
                total_cpu_hours += estimate["cpu_hours"]
                total_memory_gb_hours += estimate["memory_gb_hours"]
                
            # Calculate critical path and parallelism
            critical_path = self._calculate_critical_path(tasks)
            parallelism = self._calculate_parallelism_factor(tasks)
            
            # Estimate duration based on critical path and available parallelism
            critical_path_hours = sum(task_estimates.get(t, 0) for t in critical_path)
            estimated_duration = timedelta(hours=critical_path_hours / parallelism)
            
            return WorkflowResourceEstimate(
                workflow_id=workflow_config.get("workflow_id", f"{dag_id}_{datetime.utcnow().timestamp()}"),
                dag_id=dag_id,
                total_tasks=len(tasks),
                task_estimates=task_estimates,
                total_gpu_hours=total_gpu_hours,
                total_cpu_hours=total_cpu_hours,
                total_memory_gb_hours=total_memory_gb_hours,
                estimated_duration=estimated_duration,
                critical_path_tasks=critical_path,
                parallelism_factor=parallelism
            )
            
        except Exception as e:
            logger.error(f"Failed to estimate workflow resources: {e}")
            raise
            
    def _estimate_task_resources(
        self,
        task_type: str,
        task_config: Dict[str, Any],
        workflow_config: Dict[str, Any]
    ) -> Dict[str, float]:
        """Estimate resources for a single task"""
        # Base estimates by task type
        base_estimates = {
            "SparkSubmitOperator": {"gpu": 10.0, "cpu": 40.0, "memory": 160.0},
            "KubernetesPodOperator": {"gpu": 5.0, "cpu": 20.0, "memory": 80.0},
            "PythonOperator": {"gpu": 0.0, "cpu": 2.0, "memory": 8.0},
            "BashOperator": {"gpu": 0.0, "cpu": 1.0, "memory": 4.0},
            "DockerOperator": {"gpu": 2.0, "cpu": 8.0, "memory": 32.0},
            "MLflowOperator": {"gpu": 20.0, "cpu": 16.0, "memory": 64.0},
            "DbtRunOperator": {"gpu": 0.0, "cpu": 4.0, "memory": 16.0}
        }
        
        # Get base estimate
        base = base_estimates.get(task_type, {"gpu": 1.0, "cpu": 4.0, "memory": 16.0})
        
        # Apply task-specific multipliers
        gpu_hours = base["gpu"]
        cpu_hours = base["cpu"]
        memory_gb_hours = base["memory"]
        
        # Check for ML tasks
        if "ml_model" in task_config or "training" in task_config.get("task_id", "").lower():
            gpu_hours *= 2.0
            
        # Check for data size
        data_size_gb = workflow_config.get("data_size_gb", 1.0)
        if data_size_gb > 100:
            gpu_hours *= 1.5
            cpu_hours *= 2.0
            memory_gb_hours *= 2.0
            
        # Apply duration estimate
        estimated_duration = task_config.get("estimated_duration_hours", 1.0)
        
        return {
            "gpu_hours": gpu_hours * estimated_duration,
            "cpu_hours": cpu_hours * estimated_duration,
            "memory_gb_hours": memory_gb_hours * estimated_duration
        }
        
    def _calculate_critical_path(self, tasks: List[Dict[str, Any]]) -> List[str]:
        """Calculate critical path through DAG"""
        # Simplified critical path calculation
        # In production, would use proper graph algorithms
        critical_tasks = []
        
        # Find tasks with no upstream dependencies
        start_tasks = [t["task_id"] for t in tasks if not t.get("upstream_task_ids", [])]
        
        # Follow longest path
        for start in start_tasks:
            path = [start]
            current = start
            
            while True:
                # Find downstream tasks
                downstream = [
                    t["task_id"] for t in tasks 
                    if current in t.get("upstream_task_ids", [])
                ]
                
                if not downstream:
                    break
                    
                # Choose task with most dependencies (simplified)
                current = downstream[0]
                path.append(current)
                
            if len(path) > len(critical_tasks):
                critical_tasks = path
                
        return critical_tasks
        
    def _calculate_parallelism_factor(self, tasks: List[Dict[str, Any]]) -> float:
        """Calculate how much workflow can be parallelized"""
        # Count max concurrent tasks at any level
        levels = {}
        
        for task in tasks:
            # Simple level assignment based on dependencies
            deps = len(task.get("upstream_task_ids", []))
            level = levels.get(deps, [])
            level.append(task["task_id"])
            levels[deps] = level
            
        max_parallel = max(len(tasks) for tasks in levels.values()) if levels else 1
        
        # Return parallelism factor (1.0 = sequential, higher = more parallel)
        return min(max_parallel, 10.0)  # Cap at 10x parallelism
        
    async def allocate_workflow_compute(
        self,
        dag_id: str,
        workflow_config: Dict[str, Any],
        tenant_id: str,
        user_id: str
    ) -> WorkflowAllocationResult:
        """Allocate compute resources for workflow execution"""
        try:
            # First estimate resources
            estimate = await self.estimate_workflow_resources(dag_id, workflow_config)
            
            # Choose allocation strategy based on profile
            profile = estimate.get_resource_profile()
            
            if profile in [TaskResourceProfile.MINIMAL, TaskResourceProfile.LIGHT]:
                return await self._allocate_spot_workflow(estimate, tenant_id, user_id)
            elif profile == TaskResourceProfile.MODERATE:
                return await self._allocate_mixed_workflow(estimate, tenant_id, user_id)
            else:  # HEAVY or INTENSIVE
                return await self._allocate_reserved_workflow(estimate, tenant_id, user_id)
                
        except Exception as e:
            logger.error(f"Failed to allocate workflow compute: {e}")
            return WorkflowAllocationResult(
                success=False,
                workflow_id=estimate.workflow_id if 'estimate' in locals() else "",
                dag_id=dag_id,
                allocation_strategy="failed",
                task_allocations={},
                total_cost=Decimal("0"),
                contracts={},
                estimated_start_time=datetime.utcnow(),
                estimated_completion_time=datetime.utcnow(),
                optimization_notes=[f"Allocation failed: {str(e)}"]
            )
            
    async def _allocate_spot_workflow(
        self,
        estimate: WorkflowResourceEstimate,
        tenant_id: str,
        user_id: str
    ) -> WorkflowAllocationResult:
        """Allocate spot resources for light workflows"""
        try:
            task_allocations = {}
            total_cost = Decimal("0")
            contracts = {"spot": []}
            
            # Allocate spot resources for each task
            for task_id, gpu_hours in estimate.task_estimates.items():
                if gpu_hours > 0:
                    order_request = {
                        "resourceType": "GPU",
                        "quantity": gpu_hours,
                        "orderType": "MARKET",
                        "side": "BUY",
                        "specs": {
                            "gpu_type": "NVIDIA_T4",  # Cheaper GPU for light tasks
                            "cpu_cores": 4,
                            "memory_gb": 16,
                            "storage_gb": 50
                        },
                        "metadata": {
                            "workflow_id": estimate.workflow_id,
                            "dag_id": estimate.dag_id,
                            "task_id": task_id
                        }
                    }
                    
                    response = await self.http_client.post(
                        f"{self.derivatives_url}/api/v1/compute/spot/orders",
                        json=order_request,
                        headers={
                            "X-Tenant-ID": tenant_id,
                            "X-User-ID": user_id
                        }
                    )
                    
                    if response.status_code == 201:
                        order = response.json()
                        task_allocations[task_id] = {
                            "order_id": order["order_id"],
                            "resources": order["allocated_resources"],
                            "cost": order["total_cost"]
                        }
                        total_cost += Decimal(str(order["total_cost"]))
                        contracts["spot"].append(order["order_id"])
                        
            return WorkflowAllocationResult(
                success=len(task_allocations) > 0,
                workflow_id=estimate.workflow_id,
                dag_id=estimate.dag_id,
                allocation_strategy="spot",
                task_allocations=task_allocations,
                total_cost=total_cost,
                contracts=contracts,
                estimated_start_time=datetime.utcnow(),
                estimated_completion_time=datetime.utcnow() + estimate.estimated_duration,
                optimization_notes=["Using spot instances for cost efficiency"]
            )
            
        except Exception as e:
            logger.error(f"Spot workflow allocation failed: {e}")
            raise
            
    async def _allocate_mixed_workflow(
        self,
        estimate: WorkflowResourceEstimate,
        tenant_id: str,
        user_id: str
    ) -> WorkflowAllocationResult:
        """Use mixed strategy for moderate workflows"""
        try:
            task_allocations = {}
            total_cost = Decimal("0")
            contracts = {"spot": [], "futures": []}
            
            # Critical path tasks get futures, others get spot
            for task_id, gpu_hours in estimate.task_estimates.items():
                if gpu_hours > 0:
                    is_critical = task_id in estimate.critical_path_tasks
                    
                    if is_critical:
                        # Use futures for guaranteed execution
                        futures_request = {
                            "resourceType": "GPU",
                            "quantity": gpu_hours,
                            "deliveryTime": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
                            "duration": int(gpu_hours * 3600),
                            "specs": {
                                "gpu_type": "NVIDIA_V100",
                                "cpu_cores": 8,
                                "memory_gb": 32,
                                "storage_gb": 100
                            }
                        }
                        
                        response = await self.http_client.post(
                            f"{self.derivatives_url}/api/v1/compute/futures/contracts",
                            json=futures_request,
                            headers={
                                "X-Tenant-ID": tenant_id,
                                "X-User-ID": user_id
                            }
                        )
                        
                        if response.status_code == 201:
                            contract = response.json()
                            task_allocations[task_id] = {
                                "contract_id": contract["contract_id"],
                                "resources": contract["resources"],
                                "cost": contract["total_price"],
                                "type": "futures"
                            }
                            total_cost += Decimal(str(contract["total_price"]))
                            contracts["futures"].append(contract["contract_id"])
                    else:
                        # Use spot for non-critical tasks
                        order_request = {
                            "resourceType": "GPU",
                            "quantity": gpu_hours,
                            "orderType": "LIMIT",
                            "side": "BUY",
                            "price": 40.0,  # Competitive price
                            "specs": {
                                "gpu_type": "NVIDIA_T4",
                                "cpu_cores": 4,
                                "memory_gb": 16,
                                "storage_gb": 50
                            }
                        }
                        
                        response = await self.http_client.post(
                            f"{self.derivatives_url}/api/v1/compute/spot/orders",
                            json=order_request,
                            headers={
                                "X-Tenant-ID": tenant_id,
                                "X-User-ID": user_id
                            }
                        )
                        
                        if response.status_code == 201:
                            order = response.json()
                            task_allocations[task_id] = {
                                "order_id": order["order_id"],
                                "resources": order["allocated_resources"],
                                "cost": order["total_cost"],
                                "type": "spot"
                            }
                            total_cost += Decimal(str(order["total_cost"]))
                            contracts["spot"].append(order["order_id"])
                            
            return WorkflowAllocationResult(
                success=len(task_allocations) > 0,
                workflow_id=estimate.workflow_id,
                dag_id=estimate.dag_id,
                allocation_strategy="mixed",
                task_allocations=task_allocations,
                total_cost=total_cost,
                contracts=contracts,
                estimated_start_time=datetime.utcnow() + timedelta(hours=1),
                estimated_completion_time=datetime.utcnow() + timedelta(hours=1) + estimate.estimated_duration,
                optimization_notes=[
                    "Critical path tasks use futures for reliability",
                    "Non-critical tasks use spot for cost savings"
                ]
            )
            
        except Exception as e:
            logger.error(f"Mixed workflow allocation failed: {e}")
            raise
            
    async def _allocate_reserved_workflow(
        self,
        estimate: WorkflowResourceEstimate,
        tenant_id: str,
        user_id: str
    ) -> WorkflowAllocationResult:
        """Reserve resources for heavy workflows"""
        try:
            # For large workflows, create a capacity reservation
            reservation_request = {
                "resourceType": "GPU",
                "quantity": estimate.total_gpu_hours,
                "startTime": (datetime.utcnow() + timedelta(hours=2)).isoformat(),
                "duration": int(estimate.estimated_duration.total_seconds()),
                "specs": {
                    "gpu_type": "NVIDIA_A100",
                    "cpu_cores": 32,
                    "memory_gb": 128,
                    "storage_gb": 1000
                },
                "workflow_metadata": {
                    "workflow_id": estimate.workflow_id,
                    "dag_id": estimate.dag_id,
                    "total_tasks": estimate.total_tasks,
                    "parallelism_factor": estimate.parallelism_factor
                }
            }
            
            response = await self.http_client.post(
                f"{self.derivatives_url}/api/v1/compute/futures/capacity-reservation",
                json=reservation_request,
                headers={
                    "X-Tenant-ID": tenant_id,
                    "X-User-ID": user_id
                }
            )
            
            if response.status_code == 201:
                reservation = response.json()
                
                # Create task allocations from reservation
                task_allocations = {}
                for task_id, gpu_hours in estimate.task_estimates.items():
                    if gpu_hours > 0:
                        task_allocations[task_id] = {
                            "reservation_id": reservation["reservation_id"],
                            "resources": {
                                "gpu_hours": gpu_hours,
                                "gpu_type": "NVIDIA_A100",
                                "reserved": True
                            },
                            "cost": float(Decimal(str(reservation["total_price"])) * Decimal(str(gpu_hours)) / Decimal(str(estimate.total_gpu_hours))),
                            "type": "reserved"
                        }
                        
                return WorkflowAllocationResult(
                    success=True,
                    workflow_id=estimate.workflow_id,
                    dag_id=estimate.dag_id,
                    allocation_strategy="reserved",
                    task_allocations=task_allocations,
                    total_cost=Decimal(str(reservation["total_price"])),
                    contracts={"reservation": [reservation["reservation_id"]]},
                    estimated_start_time=datetime.utcnow() + timedelta(hours=2),
                    estimated_completion_time=datetime.utcnow() + timedelta(hours=2) + estimate.estimated_duration,
                    optimization_notes=[
                        "Reserved dedicated capacity for workflow",
                        f"Bulk discount applied: {reservation.get('discount_percentage', 0)}%"
                    ]
                )
                
        except Exception as e:
            logger.error(f"Reserved workflow allocation failed: {e}")
            raise
            
    async def optimize_recurring_workflow(
        self,
        dag_id: str,
        schedule: str,  # Cron expression
        duration_days: int,
        workflow_config: Dict[str, Any],
        tenant_id: str
    ) -> Dict[str, Any]:
        """Optimize resource allocation for recurring workflows"""
        try:
            # Estimate single run
            single_estimate = await self.estimate_workflow_resources(dag_id, workflow_config)
            
            # Calculate number of runs based on schedule
            # Simplified - in production would parse cron properly
            runs_per_day = 1
            if "hourly" in schedule:
                runs_per_day = 24
            elif "*/6" in schedule:
                runs_per_day = 4
                
            total_runs = runs_per_day * duration_days
            total_gpu_hours = single_estimate.total_gpu_hours * total_runs
            
            # Get pricing options
            pricing_response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/pricing-options",
                params={
                    "resource_type": "GPU",
                    "quantity": total_gpu_hours,
                    "duration_days": duration_days
                }
            )
            
            recommendations = {
                "dag_id": dag_id,
                "schedule": schedule,
                "duration_days": duration_days,
                "total_runs": total_runs,
                "single_run_gpu_hours": single_estimate.total_gpu_hours,
                "total_gpu_hours": total_gpu_hours,
                "pricing_options": {}
            }
            
            if pricing_response.status_code == 200:
                pricing = pricing_response.json()
                
                # Calculate costs for different strategies
                spot_cost = Decimal(str(pricing["spot_price"])) * Decimal(str(total_gpu_hours))
                futures_cost = Decimal(str(pricing["futures_price"])) * Decimal(str(total_gpu_hours))
                subscription_cost = Decimal(str(pricing.get("subscription_price", futures_cost * 0.8)))
                
                recommendations["pricing_options"] = {
                    "spot": {
                        "total_cost": float(spot_cost),
                        "per_run_cost": float(spot_cost / total_runs),
                        "risk": "high",
                        "flexibility": "high"
                    },
                    "futures": {
                        "total_cost": float(futures_cost),
                        "per_run_cost": float(futures_cost / total_runs),
                        "risk": "low",
                        "flexibility": "medium"
                    },
                    "subscription": {
                        "total_cost": float(subscription_cost),
                        "per_run_cost": float(subscription_cost / total_runs),
                        "risk": "very_low",
                        "flexibility": "low",
                        "discount": f"{int((1 - subscription_cost/futures_cost) * 100)}%"
                    }
                }
                
                # Recommend based on characteristics
                if total_runs < 10:
                    recommendations["recommended_strategy"] = "spot"
                    recommendations["reasoning"] = "Low frequency makes spot cost-effective"
                elif total_runs < 100:
                    recommendations["recommended_strategy"] = "futures"
                    recommendations["reasoning"] = "Moderate frequency benefits from guaranteed capacity"
                else:
                    recommendations["recommended_strategy"] = "subscription"
                    recommendations["reasoning"] = "High frequency justifies subscription discount"
                    
            return recommendations
            
        except Exception as e:
            logger.error(f"Failed to optimize recurring workflow: {e}")
            return {"error": str(e)}
            
    async def create_workflow_hedge(
        self,
        workflow_id: str,
        estimated_cost: Decimal,
        hedge_percentage: float,
        tenant_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Create cost hedge for workflow using options"""
        try:
            # Buy call options to hedge against price increases
            hedge_amount = float(estimated_cost) * hedge_percentage
            
            options_request = {
                "optionType": "CALL",
                "resourceType": "GPU",
                "notionalValue": hedge_amount,
                "strikePrice": 50.0,  # Current market price
                "expiry": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                "exerciseStyle": "EUROPEAN"
            }
            
            response = await self.http_client.post(
                f"{self.derivatives_url}/api/v1/compute/options/hedge",
                json=options_request,
                headers={
                    "X-Tenant-ID": tenant_id,
                    "X-User-ID": user_id
                }
            )
            
            if response.status_code == 201:
                hedge = response.json()
                
                return {
                    "workflow_id": workflow_id,
                    "hedge_created": True,
                    "option_id": hedge["option_id"],
                    "premium_paid": hedge["premium"],
                    "protected_value": hedge_amount,
                    "protection_level": f"{int(hedge_percentage * 100)}%",
                    "expiry": hedge["expiry"],
                    "break_even_price": 50.0 + hedge["premium"] / hedge_amount
                }
                
        except Exception as e:
            logger.error(f"Failed to create workflow hedge: {e}")
            
        return {
            "workflow_id": workflow_id,
            "hedge_created": False,
            "error": "Failed to create hedge"
        }
        
    async def close(self):
        """Cleanup resources"""
        await self.http_client.aclose() 