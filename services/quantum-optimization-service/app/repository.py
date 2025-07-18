"""
Quantum Optimization Repository

Handles database operations for quantum optimization problems and jobs.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_, func, desc
from sqlalchemy.orm import Session
import json

from platformq_shared.repository import BaseRepository, AsyncBaseRepository
from .models import (
    OptimizationProblem,
    OptimizationJob,
    OptimizationIteration,
    QuantumCircuit,
    OptimizationTemplate,
    SolverBenchmark,
    QuantumResourceAllocation,
    ProblemType,
    SolverType,
    JobStatus,
    BackendType
)


class OptimizationProblemRepository(BaseRepository[OptimizationProblem]):
    """Repository for optimization problem management"""
    
    def __init__(self, db: Session):
        super().__init__(OptimizationProblem, db)
    
    def get_by_problem_id(self, problem_id: str) -> Optional[OptimizationProblem]:
        """Get problem by problem_id"""
        return self.db.query(OptimizationProblem).filter(
            OptimizationProblem.problem_id == problem_id
        ).first()
    
    def search_problems(
        self,
        tenant_id: str,
        query: Optional[str] = None,
        problem_type: Optional[ProblemType] = None,
        is_template: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[OptimizationProblem]:
        """Search optimization problems"""
        q = self.db.query(OptimizationProblem).filter(
            OptimizationProblem.tenant_id == tenant_id
        )
        
        if query:
            q = q.filter(
                or_(
                    OptimizationProblem.name.ilike(f"%{query}%"),
                    OptimizationProblem.description.ilike(f"%{query}%")
                )
            )
        
        if problem_type:
            q = q.filter(OptimizationProblem.problem_type == problem_type)
        
        if is_template is not None:
            q = q.filter(OptimizationProblem.is_template == is_template)
        
        return q.offset(offset).limit(limit).all()
    
    def get_public_problems(
        self,
        problem_type: Optional[ProblemType] = None
    ) -> List[OptimizationProblem]:
        """Get public problem examples"""
        q = self.db.query(OptimizationProblem).filter(
            and_(
                OptimizationProblem.is_public == True,
                OptimizationProblem.is_template == True
            )
        )
        
        if problem_type:
            q = q.filter(OptimizationProblem.problem_type == problem_type)
        
        return q.all()
    
    def create_from_template(
        self,
        template_id: str,
        name: str,
        tenant_id: str,
        user_id: str,
        variable_values: Dict[str, Any]
    ) -> OptimizationProblem:
        """Create a problem instance from a template"""
        template = self.get_by_problem_id(template_id)
        if not template or not template.is_template:
            raise ValueError(f"Template {template_id} not found")
        
        # Substitute variables in template
        problem_data = self._substitute_template_variables(
            template.problem_data,
            variable_values
        )
        
        # Create new problem
        problem = OptimizationProblem(
            problem_id=f"problem-{datetime.utcnow().timestamp()}",
            tenant_id=tenant_id,
            name=name,
            description=f"Created from template: {template.name}",
            problem_type=template.problem_type,
            objective_function=problem_data.get("objective_function"),
            constraints=problem_data.get("constraints", []),
            variables=problem_data.get("variables", {}),
            problem_data=problem_data,
            solver_preferences=template.solver_preferences,
            created_by=user_id
        )
        
        self.db.add(problem)
        self.db.commit()
        return problem
    
    def _substitute_template_variables(
        self,
        template_data: Dict[str, Any],
        values: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Substitute template variables with actual values"""
        # Simple string replacement for now
        # In production, use a proper template engine
        data_str = json.dumps(template_data)
        for var, value in values.items():
            data_str = data_str.replace(f"{{{var}}}", str(value))
        return json.loads(data_str)


class OptimizationJobRepository(BaseRepository[OptimizationJob]):
    """Repository for optimization job management"""
    
    def __init__(self, db: Session):
        super().__init__(OptimizationJob, db)
    
    def get_by_job_id(self, job_id: str) -> Optional[OptimizationJob]:
        """Get job by job_id"""
        return self.db.query(OptimizationJob).filter(
            OptimizationJob.job_id == job_id
        ).first()
    
    def get_active_jobs(self, tenant_id: str) -> List[OptimizationJob]:
        """Get all active jobs for a tenant"""
        return self.db.query(OptimizationJob).filter(
            and_(
                OptimizationJob.tenant_id == tenant_id,
                OptimizationJob.status.in_([
                    JobStatus.PENDING,
                    JobStatus.ENCODING,
                    JobStatus.RUNNING,
                    JobStatus.DECODING
                ])
            )
        ).all()
    
    def get_problem_jobs(
        self,
        problem_id: str,
        limit: int = 10
    ) -> List[OptimizationJob]:
        """Get jobs for a specific problem"""
        return self.db.query(OptimizationJob).filter(
            OptimizationJob.problem_id == problem_id
        ).order_by(desc(OptimizationJob.submitted_at)).limit(limit).all()
    
    def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        progress: Optional[float] = None,
        solution: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """Update job status and results"""
        job = self.get_by_job_id(job_id)
        if not job:
            return False
        
        job.status = status
        
        if progress is not None:
            job.progress = progress
        
        if status == JobStatus.RUNNING and not job.started_at:
            job.started_at = datetime.utcnow()
        
        if status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            job.completed_at = datetime.utcnow()
            if job.started_at:
                job.total_time_ms = int(
                    (job.completed_at - job.started_at).total_seconds() * 1000
                )
        
        if solution:
            job.solution = solution.get("variables")
            job.objective_value = solution.get("objective_value")
            job.solution_quality = solution.get("quality", 1.0)
        
        if error_message:
            job.error_message = error_message
        
        self.db.commit()
        return True
    
    def record_iteration(
        self,
        job_id: str,
        iteration_number: int,
        solution: Dict[str, Any],
        objective_value: float,
        metrics: Optional[Dict[str, Any]] = None
    ):
        """Record an optimization iteration"""
        iteration = OptimizationIteration(
            job_id=job_id,
            iteration_number=iteration_number,
            current_solution=solution,
            objective_value=objective_value,
            gradient_norm=metrics.get("gradient_norm") if metrics else None,
            step_size=metrics.get("step_size") if metrics else None,
            improvement=metrics.get("improvement") if metrics else None,
            quantum_state=metrics.get("quantum_state") if metrics else None,
            measurement_outcomes=metrics.get("measurements") if metrics else None
        )
        self.db.add(iteration)
        self.db.commit()
    
    def get_job_statistics(
        self,
        tenant_id: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get job execution statistics"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Job counts by status
        status_counts = self.db.query(
            OptimizationJob.status,
            func.count(OptimizationJob.id)
        ).filter(
            and_(
                OptimizationJob.tenant_id == tenant_id,
                OptimizationJob.submitted_at >= cutoff_date
            )
        ).group_by(OptimizationJob.status).all()
        
        # Average solving time by solver type
        solver_stats = self.db.query(
            OptimizationJob.solver_type,
            func.avg(OptimizationJob.solving_time_ms),
            func.count(OptimizationJob.id)
        ).filter(
            and_(
                OptimizationJob.tenant_id == tenant_id,
                OptimizationJob.status == JobStatus.COMPLETED,
                OptimizationJob.completed_at >= cutoff_date
            )
        ).group_by(OptimizationJob.solver_type).all()
        
        # Success rate
        total_jobs = self.db.query(func.count(OptimizationJob.id)).filter(
            and_(
                OptimizationJob.tenant_id == tenant_id,
                OptimizationJob.submitted_at >= cutoff_date
            )
        ).scalar()
        
        successful_jobs = self.db.query(func.count(OptimizationJob.id)).filter(
            and_(
                OptimizationJob.tenant_id == tenant_id,
                OptimizationJob.status == JobStatus.COMPLETED,
                OptimizationJob.submitted_at >= cutoff_date
            )
        ).scalar()
        
        return {
            "period_days": days,
            "total_jobs": total_jobs,
            "successful_jobs": successful_jobs,
            "success_rate": (successful_jobs / total_jobs) if total_jobs > 0 else 0,
            "status_distribution": dict(status_counts),
            "solver_performance": [
                {
                    "solver_type": solver_type.value,
                    "avg_solving_time_ms": float(avg_time) if avg_time else 0,
                    "job_count": count
                }
                for solver_type, avg_time, count in solver_stats
            ]
        }


class QuantumCircuitRepository(BaseRepository[QuantumCircuit]):
    """Repository for quantum circuit management"""
    
    def __init__(self, db: Session):
        super().__init__(QuantumCircuit, db)
    
    def get_by_circuit_id(self, circuit_id: str) -> Optional[QuantumCircuit]:
        """Get circuit by circuit_id"""
        return self.db.query(QuantumCircuit).filter(
            QuantumCircuit.circuit_id == circuit_id
        ).first()
    
    def search_circuits(
        self,
        tenant_id: str,
        circuit_type: Optional[str] = None,
        min_qubits: Optional[int] = None,
        max_qubits: Optional[int] = None,
        is_template: bool = False
    ) -> List[QuantumCircuit]:
        """Search quantum circuits"""
        q = self.db.query(QuantumCircuit).filter(
            and_(
                QuantumCircuit.tenant_id == tenant_id,
                QuantumCircuit.is_template == is_template
            )
        )
        
        if circuit_type:
            q = q.filter(QuantumCircuit.circuit_type == circuit_type)
        
        if min_qubits:
            q = q.filter(QuantumCircuit.num_qubits >= min_qubits)
        
        if max_qubits:
            q = q.filter(QuantumCircuit.num_qubits <= max_qubits)
        
        return q.all()
    
    def increment_usage(self, circuit_id: str):
        """Increment circuit usage count"""
        circuit = self.get_by_circuit_id(circuit_id)
        if circuit:
            circuit.usage_count += 1
            self.db.commit()


class OptimizationTemplateRepository(BaseRepository[OptimizationTemplate]):
    """Repository for optimization template management"""
    
    def __init__(self, db: Session):
        super().__init__(OptimizationTemplate, db)
    
    def get_by_template_id(self, template_id: str) -> Optional[OptimizationTemplate]:
        """Get template by template_id"""
        return self.db.query(OptimizationTemplate).filter(
            OptimizationTemplate.template_id == template_id
        ).first()
    
    def get_public_templates(
        self,
        category: Optional[str] = None,
        problem_type: Optional[ProblemType] = None
    ) -> List[OptimizationTemplate]:
        """Get public templates"""
        q = self.db.query(OptimizationTemplate).filter(
            OptimizationTemplate.is_public == True
        )
        
        if category:
            q = q.filter(OptimizationTemplate.category == category)
        
        if problem_type:
            q = q.filter(OptimizationTemplate.problem_type == problem_type)
        
        return q.order_by(desc(OptimizationTemplate.usage_count)).all()
    
    def increment_usage(self, template_id: str):
        """Increment template usage count"""
        template = self.get_by_template_id(template_id)
        if template:
            template.usage_count += 1
            self.db.commit()
    
    def update_performance_stats(
        self,
        template_id: str,
        solving_time_ms: int,
        success: bool
    ):
        """Update template performance statistics"""
        template = self.get_by_template_id(template_id)
        if template:
            # Update average solving time
            if template.average_solving_time:
                template.average_solving_time = (
                    template.average_solving_time * template.usage_count +
                    solving_time_ms
                ) / (template.usage_count + 1)
            else:
                template.average_solving_time = solving_time_ms
            
            # Update success rate
            if template.success_rate:
                successful = int(template.success_rate * template.usage_count)
                if success:
                    successful += 1
                template.success_rate = successful / (template.usage_count + 1)
            else:
                template.success_rate = 1.0 if success else 0.0
            
            self.db.commit()


class SolverBenchmarkRepository(BaseRepository[SolverBenchmark]):
    """Repository for solver benchmark management"""
    
    def __init__(self, db: Session):
        super().__init__(SolverBenchmark, db)
    
    def get_benchmarks(
        self,
        problem_type: Optional[ProblemType] = None,
        solver_type: Optional[SolverType] = None,
        min_problem_size: Optional[int] = None,
        max_problem_size: Optional[int] = None
    ) -> List[SolverBenchmark]:
        """Get benchmark results"""
        q = self.db.query(SolverBenchmark)
        
        if problem_type:
            q = q.filter(SolverBenchmark.problem_type == problem_type)
        
        if solver_type:
            q = q.filter(SolverBenchmark.solver_type == solver_type)
        
        if min_problem_size:
            q = q.filter(SolverBenchmark.problem_size >= min_problem_size)
        
        if max_problem_size:
            q = q.filter(SolverBenchmark.problem_size <= max_problem_size)
        
        return q.order_by(desc(SolverBenchmark.benchmark_date)).all()
    
    def get_best_solver(
        self,
        problem_type: ProblemType,
        problem_size: int,
        optimize_for: str = "quality"  # quality, speed, cost
    ) -> Optional[Dict[str, Any]]:
        """Get best solver recommendation based on benchmarks"""
        # Get relevant benchmarks
        benchmarks = self.db.query(SolverBenchmark).filter(
            and_(
                SolverBenchmark.problem_type == problem_type,
                SolverBenchmark.problem_size >= problem_size * 0.8,
                SolverBenchmark.problem_size <= problem_size * 1.2
            )
        ).all()
        
        if not benchmarks:
            return None
        
        # Score each solver
        solver_scores = {}
        
        for benchmark in benchmarks:
            key = (benchmark.solver_type, benchmark.backend_type)
            
            if optimize_for == "quality":
                score = benchmark.solution_quality
            elif optimize_for == "speed":
                score = 1.0 / (benchmark.solving_time_ms + 1)
            elif optimize_for == "cost":
                score = 1.0 / (benchmark.estimated_cost_usd + 0.01)
            else:
                # Balanced score
                score = (
                    benchmark.solution_quality * 0.5 +
                    (1.0 / (benchmark.solving_time_ms + 1)) * 0.3 +
                    (1.0 / (benchmark.estimated_cost_usd + 0.01)) * 0.2
                )
            
            if key in solver_scores:
                solver_scores[key] = (solver_scores[key] + score) / 2
            else:
                solver_scores[key] = score
        
        # Get best solver
        best_solver = max(solver_scores.items(), key=lambda x: x[1])
        
        return {
            "solver_type": best_solver[0][0].value,
            "backend_type": best_solver[0][1].value,
            "score": best_solver[1],
            "based_on_benchmarks": len(benchmarks)
        }


class QuantumResourceAllocationRepository(BaseRepository[QuantumResourceAllocation]):
    """Repository for quantum resource allocation management"""
    
    def __init__(self, db: Session):
        super().__init__(QuantumResourceAllocation, db)
    
    def get_user_allocation(
        self,
        tenant_id: str,
        user_id: str
    ) -> Optional[QuantumResourceAllocation]:
        """Get resource allocation for a user"""
        return self.db.query(QuantumResourceAllocation).filter(
            and_(
                QuantumResourceAllocation.tenant_id == tenant_id,
                QuantumResourceAllocation.user_id == user_id,
                QuantumResourceAllocation.is_active == True
            )
        ).first()
    
    def check_resource_availability(
        self,
        tenant_id: str,
        user_id: str,
        required_credits: float,
        required_qubits: int
    ) -> Dict[str, Any]:
        """Check if user has sufficient resources"""
        allocation = self.get_user_allocation(tenant_id, user_id)
        
        if not allocation:
            return {
                "available": False,
                "reason": "No resource allocation found"
            }
        
        # Check suspension
        if allocation.suspended_until and allocation.suspended_until > datetime.utcnow():
            return {
                "available": False,
                "reason": f"Account suspended until {allocation.suspended_until}"
            }
        
        # Check monthly credits
        remaining_credits = allocation.monthly_quantum_credits - allocation.credits_used_this_month
        if required_credits > remaining_credits:
            return {
                "available": False,
                "reason": f"Insufficient credits: {remaining_credits:.2f} remaining"
            }
        
        # Check daily job limit
        if allocation.daily_job_limit and allocation.jobs_today >= allocation.daily_job_limit:
            return {
                "available": False,
                "reason": f"Daily job limit reached: {allocation.daily_job_limit}"
            }
        
        # Check qubit limit
        if allocation.max_qubits and required_qubits > allocation.max_qubits:
            return {
                "available": False,
                "reason": f"Exceeds qubit limit: {allocation.max_qubits}"
            }
        
        return {
            "available": True,
            "remaining_credits": remaining_credits,
            "priority_level": allocation.priority_level
        }
    
    def consume_resources(
        self,
        tenant_id: str,
        user_id: str,
        credits_used: float
    ) -> bool:
        """Consume quantum credits"""
        allocation = self.get_user_allocation(tenant_id, user_id)
        
        if not allocation:
            return False
        
        allocation.credits_used_this_month += credits_used
        allocation.jobs_today += 1
        
        # Check if need to reset daily counter
        if allocation.last_reset_date and allocation.last_reset_date.date() < datetime.utcnow().date():
            allocation.jobs_today = 1
            allocation.last_reset_date = datetime.utcnow()
        
        self.db.commit()
        return True
    
    def reset_monthly_usage(self):
        """Reset monthly usage for all allocations"""
        self.db.query(QuantumResourceAllocation).update({
            "credits_used_this_month": 0,
            "jobs_today": 0,
            "last_reset_date": datetime.utcnow()
        })
        self.db.commit() 