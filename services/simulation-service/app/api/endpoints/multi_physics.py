"""API endpoints for multi-physics simulations."""
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field
import asyncio
import logging

from app.core.deps import get_current_user
from platformq.shared.multi_physics_orchestrator import (
    MultiPhysicsJobManager,
    CouplingType,
    OptimizationType
)

from crdt import CRDTManager
from wasmtime import Store, Module, Instance
from app.ignite_manager import SimulationIgniteManager

router = APIRouter()
logger = logging.getLogger(__name__)

# Global job manager
job_manager = MultiPhysicsJobManager()
ignite_manager = SimulationIgniteManager()

crdt_manager = CRDTManager()

async def collaborative_edit(simulation_id: str, edit_data: Dict):
    crdt_manager.apply_edit(simulation_id, edit_data)
    # Trigger WASM preview
    with open('preview.wasm', 'rb') as f:
        wasmtime_bytes = f.read()
    module = Module(store.engine, wasmtime_bytes)
    instance = Instance(store, module, [])
    preview = instance.exports['generate_preview'](edit_data)
    return preview


@router.post("/federated/{federation_id}/state")
async def create_federated_session(federation_id: str):
    """Creates a new shared state cache for a federated simulation."""
    await ignite_manager.create_federated_session_cache(federation_id)
    return {"status": "created", "federation_id": federation_id}

@router.put("/federated/{federation_id}/state/{key}")
async def update_shared_state(federation_id: str, key: str, value: Any):
    """Updates a value in the shared state for a federated simulation."""
    await ignite_manager.update_shared_state(federation_id, key, value)
    return {"status": "updated"}

@router.get("/federated/{federation_id}/state/{key}")
async def get_shared_state(federation_id: str, key: str):
    """Gets a value from the shared state for a federated simulation."""
    value = await ignite_manager.get_shared_state(federation_id, key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found in shared state")
    return {"key": key, "value": value}


class DomainConfig(BaseModel):
    """Configuration for a simulation domain."""
    domain_id: str
    physics_type: str
    solver: str
    input_data: Dict[str, Any]
    design_variables: Optional[Dict[str, float]] = None


class CouplingConfig(BaseModel):
    """Configuration for domain coupling."""
    source_domain: str
    target_domain: str
    coupling_type: str
    mapping_function: Optional[str] = None
    transfer_frequency: float = 1.0


class OptimizationConfigRequest(BaseModel):
    """Optimization configuration request."""
    optimization_type: str
    objective_function: str
    constraints: List[Dict[str, Any]]
    parameters: Dict[str, Any] = Field(default_factory=dict)
    max_iterations: int = 100
    tolerance: float = 1e-6


class MultiPhysicsSimulationRequest(BaseModel):
    """Request for multi-physics simulation."""
    domains: List[DomainConfig]
    couplings: List[CouplingConfig]
    optimization: Optional[OptimizationConfigRequest] = None


class ThermalStructuralRequest(BaseModel):
    """Request for thermal-structural coupling."""
    thermal_config: Dict[str, Any]
    structural_config: Dict[str, Any]
    optimization: Optional[Dict[str, Any]] = None


class FluidStructureRequest(BaseModel):
    """Request for fluid-structure interaction."""
    fluid_config: Dict[str, Any]
    structure_config: Dict[str, Any]
    fsi_parameters: Dict[str, Any] = Field(default_factory=dict)
    optimization: Optional[Dict[str, Any]] = None


@router.post("/multi-physics/thermal-structural")
async def create_thermal_structural_simulation(
    request: ThermalStructuralRequest,
    background_tasks: BackgroundTasks,
    current_user = Depends(get_current_user)
) -> Dict[str, Any]:
    """Create and run thermal-structural coupled simulation."""
    try:
        # Add user context
        request.thermal_config["user_id"] = current_user.id
        request.structural_config["user_id"] = current_user.id
        
        # Create simulation
        sim_id = job_manager.create_thermal_structural_coupling(request.dict())
        
        # Run simulation in background
        background_tasks.add_task(
            run_simulation_task,
            sim_id,
            current_user.id
        )
        
        return {
            "simulation_id": sim_id,
            "status": "submitted",
            "coupling_type": "thermal_structural",
            "message": "Thermal-structural simulation submitted"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/multi-physics/fluid-structure")
async def create_fluid_structure_simulation(
    request: FluidStructureRequest,
    background_tasks: BackgroundTasks,
    current_user = Depends(get_current_user)
) -> Dict[str, Any]:
    """Create fluid-structure interaction simulation."""
    try:
        from platformq.shared.multi_physics_orchestrator import (
            MultiPhysicsOrchestrator,
            SimulationDomain,
            CouplingInterface
        )
        
        # Create orchestrator
        orchestrator = MultiPhysicsOrchestrator(f"fsi_{current_user.id}")
        
        # Add fluid domain
        fluid_domain = SimulationDomain(
            domain_id="fluid",
            physics_type="fluid",
            solver="OpenFOAM",
            input_data=request.fluid_config
        )
        orchestrator.add_domain(fluid_domain)
        
        # Add structure domain
        structure_domain = SimulationDomain(
            domain_id="structure",
            physics_type="structural",
            solver="FreeCAD",
            input_data=request.structure_config
        )
        orchestrator.add_domain(structure_domain)
        
        # Add FSI coupling
        fsi_coupling = CouplingInterface(
            interface_id="fsi_interface",
            source_domain="fluid",
            target_domain="structure",
            coupling_type="boundary_condition"
        )
        orchestrator.add_coupling(fsi_coupling)
        
        # Add reverse coupling
        reverse_coupling = CouplingInterface(
            interface_id="fsi_reverse",
            source_domain="structure",
            target_domain="fluid",
            coupling_type="boundary_condition"
        )
        orchestrator.add_coupling(reverse_coupling)
        
        # Store orchestrator
        sim_id = orchestrator.orchestrator_id
        job_manager.orchestrators[sim_id] = orchestrator
        
        # Run in background
        background_tasks.add_task(
            run_simulation_task,
            sim_id,
            current_user.id
        )
        
        return {
            "simulation_id": sim_id,
            "status": "submitted",
            "coupling_type": "fluid_structure",
            "message": "FSI simulation submitted"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/multi-physics/custom")
async def create_custom_multi_physics_simulation(
    request: MultiPhysicsSimulationRequest,
    background_tasks: BackgroundTasks,
    current_user = Depends(get_current_user)
) -> Dict[str, Any]:
    """Create custom multi-physics simulation with arbitrary couplings."""
    try:
        from platformq.shared.multi_physics_orchestrator import (
            MultiPhysicsOrchestrator,
            SimulationDomain,
            CouplingInterface,
            OptimizationConfig,
            OptimizationType
        )
        
        # Create orchestrator
        orchestrator = MultiPhysicsOrchestrator(f"custom_{current_user.id}")
        
        # Add domains
        for domain_config in request.domains:
            domain = SimulationDomain(
                domain_id=domain_config.domain_id,
                physics_type=domain_config.physics_type,
                solver=domain_config.solver,
                input_data=domain_config.input_data
            )
            orchestrator.add_domain(domain)
        
        # Add couplings
        for coupling_config in request.couplings:
            coupling = CouplingInterface(
                interface_id=f"{coupling_config.source_domain}_to_{coupling_config.target_domain}",
                source_domain=coupling_config.source_domain,
                target_domain=coupling_config.target_domain,
                coupling_type=coupling_config.coupling_type,
                mapping_function=coupling_config.mapping_function,
                transfer_frequency=coupling_config.transfer_frequency
            )
            orchestrator.add_coupling(coupling)
        
        # Add optimization if specified
        if request.optimization:
            opt_type = OptimizationType[request.optimization.optimization_type.upper()]
            opt_config = OptimizationConfig(
                optimization_type=opt_type,
                objective_function=request.optimization.objective_function,
                constraints=request.optimization.constraints,
                parameters=request.optimization.parameters,
                max_iterations=request.optimization.max_iterations,
                tolerance=request.optimization.tolerance
            )
            orchestrator.set_optimization(opt_config)
        
        # Store orchestrator
        sim_id = orchestrator.orchestrator_id
        job_manager.orchestrators[sim_id] = orchestrator
        
        # Run in background
        background_tasks.add_task(
            run_simulation_task,
            sim_id,
            current_user.id
        )
        
        return {
            "simulation_id": sim_id,
            "status": "submitted",
            "num_domains": len(request.domains),
            "num_couplings": len(request.couplings),
            "optimization_enabled": request.optimization is not None,
            "message": "Custom multi-physics simulation submitted"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/multi-physics/{simulation_id}/status")
async def get_simulation_status(
    simulation_id: str,
    current_user = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get status of multi-physics simulation."""
    if simulation_id not in job_manager.orchestrators:
        raise HTTPException(status_code=404, detail="Simulation not found")
    
    orchestrator = job_manager.orchestrators[simulation_id]
    
    # Build status response
    status = {
        "simulation_id": simulation_id,
        "domains": {},
        "optimization": None
    }
    
    # Domain statuses
    for domain_id, domain in orchestrator.domains.items():
        status["domains"][domain_id] = {
            "physics_type": domain.physics_type,
            "solver": domain.solver,
            "status": domain.status,
            "has_output": domain.output_data is not None
        }
    
    # Optimization status
    if orchestrator.optimization_config:
        status["optimization"] = {
            "type": orchestrator.optimization_config.optimization_type.value,
            "objective": orchestrator.optimization_config.objective_function,
            "max_iterations": orchestrator.optimization_config.max_iterations
        }
    
    return status


@router.get("/multi-physics/{simulation_id}/results")
async def get_simulation_results(
    simulation_id: str,
    current_user = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get results of completed multi-physics simulation."""
    if simulation_id not in job_manager.orchestrators:
        raise HTTPException(status_code=404, detail="Simulation not found")
    
    orchestrator = job_manager.orchestrators[simulation_id]
    
    # Check if simulation is complete
    all_complete = all(
        domain.status == "completed" 
        for domain in orchestrator.domains.values()
    )
    
    if not all_complete:
        raise HTTPException(status_code=400, detail="Simulation not yet complete")
    
    # Collect results
    results = {
        "simulation_id": simulation_id,
        "domains": {},
        "couplings": [],
        "optimization_results": None
    }
    
    # Domain results
    for domain_id, domain in orchestrator.domains.items():
        results["domains"][domain_id] = {
            "physics_type": domain.physics_type,
            "solver": domain.solver,
            "output_data": domain.output_data
        }
    
    # Coupling information
    for coupling in orchestrator.coupling_interfaces:
        results["couplings"].append({
            "source": coupling.source_domain,
            "target": coupling.target_domain,
            "type": coupling.coupling_type
        })
    
    # Optimization results (if available)
    if orchestrator.optimization_config and hasattr(orchestrator, "_optimization_history"):
        results["optimization_results"] = {
            "final_objective": orchestrator._optimization_history[-1]["objective"],
            "iterations": len(orchestrator._optimization_history),
            "converged": orchestrator._optimization_history[-1]["converged"]
        }
    
    return results


@router.post("/multi-physics/{simulation_id}/stop")
async def stop_simulation(
    simulation_id: str,
    current_user = Depends(get_current_user)
) -> Dict[str, Any]:
    """Stop a running multi-physics simulation."""
    if simulation_id not in job_manager.orchestrators:
        raise HTTPException(status_code=404, detail="Simulation not found")
    
    # Mark all domains as stopped
    orchestrator = job_manager.orchestrators[simulation_id]
    for domain in orchestrator.domains.values():
        domain.status = "stopped"
    
    return {
        "simulation_id": simulation_id,
        "status": "stopped",
        "message": "Simulation stopped by user"
    }


@router.post("/optimization/quantum")
async def submit_quantum_optimization(
    objective: str,
    constraints: List[Dict[str, Any]],
    num_qubits: int = 10,
    circuit_depth: int = 3,
    current_user = Depends(get_current_user)
) -> Dict[str, Any]:
    """Submit standalone quantum optimization job."""
    try:
        from app.services.quantum_service import submit_quantum_job
        
        quantum_job = {
            "user_id": current_user.id,
            "algorithm": "QAOA",
            "problem_type": "optimization",
            "objective_function": objective,
            "constraints": constraints,
            "num_qubits": num_qubits,
            "circuit_depth": circuit_depth
        }
        
        job_id = await submit_quantum_job(quantum_job)
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "algorithm": "QAOA",
            "message": "Quantum optimization job submitted"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/optimization/ml-surrogate")
async def train_ml_surrogate_model(
    training_data: List[Dict[str, Any]],
    model_type: str = "gaussian_process",
    current_user = Depends(get_current_user)
) -> Dict[str, Any]:
    """Train ML surrogate model for optimization."""
    try:
        from app.services.ml_surrogate import train_surrogate_model
        
        model_config = {
            "user_id": current_user.id,
            "model_type": model_type,
            "training_data": training_data
        }
        
        model_id = await train_surrogate_model(model_config)
        
        return {
            "model_id": model_id,
            "status": "training",
            "model_type": model_type,
            "num_samples": len(training_data),
            "message": "ML surrogate model training started"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Background task
async def run_simulation_task(simulation_id: str, user_id: str):
    """Background task to run simulation."""
    try:
        result = await job_manager.run_simulation(simulation_id)
        
        # Store result in cache/database
        # notify_user(user_id, simulation_id, result)
        
    except Exception as e:
        logger.error(f"Simulation {simulation_id} failed: {e}")
        # notify_user_error(user_id, simulation_id, str(e)) 