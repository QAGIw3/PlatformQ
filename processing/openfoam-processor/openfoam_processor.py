"""OpenFOAM processor with distributed computing capabilities."""
import os
import sys
import json
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from platformq.shared.compute_orchestrator import (
    ComputeOrchestrator, 
    ComputeJob, 
    JobStatus
)
from processing.spark.processor_base import ProcessorBase
from platformq_shared.policy_client import PolicyClient

logger = logging.getLogger(__name__)


@dataclass
class OpenFOAMProcessingConfig:
    """Configuration for OpenFOAM processing."""
    mode: str = "metadata"  # "metadata" or "simulate"
    solver: Optional[str] = None
    num_processors: int = 1
    parameters: Dict[str, Any] = None
    extract_fields: List[str] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}
        if self.extract_fields is None:
            self.extract_fields = ["U", "p", "T"]


class OpenFOAMProcessor(ProcessorBase):
    """Processor for OpenFOAM cases with distributed CFD capabilities."""
    
    def __init__(self, tenant_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__("OpenFOAMProcessor", tenant_id, config)
        self.orchestrator = ComputeOrchestrator("openfoam-processor")
        
        # Register job completion handler
        self.orchestrator.register_job_handler("openfoam", self._handle_job_completion)
        self.policy_client = PolicyClient("http://simulation-service:8000")
    
    def validate_config(self, processing_config: Dict[str, Any]) -> bool:
        """Validate processing configuration."""
        config = OpenFOAMProcessingConfig(**processing_config)
        
        if config.mode not in ["metadata", "simulate"]:
            raise ValueError(f"Invalid mode: {config.mode}")
        
        if config.mode == "simulate":
            if not config.solver:
                raise ValueError("Solver must be specified for simulation mode")
            
            supported_solvers = [
                "simpleFoam", "icoFoam", "pimpleFoam", "interFoam",
                "rhoCentralFoam", "sonicFoam", "buoyantSimpleFoam"
            ]
            
            if config.solver not in supported_solvers:
                raise ValueError(f"Unsupported solver: {config.solver}")
        
        return True
    
    async def process(self, federation_id: str, step_data: Dict[str, Any]):
        """Process OpenFOAM case."""
        # Read from shared state
        input_data = await self.policy_client.get_shared_state(federation_id, "flightgear_output")

        # Process data...
        output_data = {"openfoam_results": "..."}

        # Write to shared state
        await self.policy_client.update_shared_state(federation_id, "openfoam_output", output_data)
    
    def _extract_metadata(self, asset_uri: str) -> Dict[str, Any]:
        """Extract metadata from OpenFOAM case."""
        with tempfile.TemporaryDirectory() as temp_dir:
            case_dir = Path(temp_dir) / "case"
            
            # Download case files
            self._download_case(asset_uri, case_dir)
            
            metadata = {
                "case_type": "OpenFOAM",
                "structure": {}
            }
            
            # Parse controlDict
            control_dict = case_dir / "system" / "controlDict"
            if control_dict.exists():
                metadata["control_dict"] = self._parse_control_dict(control_dict)
            
            # Parse fvSchemes
            fv_schemes = case_dir / "system" / "fvSchemes"
            if fv_schemes.exists():
                metadata["numerical_schemes"] = self._parse_dict_file(fv_schemes)
            
            # Parse fvSolution
            fv_solution = case_dir / "system" / "fvSolution"
            if fv_solution.exists():
                metadata["solution_controls"] = self._parse_dict_file(fv_solution)
            
            # Check for mesh
            if (case_dir / "constant" / "polyMesh").exists():
                metadata["has_mesh"] = True
                metadata["mesh_info"] = self._get_mesh_info(case_dir)
            else:
                metadata["has_mesh"] = False
            
            # List available fields
            metadata["available_fields"] = self._list_fields(case_dir)
            
            # Get time directories
            metadata["time_steps"] = self._get_time_steps(case_dir)
            
            return metadata
    
    def _run_simulation(self, asset_uri: str, config: OpenFOAMProcessingConfig) -> Dict[str, Any]:
        """Submit OpenFOAM simulation as distributed job."""
        # Create compute job
        job = ComputeJob(
            job_id="",
            job_type="openfoam",
            input_data={
                "case_path": asset_uri,
                "solver": config.solver,
                "parameters": config.parameters
            },
            output_location=f"output-data/openfoam/{self.tenant_id}",
            parameters={
                "num_processors": config.num_processors,
                "extract_fields": config.extract_fields
            }
        )
        
        # Submit job
        job_id = self.orchestrator.submit_job(job)
        
        # For distributed mode, return job info immediately
        return {
            "status": "submitted",
            "job_id": job_id,
            "job_type": "openfoam",
            "message": f"OpenFOAM simulation submitted with {config.num_processors} processors"
        }
    
    def _handle_job_completion(self, job: ComputeJob):
        """Handle completed OpenFOAM job."""
        logger.info(f"OpenFOAM job {job.job_id} completed with status {job.status}")
        
        if job.status == JobStatus.COMPLETED:
            # Process results
            results = job.result
            
            # Store convergence history
            if "convergence" in results:
                self._store_convergence_data(job.job_id, results["convergence"])
            
            # Generate visualization if needed
            if "output_path" in results:
                self._generate_visualizations(results["output_path"])
    
    def _download_case(self, asset_uri: str, local_dir: Path):
        """Download OpenFOAM case from storage."""
        # Implementation depends on storage backend
        # For now, assume MinIO/S3
        local_dir.mkdir(parents=True, exist_ok=True)
        
        # Download recursively
        self.download_from_s3(asset_uri, str(local_dir))
    
    def _parse_control_dict(self, dict_path: Path) -> Dict[str, Any]:
        """Parse OpenFOAM controlDict file."""
        params = {}
        
        with open(dict_path, 'r') as f:
            content = f.read()
        
        # Simple parameter extraction
        import re
        param_pattern = re.compile(r'(\w+)\s+([^;]+);')
        
        for match in param_pattern.finditer(content):
            key = match.group(1)
            value = match.group(2).strip()
            
            # Try to parse numeric values
            try:
                if '.' in value:
                    params[key] = float(value)
                else:
                    params[key] = int(value)
            except ValueError:
                params[key] = value
        
        return params
    
    def _parse_dict_file(self, dict_path: Path) -> Dict[str, Any]:
        """Parse generic OpenFOAM dictionary file."""
        # Similar to _parse_control_dict but more generic
        return self._parse_control_dict(dict_path)
    
    def _get_mesh_info(self, case_dir: Path) -> Dict[str, Any]:
        """Extract mesh information."""
        mesh_info = {}
        
        # Check for blockMeshDict
        block_mesh = case_dir / "system" / "blockMeshDict"
        if block_mesh.exists():
            mesh_info["mesh_generator"] = "blockMesh"
        
        # Check for snappyHexMeshDict
        snappy_mesh = case_dir / "system" / "snappyHexMeshDict"
        if snappy_mesh.exists():
            mesh_info["mesh_generator"] = "snappyHexMesh"
        
        # Count mesh files
        poly_mesh = case_dir / "constant" / "polyMesh"
        if poly_mesh.exists():
            mesh_files = list(poly_mesh.iterdir())
            mesh_info["mesh_files_count"] = len(mesh_files)
        
        return mesh_info
    
    def _list_fields(self, case_dir: Path) -> List[str]:
        """List available fields in the case."""
        fields = set()
        
        # Check all time directories
        for item in case_dir.iterdir():
            if item.is_dir():
                try:
                    float(item.name)  # Check if it's a time directory
                    for field in item.iterdir():
                        if field.is_file():
                            fields.add(field.name)
                except ValueError:
                    continue
        
        return sorted(list(fields))
    
    def _get_time_steps(self, case_dir: Path) -> List[float]:
        """Get available time steps."""
        time_steps = []
        
        for item in case_dir.iterdir():
            if item.is_dir():
                try:
                    time_value = float(item.name)
                    time_steps.append(time_value)
                except ValueError:
                    continue
        
        return sorted(time_steps)
    
    def _store_convergence_data(self, job_id: str, convergence_data: Dict[str, Any]):
        """Store convergence history for analysis."""
        # Store in Ignite cache for fast retrieval
        convergence_cache = self.orchestrator.ignite_client.get_or_create_cache(
            "openfoam_convergence"
        )
        convergence_cache.put(job_id, convergence_data)
    
    def _generate_visualizations(self, output_path: str):
        """Generate visualizations from simulation results."""
        # This could trigger a separate visualization job
        # using ParaView or similar tools
        logger.info(f"Visualization generation for {output_path} queued")
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a simulation job."""
        job = self.orchestrator.get_job_status(job_id)
        
        if job:
            return {
                "job_id": job.job_id,
                "status": job.status.value,
                "created_at": job.created_at,
                "started_at": job.started_at,
                "completed_at": job.completed_at,
                "error": job.error,
                "result": job.result
            }
        else:
            return {"error": "Job not found"}
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running simulation."""
        return self.orchestrator.cancel_job(job_id)
    
    def list_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List simulation jobs."""
        jobs = self.orchestrator.list_jobs(
            status=JobStatus(status) if status else None
        )
        
        return [
            {
                "job_id": job.job_id,
                "status": job.status.value,
                "created_at": job.created_at,
                "solver": job.input_data.get("solver")
            }
            for job in jobs
        ]


# Main entry point for backward compatibility
def extract_openfoam_metadata(case_dir: str) -> Dict[str, Any]:
    """Legacy function for metadata extraction."""
    processor = OpenFOAMProcessor("default")
    return processor._extract_metadata(f"file://{case_dir}")


if __name__ == '__main__':
    # Example usage
    processor = OpenFOAMProcessor("test-tenant")
    
    # Metadata extraction
    metadata_config = {
        "mode": "metadata"
    }
    
    # Simulation
    simulation_config = {
        "mode": "simulate",
        "solver": "simpleFoam",
        "num_processors": 4,
        "parameters": {
            "endTime": 1000,
            "deltaT": 0.01
        }
    }
    
    # Process based on config
    # result = processor.process("s3://bucket/case", simulation_config) 