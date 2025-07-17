"""OpenFOAM worker for distributed CFD simulations."""
import os
import sys
import json
import time
import shutil
import subprocess
import tempfile
import traceback
import logging
from typing import Dict, Any, List, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base.worker_base import ComputeWorker
from openfoam_utils import (
    parse_openfoam_case, 
    decompose_case, 
    run_solver, 
    reconstruct_case,
    extract_results
)


class OpenFOAMWorker(ComputeWorker):
    """Worker for distributed OpenFOAM CFD simulations."""
    
    def __init__(self):
        super().__init__("openfoam")
        self.supported_solvers = [
            "simpleFoam", "icoFoam", "pimpleFoam", "interFoam",
            "rhoCentralFoam", "sonicFoam", "buoyantSimpleFoam"
        ]
    
    def validate_job(self, job_data: Dict[str, Any]) -> bool:
        """Validate OpenFOAM job data."""
        required_fields = ["case_path", "solver", "parameters"]
        
        for field in required_fields:
            if field not in job_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate solver
        solver = job_data["solver"]
        if solver not in self.supported_solvers:
            raise ValueError(f"Unsupported solver: {solver}. Supported: {self.supported_solvers}")
        
        # Validate parameters
        params = job_data["parameters"]
        if "num_processors" in params:
            if not isinstance(params["num_processors"], int) or params["num_processors"] < 1:
                raise ValueError("num_processors must be a positive integer")
        
        return True
    
    def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an OpenFOAM CFD simulation job."""
        job_id = job_data["job_id"]
        case_path = job_data["case_path"]
        solver = job_data["solver"]
        parameters = job_data["parameters"]
        
        # Create temporary working directory
        with tempfile.TemporaryDirectory() as work_dir:
            work_path = Path(work_dir)
            case_dir = work_path / "case"
            
            try:
                # Download case files from MinIO
                logger.info(f"Downloading case files from {case_path}")
                self._download_case_files(case_path, case_dir)
                
                # Parse case configuration
                case_config = parse_openfoam_case(case_dir)
                
                # Apply any parameter overrides
                if "deltaT" in parameters:
                    self._update_control_dict(case_dir, {"deltaT": parameters["deltaT"]})
                if "endTime" in parameters:
                    self._update_control_dict(case_dir, {"endTime": parameters["endTime"]})
                
                # Determine if parallel execution is needed
                num_processors = parameters.get("num_processors", 1)
                
                if num_processors > 1:
                    # Parallel execution
                    result = self._run_parallel_simulation(
                        case_dir, solver, num_processors, parameters
                    )
                else:
                    # Serial execution
                    result = self._run_serial_simulation(
                        case_dir, solver, parameters
                    )
                
                # Extract results and metrics
                results_data = extract_results(case_dir, parameters.get("extract_fields", []))
                
                # Upload results to MinIO
                output_path = job_data.get("output_location", f"output-data/{job_id}")
                self._upload_results(case_dir, output_path)
                
                return {
                    "status": "completed",
                    "solver": solver,
                    "num_processors": num_processors,
                    "case_config": case_config,
                    "results": results_data,
                    "output_path": output_path,
                    "convergence": result.get("convergence", {}),
                    "execution_time": result.get("execution_time", 0)
                }
                
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                raise
    
    def _download_case_files(self, case_path: str, local_dir: Path):
        """Download OpenFOAM case files from MinIO."""
        # Create case directory structure
        local_dir.mkdir(parents=True, exist_ok=True)
        
        # List all files in the case path
        bucket, prefix = case_path.split('/', 1)
        
        objects = self.minio_client.list_objects(bucket, prefix=prefix, recursive=True)
        
        for obj in objects:
            # Construct local file path
            relative_path = obj.object_name[len(prefix):].lstrip('/')
            local_file = local_dir / relative_path
            
            # Create parent directory
            local_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Download file
            self.minio_client.fget_object(bucket, obj.object_name, str(local_file))
    
    def _upload_results(self, case_dir: Path, output_path: str):
        """Upload simulation results to MinIO."""
        bucket, prefix = output_path.split('/', 1)
        
        # Upload all result files
        for root, dirs, files in os.walk(case_dir):
            # Skip processor directories for parallel cases
            if "processor" in root:
                continue
                
            for file in files:
                file_path = Path(root) / file
                relative_path = file_path.relative_to(case_dir)
                
                # Only upload result directories and logs
                if any(part in str(relative_path) for part in ["0", "constant", "system", "log"]):
                    object_name = f"{prefix}/{relative_path}"
                    self.minio_client.fput_object(bucket, object_name, str(file_path))
    
    def _update_control_dict(self, case_dir: Path, updates: Dict[str, Any]):
        """Update controlDict parameters."""
        control_dict_path = case_dir / "system" / "controlDict"
        
        # Read current content
        with open(control_dict_path, 'r') as f:
            content = f.read()
        
        # Update parameters
        for key, value in updates.items():
            # Simple regex replacement - assumes parameter exists
            import re
            pattern = rf"({key}\s+)[^;]+;"
            replacement = rf"\1{value};"
            content = re.sub(pattern, replacement, content)
        
        # Write back
        with open(control_dict_path, 'w') as f:
            f.write(content)
    
    def _run_serial_simulation(self, case_dir: Path, solver: str, 
                              parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Run serial OpenFOAM simulation."""
        logger.info(f"Running serial simulation with {solver}")
        
        # Run mesh generation if needed
        if parameters.get("generate_mesh", True):
            self._run_command(["blockMesh"], case_dir)
        
        # Run solver
        start_time = time.time()
        result = run_solver(case_dir, solver, parallel=False)
        execution_time = time.time() - start_time
        
        return {
            "convergence": result.get("convergence", {}),
            "execution_time": execution_time
        }
    
    def _run_parallel_simulation(self, case_dir: Path, solver: str, 
                                num_processors: int, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Run parallel OpenFOAM simulation."""
        logger.info(f"Running parallel simulation with {solver} on {num_processors} processors")
        
        # Run mesh generation if needed
        if parameters.get("generate_mesh", True):
            self._run_command(["blockMesh"], case_dir)
        
        # Decompose case for parallel execution
        decompose_case(case_dir, num_processors)
        
        # Run solver in parallel
        start_time = time.time()
        result = run_solver(case_dir, solver, parallel=True, num_processors=num_processors)
        execution_time = time.time() - start_time
        
        # Reconstruct case
        reconstruct_case(case_dir)
        
        return {
            "convergence": result.get("convergence", {}),
            "execution_time": execution_time
        }
    
    def _run_command(self, cmd: List[str], cwd: Path) -> Tuple[int, str, str]:
        """Run a shell command and return exit code, stdout, stderr."""
        logger.info(f"Running command: {' '.join(cmd)} in {cwd}")
        
        process = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            logger.error(f"Command failed: {stderr}")
            raise RuntimeError(f"Command {cmd[0]} failed: {stderr}")
        
        return process.returncode, stdout, stderr


if __name__ == "__main__":
    import time
    
    # Create and run worker
    worker = OpenFOAMWorker()
    
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    finally:
        worker.cleanup() 