"""
OpenFOAM processor for CFD simulation processing
"""

import logging
import json
from typing import Dict, Any, List
import os

from ..base import BaseFileProcessor

logger = logging.getLogger(__name__)


class OpenFOAMProcessor(BaseFileProcessor):
    """
    Processor for OpenFOAM computational fluid dynamics simulations
    """
    
    @property
    def processor_type(self) -> str:
        return "openfoam"
    
    @property
    def supported_formats(self) -> List[str]:
        # OpenFOAM case directories and specific file types
        return ["foam", "openfoam", "stl", "msh", "cas", "dat"]
    
    @property
    def spark_config(self) -> Dict[str, Any]:
        """Enhanced Spark config for HPC simulation"""
        config = super().spark_config
        config.update({
            "spark.executor.memory": "16g",
            "spark.executor.cores": "8",
            "spark.dynamicAllocation.maxExecutors": "20",
            "spark.network.timeout": "600s",
            "spark.executor.heartbeatInterval": "60s"
        })
        return config
    
    async def validate_input(self, file_path: str) -> bool:
        """Validate OpenFOAM case or mesh file"""
        try:
            # Check if it's a case directory
            if os.path.isdir(file_path):
                # Check for essential OpenFOAM directories
                required_dirs = ["0", "constant", "system"]
                for dir_name in required_dirs:
                    if not os.path.exists(os.path.join(file_path, dir_name)):
                        return False
                return True
            
            # Check if it's a supported mesh/geometry file
            if self.supports_file(file_path) and os.path.exists(file_path):
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error validating OpenFOAM input: {e}")
            return False
    
    async def extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from OpenFOAM case"""
        metadata = {
            "file_path": file_path,
            "processor": self.processor_type
        }
        
        if os.path.isdir(file_path):
            metadata["type"] = "case_directory"
            
            # Extract case information
            control_dict = os.path.join(file_path, "system/controlDict")
            if os.path.exists(control_dict):
                # Would parse controlDict for simulation parameters
                metadata["has_control_dict"] = True
            
            # Check for mesh
            if os.path.exists(os.path.join(file_path, "constant/polyMesh")):
                metadata["has_mesh"] = True
        else:
            metadata["type"] = "mesh_file"
            metadata["file_size"] = os.path.getsize(file_path)
            metadata["file_format"] = os.path.splitext(file_path)[1].lower().lstrip('.')
        
        return metadata
    
    def get_spark_job_script(self) -> str:
        """Get the Spark job script for OpenFOAM processing"""
        return "/opt/spark-jobs/openfoam_distributed_simulation.py"


# Spark job script content (would be deployed separately)
OPENFOAM_SPARK_JOB = '''
"""
Distributed OpenFOAM simulation using Spark
"""

from pyspark.sql import SparkSession
import json
import subprocess
import os
import shutil
import tempfile

def setup_case_partition(case_data):
    """Set up OpenFOAM case for a partition"""
    case_dir = case_data["case_dir"]
    partition_id = case_data["partition_id"]
    num_partitions = case_data["num_partitions"]
    
    # Create local copy of case
    local_case = f"/tmp/openfoam_{partition_id}"
    shutil.copytree(case_dir, local_case)
    
    # Decompose the domain for this partition
    decompose_dict = f"""
    FoamFile
    {{
        version     2.0;
        format      ascii;
        class       dictionary;
        object      decomposeParDict;
    }}
    
    numberOfSubdomains {num_partitions};
    method          scotch;
    
    distributed     yes;
    roots           
    (
        {" ".join([f'"/tmp/openfoam_{i}"' for i in range(num_partitions)])}
    );
    """
    
    with open(os.path.join(local_case, "system/decomposeParDict"), "w") as f:
        f.write(decompose_dict)
    
    return local_case

def run_simulation_partition(partition_data):
    """Run OpenFOAM simulation on a partition"""
    local_case = partition_data["local_case"]
    partition_id = partition_data["partition_id"]
    solver = partition_data.get("solver", "simpleFoam")
    
    results = {
        "partition_id": partition_id,
        "status": "started"
    }
    
    try:
        # Decompose mesh if needed
        if partition_data.get("decompose", True):
            decompose_cmd = ["decomposePar", "-case", local_case, "-force"]
            result = subprocess.run(decompose_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                results["status"] = "decompose_failed"
                results["error"] = result.stderr
                return results
        
        # Run the solver
        solver_cmd = [
            "mpirun",
            "-np", str(partition_data["cores_per_partition"]),
            solver,
            "-case", local_case,
            "-parallel"
        ]
        
        result = subprocess.run(solver_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            results["status"] = "completed"
            results["output"] = result.stdout
            
            # Extract convergence data
            results["convergence"] = extract_convergence_data(result.stdout)
        else:
            results["status"] = "solver_failed"
            results["error"] = result.stderr
            
    except Exception as e:
        results["status"] = "error"
        results["error"] = str(e)
    
    # Clean up if needed
    if partition_data.get("cleanup", True):
        shutil.rmtree(local_case, ignore_errors=True)
    
    return results

def run_mesh_operation(mesh_data):
    """Run mesh generation or conversion operations"""
    operation = mesh_data["operation"]
    input_file = mesh_data["input_file"]
    output_path = mesh_data["output_path"]
    
    results = {
        "operation": operation,
        "input": input_file
    }
    
    try:
        if operation == "snappyHexMesh":
            # Run snappyHexMesh for complex geometry meshing
            case_dir = mesh_data["case_dir"]
            cmd = ["snappyHexMesh", "-case", case_dir, "-overwrite"]
            
        elif operation == "blockMesh":
            # Run blockMesh for structured meshing
            case_dir = mesh_data["case_dir"]
            cmd = ["blockMesh", "-case", case_dir]
            
        elif operation == "convert":
            # Convert mesh formats
            output_file = os.path.join(output_path, "converted.foam")
            if input_file.endswith(".msh"):
                cmd = ["gmshToFoam", input_file, "-case", output_path]
            elif input_file.endswith(".cas"):
                cmd = ["fluent3DMeshToFoam", input_file, "-case", output_path]
            else:
                results["status"] = "unsupported_format"
                return results
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            results["status"] = "success"
            results["output"] = result.stdout
        else:
            results["status"] = "failed"
            results["error"] = result.stderr
            
    except Exception as e:
        results["status"] = "error"
        results["error"] = str(e)
    
    return results

def extract_convergence_data(solver_output):
    """Extract convergence data from solver output"""
    convergence = {
        "iterations": [],
        "residuals": {}
    }
    
    # Parse solver output for residuals
    lines = solver_output.split('\\n')
    for line in lines:
        if "Solving for" in line:
            # Extract variable and residual
            parts = line.split()
            if len(parts) > 7 and "Final" in line:
                var = parts[2].rstrip(',')
                residual = float(parts[7].rstrip(','))
                if var not in convergence["residuals"]:
                    convergence["residuals"][var] = []
                convergence["residuals"][var].append(residual)
    
    return convergence

def main():
    spark = SparkSession.builder \\
        .appName("OpenFOAMDistributedSimulation") \\
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # Load job configuration
    config = json.loads(sc.getConf().get("spark.job.config"))
    
    job_type = config.get("job_type", "simulation")
    
    if job_type == "simulation":
        # Distributed simulation
        case_dir = config["input_path"]
        num_partitions = config.get("num_partitions", 4)
        cores_per_partition = config.get("cores_per_partition", 4)
        
        # Create partition configurations
        partitions = []
        for i in range(num_partitions):
            partition_data = {
                "case_dir": case_dir,
                "partition_id": i,
                "num_partitions": num_partitions,
                "cores_per_partition": cores_per_partition,
                "solver": config.get("solver", "simpleFoam"),
                "decompose": i == 0,  # Only decompose once
                "cleanup": config.get("cleanup", True)
            }
            
            # Set up case for partition
            local_case = setup_case_partition(partition_data)
            partition_data["local_case"] = local_case
            
            partitions.append(partition_data)
        
        # Run simulations in parallel
        partitions_rdd = sc.parallelize(partitions, num_partitions)
        results = partitions_rdd.map(run_simulation_partition).collect()
        
    elif job_type == "mesh":
        # Mesh generation/conversion
        mesh_operations = config.get("mesh_operations", [])
        
        mesh_rdd = sc.parallelize(mesh_operations)
        results = mesh_rdd.map(run_mesh_operation).collect()
    
    else:
        results = [{"error": f"Unknown job type: {job_type}"}]
    
    # Save results
    output_path = config["output_path"]
    results_path = os.path.join(output_path, "openfoam_results.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    
    spark.stop()

if __name__ == "__main__":
    main()
''' 