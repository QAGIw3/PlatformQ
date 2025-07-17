"""FreeCAD worker for distributed structural analysis."""
import os
import sys
import json
import time
import subprocess
import tempfile
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

# Add FreeCAD to Python path
sys.path.append('/usr/lib/freecad/lib')

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import FreeCAD
import Part
import Fem
import ObjectsFem
from FreeCAD import Base

from base.worker_base import ComputeWorker
from freecad_utils import (
    parse_analysis_config,
    setup_material_properties,
    generate_mesh,
    apply_boundary_conditions,
    run_fem_analysis,
    extract_analysis_results
)

logger = logging.getLogger(__name__)


class FreeCADWorker(ComputeWorker):
    """Worker for distributed FreeCAD structural analysis."""
    
    def __init__(self):
        super().__init__("freecad")
        self.supported_analyses = [
            "static_structural", "modal", "thermal", "buckling",
            "harmonic", "transient", "fatigue", "topology_optimization"
        ]
        
        # Initialize FreeCAD
        FreeCAD.newDocument("temp")
    
    def validate_job(self, job_data: Dict[str, Any]) -> bool:
        """Validate FreeCAD analysis job."""
        required_fields = ["model_file", "analysis_type", "parameters"]
        
        for field in required_fields:
            if field not in job_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate analysis type
        if job_data["analysis_type"] not in self.supported_analyses:
            raise ValueError(f"Unsupported analysis type: {job_data['analysis_type']}")
        
        # Validate model file extension
        model_ext = Path(job_data["model_file"]).suffix.lower()
        valid_extensions = ['.fcstd', '.step', '.stp', '.iges', '.igs', '.brep']
        if model_ext not in valid_extensions:
            raise ValueError(f"Unsupported model format: {model_ext}")
        
        return True
    
    def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a FreeCAD structural analysis job."""
        job_id = job_data["job_id"]
        model_file = job_data["model_file"]
        analysis_type = job_data["analysis_type"]
        parameters = job_data["parameters"]
        
        # Create temporary working directory
        with tempfile.TemporaryDirectory() as work_dir:
            work_path = Path(work_dir)
            
            try:
                # Download model file
                logger.info(f"Downloading model from {model_file}")
                local_model = work_path / "model.fcstd"
                self.download_input_data(model_file, str(local_model))
                
                # Load model in FreeCAD
                doc = FreeCAD.open(str(local_model))
                
                # Setup analysis
                analysis_config = self._setup_analysis(
                    doc, analysis_type, parameters
                )
                
                # Generate mesh
                mesh_info = generate_mesh(
                    doc,
                    parameters.get("mesh_settings", {})
                )
                
                # Apply materials
                if "materials" in parameters:
                    setup_material_properties(doc, parameters["materials"])
                
                # Apply boundary conditions
                if "boundary_conditions" in parameters:
                    apply_boundary_conditions(
                        doc, 
                        parameters["boundary_conditions"]
                    )
                
                # Run analysis
                start_time = time.time()
                analysis_results = run_fem_analysis(
                    doc,
                    analysis_type,
                    analysis_config,
                    work_path
                )
                analysis_time = time.time() - start_time
                
                # Extract results
                results_data = extract_analysis_results(
                    doc,
                    analysis_type,
                    analysis_results,
                    parameters.get("output_requests", [])
                )
                
                # Save results
                results_file = work_path / "results.json"
                with open(results_file, 'w') as f:
                    json.dump(results_data, f, indent=2)
                
                # Upload results
                output_path = job_data.get("output_location", f"output-data/{job_id}")
                self._upload_results(work_path, output_path)
                
                # Close document
                FreeCAD.closeDocument(doc.Name)
                
                return {
                    "status": "completed",
                    "analysis_type": analysis_type,
                    "mesh_info": mesh_info,
                    "analysis_time": analysis_time,
                    "results_summary": results_data.get("summary", {}),
                    "output_path": output_path
                }
                
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                raise
    
    def _setup_analysis(self, doc, analysis_type: str, 
                       parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Setup FreeCAD analysis based on type."""
        config = {
            "analysis_type": analysis_type,
            "solver": parameters.get("solver", "CalculiX"),
            "settings": {}
        }
        
        if analysis_type == "static_structural":
            config["settings"] = {
                "geometrical_nonlinearity": parameters.get("nonlinear", False),
                "iterations": parameters.get("max_iterations", 100),
                "tolerance": parameters.get("convergence_tolerance", 1e-6)
            }
            
        elif analysis_type == "modal":
            config["settings"] = {
                "number_of_modes": parameters.get("num_modes", 10),
                "frequency_range": parameters.get("frequency_range", [0, 1000]),
                "normalize_modes": parameters.get("normalize", True)
            }
            
        elif analysis_type == "thermal":
            config["settings"] = {
                "steady_state": parameters.get("steady_state", True),
                "initial_temperature": parameters.get("initial_temp", 20),
                "time_step": parameters.get("time_step", 1.0),
                "total_time": parameters.get("total_time", 100)
            }
            
        elif analysis_type == "buckling":
            config["settings"] = {
                "buckling_factors": parameters.get("num_factors", 5),
                "preload_analysis": parameters.get("preload", True)
            }
            
        elif analysis_type == "topology_optimization":
            config["settings"] = {
                "volume_fraction": parameters.get("volume_fraction", 0.3),
                "filter_radius": parameters.get("filter_radius", 2.0),
                "penalty": parameters.get("penalty", 3.0),
                "iterations": parameters.get("max_iterations", 50),
                "optimization_goal": parameters.get("goal", "minimize_compliance")
            }
        
        return config
    
    def _create_fem_analysis(self, doc) -> Any:
        """Create FEM analysis container in FreeCAD."""
        # Create analysis container
        analysis = ObjectsFem.makeAnalysis(doc, 'Analysis')
        
        # Add solver
        solver = ObjectsFem.makeSolverCalculixCcx(doc, 'CalculiX')
        analysis.addObject(solver)
        
        # Find solid objects
        for obj in doc.Objects:
            if hasattr(obj, 'Shape') and obj.Shape.Solids:
                analysis.addObject(obj)
        
        return analysis
    
    def _run_distributed_mesh_generation(self, doc, mesh_settings: Dict[str, Any],
                                       num_workers: int = 4) -> Dict[str, Any]:
        """Run distributed mesh generation for large models."""
        # Split model into regions
        regions = self._split_model_regions(doc, num_workers)
        
        # Generate mesh for each region in parallel
        mesh_jobs = []
        for i, region in enumerate(regions):
            job = {
                "region_id": i,
                "bounds": region["bounds"],
                "settings": mesh_settings
            }
            mesh_jobs.append(job)
        
        # This would submit to other workers in a real implementation
        # For now, we'll process locally
        mesh_results = []
        for job in mesh_jobs:
            result = self._generate_region_mesh(doc, job)
            mesh_results.append(result)
        
        # Merge meshes
        final_mesh = self._merge_meshes(mesh_results)
        
        return {
            "total_nodes": final_mesh["nodes"],
            "total_elements": final_mesh["elements"],
            "regions_processed": len(regions)
        }
    
    def _split_model_regions(self, doc, num_regions: int) -> List[Dict[str, Any]]:
        """Split model into regions for distributed processing."""
        regions = []
        
        # Get overall bounding box
        bbox = self._get_model_bbox(doc)
        
        # Simple splitting along X-axis
        x_range = bbox["x_max"] - bbox["x_min"]
        region_width = x_range / num_regions
        
        for i in range(num_regions):
            region = {
                "id": i,
                "bounds": {
                    "x_min": bbox["x_min"] + i * region_width,
                    "x_max": bbox["x_min"] + (i + 1) * region_width,
                    "y_min": bbox["y_min"],
                    "y_max": bbox["y_max"],
                    "z_min": bbox["z_min"],
                    "z_max": bbox["z_max"]
                }
            }
            regions.append(region)
        
        return regions
    
    def _get_model_bbox(self, doc) -> Dict[str, float]:
        """Get model bounding box."""
        bbox = None
        
        for obj in doc.Objects:
            if hasattr(obj, 'Shape') and obj.Shape.Solids:
                if bbox is None:
                    bbox = obj.Shape.BoundBox
                else:
                    bbox.add(obj.Shape.BoundBox)
        
        if bbox:
            return {
                "x_min": bbox.XMin, "x_max": bbox.XMax,
                "y_min": bbox.YMin, "y_max": bbox.YMax,
                "z_min": bbox.ZMin, "z_max": bbox.ZMax
            }
        
        return {"x_min": 0, "x_max": 0, "y_min": 0, "y_max": 0, "z_min": 0, "z_max": 0}
    
    def _generate_region_mesh(self, doc, region_job: Dict[str, Any]) -> Dict[str, Any]:
        """Generate mesh for a specific region."""
        # This is a simplified implementation
        # In reality, would use gmsh or netgen with region constraints
        
        return {
            "region_id": region_job["region_id"],
            "nodes": 10000,  # Placeholder
            "elements": 50000  # Placeholder
        }
    
    def _merge_meshes(self, mesh_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Merge meshes from different regions."""
        total_nodes = sum(m["nodes"] for m in mesh_results)
        total_elements = sum(m["elements"] for m in mesh_results)
        
        return {
            "nodes": total_nodes,
            "elements": total_elements
        }
    
    def _upload_results(self, work_dir: Path, output_path: str):
        """Upload analysis results to MinIO."""
        bucket, prefix = output_path.split('/', 1)
        
        # Upload all result files
        for file_path in work_dir.glob('*'):
            if file_path.is_file():
                object_name = f"{prefix}/{file_path.name}"
                self.minio_client.fput_object(bucket, object_name, str(file_path))
                logger.info(f"Uploaded {file_path.name}")
        
        # Also upload VTK files if present (for visualization)
        vtk_dir = work_dir / "vtk_output"
        if vtk_dir.exists():
            for vtk_file in vtk_dir.glob('*.vtk'):
                object_name = f"{prefix}/vtk/{vtk_file.name}"
                self.minio_client.fput_object(bucket, object_name, str(vtk_file))


if __name__ == "__main__":
    # Create and run worker
    worker = FreeCADWorker()
    
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    finally:
        worker.cleanup() 