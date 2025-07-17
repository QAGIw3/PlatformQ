"""FreeCAD processor with distributed structural analysis capabilities."""
import os
import sys
import json
import logging
import tempfile
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

# Add FreeCAD to Python path if available
try:
    sys.path.append('/usr/lib/freecad/lib')
    import FreeCAD
    import Part
    FREECAD_AVAILABLE = True
except ImportError:
    FREECAD_AVAILABLE = False
    FreeCAD = None
    Part = None

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from platformq.shared.compute_orchestrator import (
    ComputeOrchestrator, 
    ComputeJob, 
    JobStatus
)
from processing.spark.processor_base import ProcessorBase

logger = logging.getLogger(__name__)


@dataclass
class FreeCADProcessingConfig:
    """Configuration for FreeCAD processing."""
    mode: str = "metadata"  # "metadata" or "fem_analysis"
    analysis_type: Optional[str] = None
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


class FreeCADProcessor(ProcessorBase):
    """Processor for FreeCAD files with distributed structural analysis."""
    
    def __init__(self, tenant_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__("FreeCADProcessor", tenant_id, config)
        self.orchestrator = ComputeOrchestrator("freecad-processor")
        
        # Register job completion handler
        self.orchestrator.register_job_handler("freecad", self._handle_job_completion)
    
    def validate_config(self, processing_config: Dict[str, Any]) -> bool:
        """Validate processing configuration."""
        config = FreeCADProcessingConfig(**processing_config)
        
        if config.mode not in ["metadata", "fem_analysis"]:
            raise ValueError(f"Invalid mode: {config.mode}")
        
        if config.mode == "fem_analysis":
            valid_analyses = [
                "static_structural", "modal", "thermal", "buckling",
                "harmonic", "transient", "fatigue", "topology_optimization"
            ]
            
            if config.analysis_type not in valid_analyses:
                raise ValueError(f"Invalid analysis type: {config.analysis_type}")
        
        return True
    
    def process(self, asset_uri: str, processing_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process FreeCAD file."""
        config = FreeCADProcessingConfig(**processing_config)
        
        if config.mode == "metadata":
            return self._extract_metadata(asset_uri)
        else:
            return self._submit_fem_analysis(asset_uri, config)
    
    def _extract_metadata(self, asset_uri: str) -> Dict[str, Any]:
        """Extract metadata from FreeCAD file."""
        if not FREECAD_AVAILABLE:
            return {"error": "FreeCAD not available in processing environment"}
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download FreeCAD file
            local_file = Path(temp_dir) / "model.fcstd"
            self.download_from_s3(asset_uri, str(local_file))
            
            # Open in FreeCAD
            doc = FreeCAD.open(str(local_file))
            
            metadata = {
                "document": {
                    "name": doc.Name,
                    "label": doc.Label,
                    "filename": doc.FileName,
                    "created_by": doc.CreatedBy if hasattr(doc, 'CreatedBy') else None,
                    "creation_date": doc.CreationDate if hasattr(doc, 'CreationDate') else None,
                    "last_modified_by": doc.LastModifiedBy if hasattr(doc, 'LastModifiedBy') else None,
                    "last_modified_date": doc.LastModifiedDate if hasattr(doc, 'LastModifiedDate') else None
                },
                "objects": [],
                "summary": {
                    "total_objects": 0,
                    "total_volume": 0,
                    "total_mass": 0,
                    "total_area": 0,
                    "object_types": {}
                }
            }
            
            # Analyze objects
            for obj in doc.Objects:
                obj_data = {
                    "name": obj.Name,
                    "label": obj.Label,
                    "type": obj.TypeId
                }
                
                # Count object types
                obj_type = obj.TypeId.split('::')[-1]
                metadata["summary"]["object_types"][obj_type] = \
                    metadata["summary"]["object_types"].get(obj_type, 0) + 1
                
                # Extract shape properties
                if hasattr(obj, "Shape") and isinstance(obj.Shape, Part.Shape):
                    shape = obj.Shape
                    
                    # Geometry properties
                    obj_data["geometry"] = {
                        "volume": shape.Volume,
                        "mass": shape.Mass,
                        "area": shape.Area,
                        "is_solid": shape.isClosed(),
                        "is_valid": shape.isValid(),
                        "shape_type": shape.ShapeType
                    }
                    
                    # Center of mass
                    com = shape.CenterOfMass
                    obj_data["geometry"]["center_of_mass"] = {
                        "x": com.x, "y": com.y, "z": com.z
                    }
                    
                    # Bounding box
                    bbox = shape.BoundBox
                    obj_data["geometry"]["bounding_box"] = {
                        "x_min": bbox.XMin, "x_max": bbox.XMax,
                        "y_min": bbox.YMin, "y_max": bbox.YMax,
                        "z_min": bbox.ZMin, "z_max": bbox.ZMax,
                        "diagonal": bbox.DiagonalLength
                    }
                    
                    # Principal properties
                    if shape.Volume > 0:
                        try:
                            moments = shape.MatrixOfInertia
                            obj_data["geometry"]["inertia_matrix"] = [
                                [moments.A11, moments.A12, moments.A13],
                                [moments.A21, moments.A22, moments.A23],
                                [moments.A31, moments.A32, moments.A33]
                            ]
                        except:
                            pass
                    
                    # Update summary
                    metadata["summary"]["total_volume"] += shape.Volume
                    metadata["summary"]["total_mass"] += shape.Mass
                    metadata["summary"]["total_area"] += shape.Area
                
                # Material properties if available
                if hasattr(obj, "Material") and obj.Material:
                    obj_data["material"] = {
                        "name": obj.Material.get("Name", "Unknown"),
                        "density": obj.Material.get("Density", None),
                        "youngs_modulus": obj.Material.get("YoungsModulus", None),
                        "poisson_ratio": obj.Material.get("PoissonRatio", None)
                    }
                
                metadata["objects"].append(obj_data)
            
            metadata["summary"]["total_objects"] = len(doc.Objects)
            
            # Close document
            FreeCAD.closeDocument(doc.Name)
            
            return metadata
    
    def _submit_fem_analysis(self, model_uri: str, 
                           config: FreeCADProcessingConfig) -> Dict[str, Any]:
        """Submit FreeCAD FEM analysis job."""
        # Create compute job
        job = ComputeJob(
            job_id="",
            job_type="freecad",
            input_data={
                "model_file": model_uri,
                "analysis_type": config.analysis_type,
                "parameters": config.parameters
            },
            output_location=f"output-data/freecad/{self.tenant_id}",
            parameters={}
        )
        
        # Submit job
        job_id = self.orchestrator.submit_job(job)
        
        return {
            "status": "submitted",
            "job_id": job_id,
            "analysis_type": config.analysis_type,
            "message": f"FreeCAD {config.analysis_type} analysis submitted"
        }
    
    def _handle_job_completion(self, job: ComputeJob):
        """Handle completed FreeCAD analysis job."""
        logger.info(f"FreeCAD job {job.job_id} completed with status {job.status}")
        
        if job.status == JobStatus.COMPLETED:
            # Store analysis results
            if job.result and "results_summary" in job.result:
                self._store_analysis_results(job.job_id, job.result["results_summary"])
    
    def _store_analysis_results(self, job_id: str, results_summary: Dict[str, Any]):
        """Store analysis results for retrieval."""
        # Store in Ignite cache
        results_cache = self.orchestrator.ignite_client.get_or_create_cache(
            "freecad_results"
        )
        results_cache.put(job_id, results_summary)
    
    def submit_optimization_study(self, model_uri: str, 
                                 optimization_config: Dict[str, Any]) -> Dict[str, Any]:
        """Submit topology optimization study."""
        # Configure optimization parameters
        parameters = {
            "volume_fraction": optimization_config.get("target_volume_fraction", 0.3),
            "filter_radius": optimization_config.get("filter_radius", 2.0),
            "penalty": optimization_config.get("penalty", 3.0),
            "max_iterations": optimization_config.get("max_iterations", 50),
            "optimization_goal": optimization_config.get("goal", "minimize_compliance"),
            "constraints": optimization_config.get("constraints", []),
            "loads": optimization_config.get("loads", []),
            "boundary_conditions": optimization_config.get("boundary_conditions", []),
            "materials": optimization_config.get("materials", [])
        }
        
        config = FreeCADProcessingConfig(
            mode="fem_analysis",
            analysis_type="topology_optimization",
            parameters=parameters
        )
        
        return self._submit_fem_analysis(model_uri, config)
    
    def submit_fatigue_analysis(self, model_uri: str,
                               loading_history: List[Dict[str, Any]],
                               material_data: Dict[str, Any]) -> Dict[str, Any]:
        """Submit fatigue analysis with loading history."""
        parameters = {
            "loading_history": loading_history,
            "material_fatigue_data": material_data,
            "analysis_method": "rainflow",
            "mean_stress_correction": "goodman",
            "safety_factor": 2.0
        }
        
        config = FreeCADProcessingConfig(
            mode="fem_analysis",
            analysis_type="fatigue",
            parameters=parameters
        )
        
        return self._submit_fem_analysis(model_uri, config)
    
    def submit_coupled_analysis(self, model_uri: str,
                              coupling_config: Dict[str, Any]) -> Dict[str, Any]:
        """Submit multi-physics coupled analysis."""
        # This could couple thermal-structural, fluid-structure, etc.
        analysis_sequence = []
        
        if "thermal_loads" in coupling_config:
            # First run thermal analysis
            thermal_job = self._submit_fem_analysis(
                model_uri,
                FreeCADProcessingConfig(
                    mode="fem_analysis",
                    analysis_type="thermal",
                    parameters=coupling_config["thermal_loads"]
                )
            )
            analysis_sequence.append(thermal_job)
        
        # Then run structural with thermal results
        structural_params = coupling_config.get("structural", {})
        structural_params["thermal_results"] = "${thermal_job.results}"
        
        structural_job = self._submit_fem_analysis(
            model_uri,
            FreeCADProcessingConfig(
                mode="fem_analysis",
                analysis_type="static_structural",
                parameters=structural_params
            )
        )
        analysis_sequence.append(structural_job)
        
        return {
            "analysis_type": "coupled",
            "sequence": analysis_sequence,
            "coupling_type": coupling_config.get("type", "thermal-structural")
        }
    
    def get_analysis_results(self, job_id: str) -> Dict[str, Any]:
        """Get detailed FEM analysis results."""
        job = self.orchestrator.get_job_status(job_id)
        
        if not job:
            return {"error": "Job not found"}
        
        results = {
            "job_id": job_id,
            "status": job.status.value,
            "analysis_type": job.input_data.get("analysis_type")
        }
        
        if job.status == JobStatus.COMPLETED and job.result:
            results.update(job.result)
            
            # Get detailed results from cache
            results_cache = self.orchestrator.ignite_client.get_or_create_cache(
                "freecad_results"
            )
            detailed_results = results_cache.get(job_id)
            if detailed_results:
                results["detailed_results"] = detailed_results
        
        return results


# Backward compatibility
def extract_freecad_metadata(filepath: str) -> Dict[str, Any]:
    """Legacy function for metadata extraction."""
    if not FREECAD_AVAILABLE:
        return {"error": "FreeCAD not available"}
    
    processor = FreeCADProcessor("default")
    return processor._extract_metadata(f"file://{filepath}")


if __name__ == '__main__':
    # Example usage
    processor = FreeCADProcessor("test-tenant")
    
    # Metadata extraction
    metadata_config = {
        "mode": "metadata"
    }
    
    # Static structural analysis
    static_config = {
        "mode": "fem_analysis",
        "analysis_type": "static_structural",
        "parameters": {
            "materials": [
                {
                    "name": "Steel",
                    "youngs_modulus": 210000,
                    "poisson_ratio": 0.3,
                    "density": 7850,
                    "assign_to": ["Part001"]
                }
            ],
            "boundary_conditions": [
                {
                    "type": "fixed",
                    "name": "FixedSupport",
                    "apply_to": [{"object": "Part001", "type": "face", "index": 0}]
                },
                {
                    "type": "force", 
                    "name": "Load",
                    "magnitude": 10000,
                    "direction": [0, 0, -1],
                    "apply_to": [{"object": "Part001", "type": "face", "index": 5}]
                }
            ],
            "mesh_settings": {
                "max_element_size": 10.0,
                "min_element_size": 1.0,
                "element_order": 2
            }
        }
    }
    
    # Modal analysis
    modal_config = {
        "mode": "fem_analysis", 
        "analysis_type": "modal",
        "parameters": {
            "num_modes": 10,
            "frequency_range": [0, 1000]
        }
    } 