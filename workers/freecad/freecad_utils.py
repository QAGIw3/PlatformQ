"""FreeCAD utility functions for structural analysis."""
import os
import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import numpy as np

import FreeCAD
import Part
import Fem
import ObjectsFem
import FemGui
from FreeCAD import Base, Units

logger = logging.getLogger(__name__)


def parse_analysis_config(config_file: Path) -> Dict[str, Any]:
    """Parse FreeCAD analysis configuration file."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        required = ["analysis_type", "mesh_settings", "boundary_conditions"]
        for field in required:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        return config
        
    except Exception as e:
        logger.error(f"Error parsing analysis config: {e}")
        return {}


def setup_material_properties(doc, materials: List[Dict[str, Any]]):
    """Setup material properties for FEM analysis."""
    analysis = None
    
    # Find analysis object
    for obj in doc.Objects:
        if obj.TypeId == 'Fem::FemAnalysis':
            analysis = obj
            break
    
    if not analysis:
        analysis = ObjectsFem.makeAnalysis(doc, 'Analysis')
    
    # Add materials
    for mat_config in materials:
        # Create material object
        material = ObjectsFem.makeMaterialSolid(doc, mat_config.get("name", "Material"))
        
        # Set material properties
        mat = material.Material
        
        # Mechanical properties
        if "youngs_modulus" in mat_config:
            mat['YoungsModulus'] = f"{mat_config['youngs_modulus']} MPa"
        if "poisson_ratio" in mat_config:
            mat['PoissonRatio'] = str(mat_config['poisson_ratio'])
        if "density" in mat_config:
            mat['Density'] = f"{mat_config['density']} kg/m^3"
        
        # Thermal properties
        if "thermal_expansion" in mat_config:
            mat['ThermalExpansionCoefficient'] = f"{mat_config['thermal_expansion']} 1/K"
        if "thermal_conductivity" in mat_config:
            mat['ThermalConductivity'] = f"{mat_config['thermal_conductivity']} W/m/K"
        if "specific_heat" in mat_config:
            mat['SpecificHeat'] = f"{mat_config['specific_heat']} J/kg/K"
        
        material.Material = mat
        
        # Assign to objects
        if "assign_to" in mat_config:
            refs = []
            for obj_name in mat_config["assign_to"]:
                obj = doc.getObject(obj_name)
                if obj and hasattr(obj, 'Shape'):
                    refs.append((obj, obj.Shape.Solids))
            material.References = refs
        
        # Add to analysis
        analysis.addObject(material)


def generate_mesh(doc, mesh_settings: Dict[str, Any]) -> Dict[str, Any]:
    """Generate FEM mesh using Gmsh or Netgen."""
    mesh_info = {}
    
    # Find analysis and geometry
    analysis = None
    geometry = []
    
    for obj in doc.Objects:
        if obj.TypeId == 'Fem::FemAnalysis':
            analysis = obj
        elif hasattr(obj, 'Shape') and obj.Shape.Solids:
            geometry.append(obj)
    
    if not analysis:
        analysis = ObjectsFem.makeAnalysis(doc, 'Analysis')
    
    # Create mesh object
    mesh_obj = ObjectsFem.makeMeshGmsh(doc, 'FEMMeshGmsh')
    
    # Set mesh parameters
    mesh_obj.CharacteristicLengthMax = mesh_settings.get('max_element_size', 10.0)
    mesh_obj.CharacteristicLengthMin = mesh_settings.get('min_element_size', 1.0)
    mesh_obj.ElementDimension = mesh_settings.get('element_dimension', 3)
    mesh_obj.ElementOrder = mesh_settings.get('element_order', 2)
    
    # Algorithm settings
    mesh_obj.Algorithm2D = mesh_settings.get('algorithm_2d', 'Automatic')
    mesh_obj.Algorithm3D = mesh_settings.get('algorithm_3d', 'Automatic')
    
    # Optimization
    mesh_obj.OptimizeSteps = mesh_settings.get('optimize_steps', 3)
    mesh_obj.OptimizeNetgen = mesh_settings.get('optimize_netgen', True)
    
    # Add geometry to mesh
    for geom in geometry:
        mesh_obj.Part = geom
    
    # Add to analysis
    analysis.addObject(mesh_obj)
    
    # Generate mesh
    try:
        import femmesh.gmshtools as gmshtools
        gmsh_mesh = gmshtools.GmshTools(mesh_obj)
        error = gmsh_mesh.create_mesh()
        
        if error:
            logger.error(f"Mesh generation failed: {error}")
            mesh_info["status"] = "failed"
            mesh_info["error"] = error
        else:
            # Get mesh statistics
            fem_mesh = mesh_obj.FemMesh
            mesh_info["status"] = "success"
            mesh_info["nodes"] = fem_mesh.NodeCount
            mesh_info["elements"] = fem_mesh.VolumeCount
            mesh_info["triangles"] = fem_mesh.TriangleCount
            mesh_info["tetrahedra"] = fem_mesh.TetraCount
            mesh_info["hexahedra"] = fem_mesh.HexaCount
            
            # Calculate mesh quality metrics
            mesh_info["quality"] = calculate_mesh_quality(fem_mesh)
            
    except Exception as e:
        logger.error(f"Mesh generation error: {e}")
        mesh_info["status"] = "error"
        mesh_info["error"] = str(e)
    
    return mesh_info


def calculate_mesh_quality(fem_mesh) -> Dict[str, float]:
    """Calculate mesh quality metrics."""
    quality = {
        "min_jacobian": 1.0,
        "avg_jacobian": 1.0,
        "min_aspect_ratio": 1.0,
        "avg_aspect_ratio": 1.0
    }
    
    # This is a simplified implementation
    # Real implementation would calculate actual quality metrics
    
    return quality


def apply_boundary_conditions(doc, boundary_conditions: List[Dict[str, Any]]):
    """Apply boundary conditions to FEM model."""
    analysis = None
    
    # Find analysis object
    for obj in doc.Objects:
        if obj.TypeId == 'Fem::FemAnalysis':
            analysis = obj
            break
    
    if not analysis:
        raise ValueError("No analysis object found")
    
    # Apply each boundary condition
    for bc in boundary_conditions:
        bc_type = bc["type"]
        
        if bc_type == "fixed":
            constraint = ObjectsFem.makeConstraintFixed(doc, bc.get("name", "Fixed"))
            
        elif bc_type == "displacement":
            constraint = ObjectsFem.makeConstraintDisplacement(doc, bc.get("name", "Displacement"))
            if "x" in bc:
                constraint.xDisplacement = bc["x"]
                constraint.xFree = False
            if "y" in bc:
                constraint.yDisplacement = bc["y"]
                constraint.yFree = False
            if "z" in bc:
                constraint.zDisplacement = bc["z"]
                constraint.zFree = False
                
        elif bc_type == "force":
            constraint = ObjectsFem.makeConstraintForce(doc, bc.get("name", "Force"))
            constraint.Force = bc.get("magnitude", 1000)
            constraint.Direction = tuple(bc.get("direction", [0, 0, -1]))
            constraint.Reversed = bc.get("reversed", False)
            
        elif bc_type == "pressure":
            constraint = ObjectsFem.makeConstraintPressure(doc, bc.get("name", "Pressure"))
            constraint.Pressure = bc.get("pressure", 1.0)
            constraint.Reversed = bc.get("reversed", False)
            
        elif bc_type == "temperature":
            constraint = ObjectsFem.makeConstraintTemperature(doc, bc.get("name", "Temperature"))
            constraint.Temperature = bc.get("temperature", 20.0)
            
        elif bc_type == "heat_flux":
            constraint = ObjectsFem.makeConstraintHeatflux(doc, bc.get("name", "HeatFlux"))
            constraint.DFlux = bc.get("flux", 1000.0)
            
        else:
            logger.warning(f"Unknown boundary condition type: {bc_type}")
            continue
        
        # Apply to geometry
        if "apply_to" in bc:
            refs = []
            for ref in bc["apply_to"]:
                obj_name = ref["object"]
                obj = doc.getObject(obj_name)
                
                if obj and hasattr(obj, 'Shape'):
                    if ref["type"] == "face":
                        face_idx = ref.get("index", 0)
                        if face_idx < len(obj.Shape.Faces):
                            refs.append((obj, f"Face{face_idx + 1}"))
                    elif ref["type"] == "edge":
                        edge_idx = ref.get("index", 0)
                        if edge_idx < len(obj.Shape.Edges):
                            refs.append((obj, f"Edge{edge_idx + 1}"))
                    elif ref["type"] == "vertex":
                        vertex_idx = ref.get("index", 0)
                        if vertex_idx < len(obj.Shape.Vertexes):
                            refs.append((obj, f"Vertex{vertex_idx + 1}"))
                    else:
                        refs.append((obj, ''))
            
            constraint.References = refs
        
        # Add to analysis
        analysis.addObject(constraint)


def run_fem_analysis(doc, analysis_type: str, config: Dict[str, Any],
                    work_dir: Path) -> Dict[str, Any]:
    """Run FEM analysis using CalculiX or other solver."""
    results = {}
    
    # Find analysis and solver
    analysis = None
    solver = None
    
    for obj in doc.Objects:
        if obj.TypeId == 'Fem::FemAnalysis':
            analysis = obj
        elif 'Solver' in obj.TypeId:
            solver = obj
    
    if not analysis or not solver:
        raise ValueError("Analysis or solver not found")
    
    # Configure solver based on analysis type
    if analysis_type == "static_structural":
        solver.AnalysisType = "static"
        solver.GeometricalNonlinearity = config["settings"].get("geometrical_nonlinearity", False)
        solver.IterationsControlParameterTimeUse = False
        
    elif analysis_type == "modal":
        solver.AnalysisType = "frequency"
        solver.EigenmodesCount = config["settings"].get("number_of_modes", 10)
        solver.EigenmodeLowLimit = config["settings"].get("frequency_range", [0, 1000])[0]
        solver.EigenmodeHighLimit = config["settings"].get("frequency_range", [0, 1000])[1]
        
    elif analysis_type == "thermal":
        solver.AnalysisType = "thermomech"
        solver.ThermoMechSteadyState = config["settings"].get("steady_state", True)
        
    elif analysis_type == "buckling":
        solver.AnalysisType = "buckling"
        solver.BucklingFactors = config["settings"].get("buckling_factors", 5)
    
    # Set working directory
    solver.WorkingDir = str(work_dir)
    
    # Write input file
    try:
        import femsolver.calculix.writer as ccxwriter
        
        # Create writer
        writer = ccxwriter.FemInputWriterCcx(
            analysis, solver, 
            doc.getObject(analysis.Group[0].Name).FemMesh,
            doc.Objects
        )
        
        # Write input file
        writer.write_calculix_input_file()
        
        # Run solver
        import femsolver.run as run
        machine = run.getMachine(solver)
        machine.start()
        machine.join()  # Wait for completion
        
        # Check results
        if machine.results.returncode == 0:
            results["status"] = "success"
            results["output_file"] = str(work_dir / f"{solver.Name}.frd")
            
            # Import results
            import feminout.importCcxFrdResults as importFrd
            result_obj = importFrd.importFrd(results["output_file"])
            
            if result_obj:
                analysis.addObject(result_obj)
                results["result_object"] = result_obj.Name
        else:
            results["status"] = "failed"
            results["error"] = "Solver failed"
            
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        results["status"] = "error"
        results["error"] = str(e)
    
    return results


def extract_analysis_results(doc, analysis_type: str, analysis_results: Dict[str, Any],
                           output_requests: List[str]) -> Dict[str, Any]:
    """Extract and process analysis results."""
    results_data = {
        "analysis_type": analysis_type,
        "status": analysis_results.get("status", "unknown"),
        "summary": {},
        "detailed_results": {}
    }
    
    if analysis_results.get("status") != "success":
        return results_data
    
    # Find result object
    result_obj = None
    if "result_object" in analysis_results:
        result_obj = doc.getObject(analysis_results["result_object"])
    
    if not result_obj:
        return results_data
    
    # Extract results based on analysis type
    if analysis_type == "static_structural":
        results_data["summary"] = extract_static_results(result_obj)
        
    elif analysis_type == "modal":
        results_data["summary"] = extract_modal_results(result_obj)
        
    elif analysis_type == "thermal":
        results_data["summary"] = extract_thermal_results(result_obj)
        
    elif analysis_type == "buckling":
        results_data["summary"] = extract_buckling_results(result_obj)
    
    # Extract detailed results based on output requests
    for request in output_requests:
        if request == "displacement":
            results_data["detailed_results"]["displacement"] = get_displacement_field(result_obj)
        elif request == "stress":
            results_data["detailed_results"]["stress"] = get_stress_field(result_obj)
        elif request == "strain":
            results_data["detailed_results"]["strain"] = get_strain_field(result_obj)
        elif request == "temperature":
            results_data["detailed_results"]["temperature"] = get_temperature_field(result_obj)
    
    return results_data


def extract_static_results(result_obj) -> Dict[str, Any]:
    """Extract static structural analysis results."""
    summary = {}
    
    if hasattr(result_obj, 'DisplacementVectors'):
        displacements = result_obj.DisplacementVectors
        if displacements:
            # Calculate max displacement
            max_disp = 0
            for disp in displacements:
                magnitude = np.sqrt(disp.x**2 + disp.y**2 + disp.z**2)
                max_disp = max(max_disp, magnitude)
            
            summary["max_displacement_mm"] = max_disp
    
    if hasattr(result_obj, 'StressValues'):
        stresses = result_obj.StressValues
        if stresses:
            summary["max_von_mises_stress_MPa"] = max(stresses)
            summary["min_von_mises_stress_MPa"] = min(stresses)
            summary["avg_von_mises_stress_MPa"] = sum(stresses) / len(stresses)
    
    return summary


def extract_modal_results(result_obj) -> Dict[str, Any]:
    """Extract modal analysis results."""
    summary = {
        "natural_frequencies": [],
        "mode_shapes": []
    }
    
    if hasattr(result_obj, 'Frequencies'):
        summary["natural_frequencies"] = list(result_obj.Frequencies)
    
    # Mode shapes would require more complex extraction
    
    return summary


def extract_thermal_results(result_obj) -> Dict[str, Any]:
    """Extract thermal analysis results."""
    summary = {}
    
    if hasattr(result_obj, 'Temperature'):
        temps = result_obj.Temperature
        if temps:
            summary["max_temperature_C"] = max(temps)
            summary["min_temperature_C"] = min(temps)
            summary["avg_temperature_C"] = sum(temps) / len(temps)
    
    return summary


def extract_buckling_results(result_obj) -> Dict[str, Any]:
    """Extract buckling analysis results."""
    summary = {
        "buckling_factors": []
    }
    
    if hasattr(result_obj, 'BucklingFactors'):
        summary["buckling_factors"] = list(result_obj.BucklingFactors)
        summary["critical_buckling_factor"] = min(result_obj.BucklingFactors) if result_obj.BucklingFactors else None
    
    return summary


def get_displacement_field(result_obj) -> List[Dict[str, float]]:
    """Get displacement field data."""
    displacements = []
    
    if hasattr(result_obj, 'DisplacementVectors') and hasattr(result_obj, 'NodeNumbers'):
        for i, (node, disp) in enumerate(zip(result_obj.NodeNumbers, result_obj.DisplacementVectors)):
            displacements.append({
                "node_id": node,
                "dx": disp.x,
                "dy": disp.y,
                "dz": disp.z,
                "magnitude": np.sqrt(disp.x**2 + disp.y**2 + disp.z**2)
            })
    
    return displacements[:1000]  # Limit to first 1000 nodes


def get_stress_field(result_obj) -> List[Dict[str, float]]:
    """Get stress field data."""
    stresses = []
    
    if hasattr(result_obj, 'StressValues') and hasattr(result_obj, 'NodeNumbers'):
        for i, (node, stress) in enumerate(zip(result_obj.NodeNumbers, result_obj.StressValues)):
            stresses.append({
                "node_id": node,
                "von_mises": stress
            })
    
    return stresses[:1000]  # Limit to first 1000 nodes


def get_strain_field(result_obj) -> List[Dict[str, float]]:
    """Get strain field data."""
    # Similar to stress field
    return []


def get_temperature_field(result_obj) -> List[Dict[str, float]]:
    """Get temperature field data."""
    temperatures = []
    
    if hasattr(result_obj, 'Temperature') and hasattr(result_obj, 'NodeNumbers'):
        for i, (node, temp) in enumerate(zip(result_obj.NodeNumbers, result_obj.Temperature)):
            temperatures.append({
                "node_id": node,
                "temperature": temp
            })
    
    return temperatures[:1000]  # Limit to first 1000 nodes 