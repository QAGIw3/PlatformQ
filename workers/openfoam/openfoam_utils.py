"""OpenFOAM utility functions."""
import os
import re
import subprocess
import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import numpy as np

logger = logging.getLogger(__name__)


def parse_openfoam_case(case_dir: Path) -> Dict[str, Any]:
    """Parse OpenFOAM case configuration."""
    config = {}
    
    # Parse controlDict
    control_dict_path = case_dir / "system" / "controlDict"
    if control_dict_path.exists():
        config["control"] = parse_foam_dict(control_dict_path)
    
    # Parse fvSchemes
    fv_schemes_path = case_dir / "system" / "fvSchemes"
    if fv_schemes_path.exists():
        config["schemes"] = parse_foam_dict(fv_schemes_path)
    
    # Parse fvSolution
    fv_solution_path = case_dir / "system" / "fvSolution"
    if fv_solution_path.exists():
        config["solution"] = parse_foam_dict(fv_solution_path)
    
    # Check for mesh
    config["has_mesh"] = (case_dir / "constant" / "polyMesh").exists()
    
    # Get initial conditions
    time_dirs = get_time_directories(case_dir)
    if time_dirs:
        config["initial_time"] = min(time_dirs)
        config["available_times"] = sorted(time_dirs)
    
    return config


def parse_foam_dict(dict_path: Path) -> Dict[str, Any]:
    """Parse an OpenFOAM dictionary file."""
    params = {}
    
    with open(dict_path, 'r') as f:
        content = f.read()
    
    # Remove comments
    content = re.sub(r'//.*?\n', '\n', content)
    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
    
    # Simple parameter extraction
    param_pattern = re.compile(r'(\w+)\s+([^;]+);')
    for match in param_pattern.finditer(content):
        key = match.group(1)
        value = match.group(2).strip()
        
        # Try to parse value
        if value in ['true', 'yes', 'on']:
            params[key] = True
        elif value in ['false', 'no', 'off']:
            params[key] = False
        elif value.replace('.', '').replace('-', '').isdigit():
            try:
                params[key] = float(value)
            except:
                params[key] = value
        else:
            params[key] = value
    
    return params


def get_time_directories(case_dir: Path) -> List[float]:
    """Get list of time directories in the case."""
    time_dirs = []
    
    for entry in case_dir.iterdir():
        if entry.is_dir():
            try:
                time_value = float(entry.name)
                time_dirs.append(time_value)
            except ValueError:
                continue
    
    return time_dirs


def decompose_case(case_dir: Path, num_processors: int):
    """Decompose case for parallel execution."""
    # Create decomposeParDict
    decompose_dict = case_dir / "system" / "decomposeParDict"
    
    with open(decompose_dict, 'w') as f:
        f.write(f"""FoamFile
{{
    version     2.0;
    format      ascii;
    class       dictionary;
    location    "system";
    object      decomposeParDict;
}}

numberOfSubdomains {num_processors};

method          scotch;

simpleCoeffs
{{
    n               ({num_processors} 1 1);
    delta           0.001;
}}

hierarchicalCoeffs
{{
    n               ({num_processors} 1 1);
    delta           0.001;
    order           xyz;
}}

scotchCoeffs
{{
}}

distributed     no;

roots           ( );
""")
    
    # Run decomposePar
    cmd = ["decomposePar", "-force"]
    logger.info(f"Running decomposePar with {num_processors} processors")
    
    process = subprocess.run(
        cmd,
        cwd=str(case_dir),
        capture_output=True,
        text=True
    )
    
    if process.returncode != 0:
        raise RuntimeError(f"decomposePar failed: {process.stderr}")
    
    logger.info("Case decomposition completed")


def run_solver(case_dir: Path, solver: str, parallel: bool = False, 
               num_processors: Optional[int] = None) -> Dict[str, Any]:
    """Run OpenFOAM solver."""
    result = {}
    
    if parallel and num_processors:
        cmd = ["mpirun", "-np", str(num_processors), solver, "-parallel"]
        log_file = case_dir / f"log.{solver}.parallel"
    else:
        cmd = [solver]
        log_file = case_dir / f"log.{solver}"
    
    logger.info(f"Running solver: {' '.join(cmd)}")
    
    # Run solver and capture output
    with open(log_file, 'w') as log:
        process = subprocess.Popen(
            cmd,
            cwd=str(case_dir),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        # Wait for completion
        process.wait()
    
    if process.returncode != 0:
        # Read last lines of log for error
        with open(log_file, 'r') as f:
            lines = f.readlines()
            error_msg = ''.join(lines[-50:])  # Last 50 lines
        raise RuntimeError(f"Solver {solver} failed: {error_msg}")
    
    # Parse convergence from log
    result["convergence"] = parse_convergence_log(log_file)
    
    logger.info(f"Solver {solver} completed successfully")
    return result


def reconstruct_case(case_dir: Path):
    """Reconstruct parallel case."""
    cmd = ["reconstructPar", "-latestTime"]
    logger.info("Running reconstructPar")
    
    process = subprocess.run(
        cmd,
        cwd=str(case_dir),
        capture_output=True,
        text=True
    )
    
    if process.returncode != 0:
        # Try without -latestTime option
        cmd = ["reconstructPar"]
        process = subprocess.run(
            cmd,
            cwd=str(case_dir),
            capture_output=True,
            text=True
        )
        
        if process.returncode != 0:
            raise RuntimeError(f"reconstructPar failed: {process.stderr}")
    
    logger.info("Case reconstruction completed")


def parse_convergence_log(log_file: Path) -> Dict[str, Any]:
    """Parse convergence information from solver log."""
    convergence = {
        "iterations": [],
        "residuals": {},
        "continuity_errors": []
    }
    
    with open(log_file, 'r') as f:
        lines = f.readlines()
    
    # Parse residuals
    residual_pattern = re.compile(r'Solving for (\w+).*Final residual = ([\d.e+-]+)')
    continuity_pattern = re.compile(r'continuity errors.*sum local = ([\d.e+-]+).*global = ([\d.e+-]+)')
    time_pattern = re.compile(r'^Time = ([\d.]+)')
    
    current_time = None
    time_residuals = {}
    
    for line in lines:
        # Check for time step
        time_match = time_pattern.match(line)
        if time_match:
            if current_time and time_residuals:
                convergence["iterations"].append({
                    "time": current_time,
                    "residuals": time_residuals.copy()
                })
            current_time = float(time_match.group(1))
            time_residuals = {}
        
        # Check for residuals
        residual_match = residual_pattern.search(line)
        if residual_match:
            field = residual_match.group(1)
            residual = float(residual_match.group(2))
            
            if field not in convergence["residuals"]:
                convergence["residuals"][field] = []
            
            time_residuals[field] = residual
            convergence["residuals"][field].append(residual)
        
        # Check for continuity errors
        continuity_match = continuity_pattern.search(line)
        if continuity_match:
            convergence["continuity_errors"].append({
                "time": current_time,
                "local": float(continuity_match.group(1)),
                "global": float(continuity_match.group(2))
            })
    
    # Add last time step
    if current_time and time_residuals:
        convergence["iterations"].append({
            "time": current_time,
            "residuals": time_residuals
        })
    
    return convergence


def extract_results(case_dir: Path, fields: List[str]) -> Dict[str, Any]:
    """Extract simulation results for specified fields."""
    results = {}
    
    # Get latest time directory
    time_dirs = get_time_directories(case_dir)
    if not time_dirs:
        return results
    
    latest_time = max(time_dirs)
    time_dir = case_dir / str(latest_time)
    
    results["final_time"] = latest_time
    results["fields"] = {}
    
    # Extract requested fields
    for field in fields:
        field_file = time_dir / field
        if field_file.exists():
            field_data = read_field_file(field_file)
            
            # Calculate statistics
            if field_data is not None:
                results["fields"][field] = {
                    "min": float(np.min(field_data)),
                    "max": float(np.max(field_data)),
                    "mean": float(np.mean(field_data)),
                    "std": float(np.std(field_data))
                }
    
    # Extract mesh quality metrics if available
    check_mesh_log = case_dir / "log.checkMesh"
    if check_mesh_log.exists():
        results["mesh_quality"] = parse_mesh_quality(check_mesh_log)
    
    return results


def read_field_file(field_file: Path) -> Optional[np.ndarray]:
    """Read OpenFOAM field file and extract values."""
    try:
        with open(field_file, 'r') as f:
            content = f.read()
        
        # Find internalField section
        internal_match = re.search(r'internalField\s+nonuniform\s+List<\w+>\s*\d+\s*\((.*?)\)', 
                                  content, re.DOTALL)
        
        if internal_match:
            values_str = internal_match.group(1)
            # Extract numbers
            values = re.findall(r'[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?', values_str)
            return np.array([float(v) for v in values])
        
        # Try uniform field
        uniform_match = re.search(r'internalField\s+uniform\s+([-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?)', 
                                 content)
        if uniform_match:
            value = float(uniform_match.group(1))
            # Return array with single value
            return np.array([value])
        
    except Exception as e:
        logger.error(f"Failed to read field file {field_file}: {e}")
    
    return None


def parse_mesh_quality(check_mesh_log: Path) -> Dict[str, Any]:
    """Parse mesh quality metrics from checkMesh log."""
    quality = {}
    
    try:
        with open(check_mesh_log, 'r') as f:
            content = f.read()
        
        # Extract mesh statistics
        patterns = {
            "cells": r'cells:\s*(\d+)',
            "faces": r'faces:\s*(\d+)',
            "points": r'points:\s*(\d+)',
            "non_orthogonality_max": r'Max non-orthogonality = ([\d.]+)',
            "non_orthogonality_avg": r'average: ([\d.]+)',
            "max_skewness": r'Max skewness = ([\d.]+)',
            "max_aspect_ratio": r'Max aspect ratio = ([\d.]+)'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, content)
            if match:
                try:
                    quality[key] = float(match.group(1))
                except:
                    quality[key] = match.group(1)
        
        # Check if mesh is OK
        quality["mesh_ok"] = "Mesh OK" in content
        
    except Exception as e:
        logger.error(f"Failed to parse mesh quality: {e}")
    
    return quality 