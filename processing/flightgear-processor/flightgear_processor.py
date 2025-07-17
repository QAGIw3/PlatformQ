"""FlightGear processor with distributed scenario testing capabilities."""
import os
import sys
import json
import csv
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
class FlightGearProcessingConfig:
    """Configuration for FlightGear processing."""
    mode: str = "metadata"  # "metadata" or "scenario_test"
    scenario_type: Optional[str] = None
    aircraft: str = "c172p"
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


class FlightGearProcessor(ProcessorBase):
    """Processor for FlightGear data with distributed scenario testing."""
    
    def __init__(self, tenant_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__("FlightGearProcessor", tenant_id, config)
        self.orchestrator = ComputeOrchestrator("flightgear-processor")
        self.policy_client = PolicyClient("http://simulation-service:8000")
        
        # Register job completion handler
        self.orchestrator.register_job_handler("flightgear", self._handle_job_completion)
    
    def validate_config(self, processing_config: Dict[str, Any]) -> bool:
        """Validate processing configuration."""
        config = FlightGearProcessingConfig(**processing_config)
        
        if config.mode not in ["metadata", "scenario_test"]:
            raise ValueError(f"Invalid mode: {config.mode}")
        
        if config.mode == "scenario_test":
            valid_scenarios = [
                "takeoff_performance", "landing_approach", "emergency_procedures",
                "weather_conditions", "system_failures", "navigation_accuracy",
                "fuel_efficiency", "autopilot_testing", "formation_flight"
            ]
            
            if config.scenario_type not in valid_scenarios:
                raise ValueError(f"Invalid scenario type: {config.scenario_type}")
        
        return True
    
    async def process(self, federation_id: str, step_data: Dict[str, Any]):
        # Read from shared state
        input_data = await self.policy_client.get_shared_state(federation_id, "openfoam_output")

        # Process data...
        output_data = {"flightgear_results": "..."}

        # Write to shared state
        await self.policy_client.update_shared_state(federation_id, "flightgear_output", output_data)
    
    def _extract_metadata(self, asset_uri: str) -> Dict[str, Any]:
        """Extract metadata from FlightGear log file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download log file
            log_file = Path(temp_dir) / "flight_log.csv"
            self.download_from_s3(asset_uri, str(log_file))
            
            # Parse log file
            with open(log_file, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                return {"error": "Log file is empty"}
            
            first_point = rows[0]
            last_point = rows[-1]
            
            # Extract metadata
            metadata = {
                "log_type": "FlightGear",
                "total_points": len(rows),
                "fields": list(first_point.keys()),
                "flight_summary": {}
            }
            
            # Calculate flight summary
            if 'time_seconds' in first_point:
                start_time = float(first_point.get('time_seconds', 0))
                end_time = float(last_point.get('time_seconds', 0))
                metadata["flight_summary"]["duration_seconds"] = end_time - start_time
            
            # Aircraft info
            metadata["flight_summary"]["aircraft"] = first_point.get('aircraft', 'Unknown')
            
            # Position data
            metadata["flight_summary"]["start_position"] = {
                "latitude": float(first_point.get('latitude_deg', 0)),
                "longitude": float(first_point.get('longitude_deg', 0)),
                "altitude_ft": float(first_point.get('altitude_ft', 0))
            }
            
            metadata["flight_summary"]["end_position"] = {
                "latitude": float(last_point.get('latitude_deg', 0)),
                "longitude": float(last_point.get('longitude_deg', 0)),
                "altitude_ft": float(last_point.get('altitude_ft', 0))
            }
            
            # Calculate statistics
            altitudes = [float(r.get('altitude_ft', 0)) for r in rows if 'altitude_ft' in r]
            speeds = [float(r.get('airspeed_kts', 0)) for r in rows if 'airspeed_kts' in r]
            
            if altitudes:
                metadata["flight_summary"]["altitude_stats"] = {
                    "min": min(altitudes),
                    "max": max(altitudes),
                    "avg": sum(altitudes) / len(altitudes)
                }
            
            if speeds:
                metadata["flight_summary"]["speed_stats"] = {
                    "min": min(speeds),
                    "max": max(speeds),
                    "avg": sum(speeds) / len(speeds)
                }
            
            # Detect events
            metadata["events"] = self._detect_flight_events(rows)
            
            return metadata
    
    def _detect_flight_events(self, flight_data: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Detect significant events in flight data."""
        events = []
        
        # Detect takeoff
        for i in range(1, len(flight_data)):
            prev_alt = float(flight_data[i-1].get('altitude_ft', 0))
            curr_alt = float(flight_data[i].get('altitude_ft', 0))
            
            # Takeoff detected
            if prev_alt < 50 and curr_alt >= 50:
                events.append({
                    "type": "takeoff",
                    "time": float(flight_data[i].get('time_seconds', 0)),
                    "altitude": curr_alt,
                    "airspeed": float(flight_data[i].get('airspeed_kts', 0))
                })
            
            # Landing detected
            if prev_alt >= 50 and curr_alt < 50 and i > len(flight_data) / 2:
                events.append({
                    "type": "landing",
                    "time": float(flight_data[i].get('time_seconds', 0)),
                    "altitude": curr_alt,
                    "airspeed": float(flight_data[i].get('airspeed_kts', 0))
                })
        
        return events
    
    def _run_scenario_test(self, scenario_config_uri: str, 
                          config: FlightGearProcessingConfig) -> Dict[str, Any]:
        """Submit FlightGear scenario testing job."""
        # Create compute job
        job = ComputeJob(
            job_id="",
            job_type="flightgear",
            input_data={
                "scenario_type": config.scenario_type,
                "aircraft": config.aircraft,
                "parameters": config.parameters
            },
            output_location=f"output-data/flightgear/{self.tenant_id}",
            parameters={}
        )
        
        # Add scenario config if provided
        if scenario_config_uri:
            job.input_data["scenario_config"] = scenario_config_uri
        
        # Submit job
        job_id = self.orchestrator.submit_job(job)
        
        return {
            "status": "submitted",
            "job_id": job_id,
            "scenario_type": config.scenario_type,
            "aircraft": config.aircraft,
            "message": f"FlightGear scenario test submitted"
        }
    
    def _handle_job_completion(self, job: ComputeJob):
        """Handle completed FlightGear scenario test."""
        logger.info(f"FlightGear job {job.job_id} completed with status {job.status}")
        
        if job.status == JobStatus.COMPLETED:
            # Process scenario results
            results = job.result
            
            # Store analysis results
            if "analysis" in results:
                self._store_scenario_analysis(job.job_id, results["analysis"])
    
    def _store_scenario_analysis(self, job_id: str, analysis: Dict[str, Any]):
        """Store scenario analysis results."""
        # Store in Ignite cache for fast retrieval
        analysis_cache = self.orchestrator.ignite_client.get_or_create_cache(
            "flightgear_analysis"
        )
        analysis_cache.put(job_id, analysis)
    
    def run_parameter_sweep(self, base_config: Dict[str, Any], 
                           parameter_name: str,
                           parameter_values: List[Any]) -> Dict[str, Any]:
        """Run multiple scenario tests with parameter variations."""
        jobs = []
        
        for value in parameter_values:
            # Create config for this variation
            config = base_config.copy()
            config["parameters"][parameter_name] = value
            
            # Submit job
            result = self._run_scenario_test("", FlightGearProcessingConfig(**config))
            jobs.append({
                "job_id": result["job_id"],
                parameter_name: value
            })
        
        return {
            "sweep_type": "parameter",
            "parameter": parameter_name,
            "values": parameter_values,
            "jobs": jobs,
            "total_jobs": len(jobs)
        }
    
    def run_monte_carlo_analysis(self, scenario_config: Dict[str, Any],
                                num_runs: int = 100,
                                variables: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run Monte Carlo analysis with random variations."""
        import random
        
        jobs = []
        
        for i in range(num_runs):
            # Create config with random variations
            config = scenario_config.copy()
            run_variations = {}
            
            if variables:
                for var in variables:
                    name = var["name"]
                    min_val = var["min"]
                    max_val = var["max"]
                    
                    if var.get("type") == "int":
                        value = random.randint(min_val, max_val)
                    else:
                        value = random.uniform(min_val, max_val)
                    
                    config["parameters"][name] = value
                    run_variations[name] = value
            
            # Submit job
            result = self._run_scenario_test("", FlightGearProcessingConfig(**config))
            jobs.append({
                "job_id": result["job_id"],
                "run_number": i,
                "variations": run_variations
            })
        
        return {
            "analysis_type": "monte_carlo",
            "num_runs": num_runs,
            "variables": variables,
            "jobs": jobs
        }
    
    def get_scenario_results(self, job_id: str) -> Dict[str, Any]:
        """Get detailed results from a scenario test."""
        job = self.orchestrator.get_job_status(job_id)
        
        if not job:
            return {"error": "Job not found"}
        
        results = {
            "job_id": job_id,
            "status": job.status.value,
            "scenario_type": job.input_data.get("scenario_type"),
            "aircraft": job.input_data.get("aircraft")
        }
        
        if job.status == JobStatus.COMPLETED and job.result:
            results.update(job.result)
            
            # Get analysis from cache if available
            analysis_cache = self.orchestrator.ignite_client.get_or_create_cache(
                "flightgear_analysis"
            )
            analysis = analysis_cache.get(job_id)
            if analysis:
                results["detailed_analysis"] = analysis
        
        return results


# Backward compatibility
def extract_flightgear_metadata(filepath: str) -> Dict[str, Any]:
    """Legacy function for metadata extraction."""
    processor = FlightGearProcessor("default")
    return processor._extract_metadata(f"file://{filepath}")


if __name__ == '__main__':
    # Example usage
    processor = FlightGearProcessor("test-tenant")
    
    # Metadata extraction
    metadata_config = {
        "mode": "metadata"
    }
    
    # Scenario test - takeoff performance
    takeoff_config = {
        "mode": "scenario_test",
        "scenario_type": "takeoff_performance",
        "aircraft": "c172p",
        "parameters": {
            "airport": "KSFO",
            "runway": "28L",
            "fuel_percent": 75,
            "payload_lbs": 400,
            "wind_speed_kts": 10,
            "wind_heading": 270
        }
    }
    
    # Emergency procedures test
    emergency_config = {
        "mode": "scenario_test",
        "scenario_type": "emergency_procedures",
        "aircraft": "b747-400",
        "parameters": {
            "altitude": 10000,
            "failures": [
                {"type": "engine_failure", "engine": 0, "time_seconds": 60}
            ],
            "max_duration_seconds": 300
        }
    } 