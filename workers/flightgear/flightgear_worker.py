"""FlightGear worker for distributed flight scenario testing."""
import os
import sys
import json
import time
import asyncio
import subprocess
import tempfile
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import telnetlib3
import csv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base.worker_base import ComputeWorker
from flightgear_utils import (
    parse_scenario_config,
    generate_flight_plan,
    setup_weather_conditions,
    inject_failures,
    analyze_flight_data
)

logger = logging.getLogger(__name__)


class FlightGearWorker(ComputeWorker):
    """Worker for distributed FlightGear scenario testing."""
    
    def __init__(self):
        super().__init__("flightgear")
        self.fg_root = os.getenv('FG_ROOT', '/usr/share/games/flightgear')
        self.telnet_port = 5500
        self.http_port = 5400
        
        # Start Xvfb for headless operation
        self._start_xvfb()
    
    def _start_xvfb(self):
        """Start X virtual framebuffer for headless FlightGear."""
        try:
            subprocess.Popen([
                'Xvfb', ':99', '-screen', '0', '1920x1080x24',
                '-ac', '+extension', 'GLX', '+render', '-noreset'
            ])
            os.environ['DISPLAY'] = ':99'
            time.sleep(2)
            logger.info("Started Xvfb for headless operation")
        except Exception as e:
            logger.warning(f"Failed to start Xvfb: {e}")
    
    def validate_job(self, job_data: Dict[str, Any]) -> bool:
        """Validate FlightGear scenario job."""
        required_fields = ["scenario_type", "aircraft", "parameters"]
        
        for field in required_fields:
            if field not in job_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate scenario type
        valid_scenarios = [
            "takeoff_performance", "landing_approach", "emergency_procedures",
            "weather_conditions", "system_failures", "navigation_accuracy",
            "fuel_efficiency", "autopilot_testing", "formation_flight"
        ]
        
        if job_data["scenario_type"] not in valid_scenarios:
            raise ValueError(f"Invalid scenario type: {job_data['scenario_type']}")
        
        # Validate aircraft
        if not job_data["aircraft"]:
            raise ValueError("Aircraft must be specified")
        
        return True
    
    def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a FlightGear scenario testing job."""
        job_id = job_data["job_id"]
        scenario_type = job_data["scenario_type"]
        aircraft = job_data["aircraft"]
        parameters = job_data["parameters"]
        
        # Create temporary working directory
        with tempfile.TemporaryDirectory() as work_dir:
            work_path = Path(work_dir)
            
            try:
                # Download scenario configuration if provided
                if "scenario_config" in job_data:
                    scenario_path = work_path / "scenario.xml"
                    self.download_input_data(job_data["scenario_config"], str(scenario_path))
                    scenario_config = parse_scenario_config(scenario_path)
                else:
                    # Generate scenario configuration
                    scenario_config = self._generate_scenario_config(
                        scenario_type, aircraft, parameters
                    )
                
                # Run the scenario
                result = self._run_scenario(
                    work_path, aircraft, scenario_config, parameters
                )
                
                # Analyze results
                analysis = analyze_flight_data(
                    work_path / "flight_data.csv",
                    scenario_type,
                    parameters.get("analysis_criteria", {})
                )
                
                # Upload results
                output_path = job_data.get("output_location", f"output-data/{job_id}")
                self._upload_results(work_path, output_path)
                
                return {
                    "status": "completed",
                    "scenario_type": scenario_type,
                    "aircraft": aircraft,
                    "duration": result.get("duration", 0),
                    "data_points": result.get("data_points", 0),
                    "analysis": analysis,
                    "output_path": output_path
                }
                
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                raise
    
    def _generate_scenario_config(self, scenario_type: str, aircraft: str,
                                 parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Generate scenario configuration based on type."""
        config = {
            "aircraft": aircraft,
            "scenario_type": scenario_type,
            "initial_conditions": {},
            "environment": {},
            "triggers": [],
            "monitoring": {}
        }
        
        # Set initial conditions based on scenario
        if scenario_type == "takeoff_performance":
            config["initial_conditions"] = {
                "airport": parameters.get("airport", "KSFO"),
                "runway": parameters.get("runway", "28L"),
                "fuel_percent": parameters.get("fuel_percent", 75),
                "payload_lbs": parameters.get("payload_lbs", 0),
                "flaps": parameters.get("flaps", 2)
            }
            config["monitoring"] = {
                "parameters": ["airspeed", "altitude", "pitch", "engine_n1", "gear_position"],
                "sample_rate_hz": 10
            }
            
        elif scenario_type == "landing_approach":
            config["initial_conditions"] = {
                "position": {
                    "distance_nm": parameters.get("approach_distance", 10),
                    "altitude_ft": parameters.get("approach_altitude", 3000),
                    "heading": parameters.get("approach_heading", 280)
                },
                "airport": parameters.get("airport", "KSFO"),
                "runway": parameters.get("runway", "28L"),
                "airspeed_kts": parameters.get("approach_speed", 140)
            }
            
        elif scenario_type == "emergency_procedures":
            config["initial_conditions"] = {
                "altitude_ft": parameters.get("altitude", 10000),
                "airspeed_kts": parameters.get("airspeed", 250),
                "position": parameters.get("position", {"lat": 37.6213, "lon": -122.3789})
            }
            config["triggers"] = self._generate_failure_triggers(parameters)
            
        elif scenario_type == "weather_conditions":
            config["environment"] = {
                "weather": setup_weather_conditions(parameters.get("weather", {})),
                "time_of_day": parameters.get("time_of_day", "noon")
            }
        
        return config
    
    def _generate_failure_triggers(self, parameters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate failure triggers for emergency scenarios."""
        triggers = []
        failures = parameters.get("failures", ["engine_failure"])
        
        for i, failure in enumerate(failures):
            trigger_time = parameters.get(f"failure_{i}_time", 60 + i * 30)
            
            if failure == "engine_failure":
                triggers.append({
                    "time_seconds": trigger_time,
                    "action": "set_property",
                    "property": "/engines/engine[0]/running",
                    "value": "false"
                })
            elif failure == "hydraulic_failure":
                triggers.append({
                    "time_seconds": trigger_time,
                    "action": "set_property",
                    "property": "/systems/hydraulic/pressure",
                    "value": "0"
                })
            elif failure == "electrical_failure":
                triggers.append({
                    "time_seconds": trigger_time,
                    "action": "set_property",
                    "property": "/systems/electrical/battery-bus",
                    "value": "0"
                })
        
        return triggers
    
    def _run_scenario(self, work_dir: Path, aircraft: str,
                     scenario_config: Dict[str, Any],
                     parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Run FlightGear scenario."""
        # Prepare FlightGear command
        cmd = [
            "fgfs",
            f"--aircraft={aircraft}",
            "--disable-sound",
            "--disable-panel",
            "--disable-hud",
            "--fog-disable",
            "--telnet=localhost,{},{}".format(self.telnet_port, self.telnet_port),
            "--httpd={}".format(self.http_port),
            f"--fg-root={self.fg_root}",
            "--log-level=warn"
        ]
        
        # Add initial conditions
        initial = scenario_config.get("initial_conditions", {})
        if "airport" in initial:
            cmd.append(f"--airport={initial['airport']}")
        if "runway" in initial:
            cmd.append(f"--runway={initial['runway']}")
        if "altitude_ft" in initial:
            cmd.append(f"--altitude={initial['altitude_ft']}")
        if "airspeed_kts" in initial:
            cmd.append(f"--vc={initial['airspeed_kts']}")
        
        # Add weather settings
        weather = scenario_config.get("environment", {}).get("weather", {})
        if weather:
            if "wind_speed_kts" in weather:
                cmd.append(f"--wind={weather.get('wind_heading', 0)}@{weather['wind_speed_kts']}")
            if "visibility_m" in weather:
                cmd.append(f"--visibility={weather['visibility_m']}")
        
        # Start FlightGear
        logger.info(f"Starting FlightGear: {' '.join(cmd)}")
        fg_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(work_dir)
        )
        
        # Wait for FlightGear to start
        time.sleep(10)
        
        try:
            # Run scenario control asynchronously
            result = asyncio.run(
                self._control_scenario(work_dir, scenario_config, parameters)
            )
            
            return result
            
        finally:
            # Terminate FlightGear
            fg_process.terminate()
            fg_process.wait(timeout=10)
    
    async def _control_scenario(self, work_dir: Path,
                              scenario_config: Dict[str, Any],
                              parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Control FlightGear scenario via telnet."""
        # Connect to FlightGear telnet
        reader, writer = await telnetlib3.open_connection(
            'localhost', self.telnet_port
        )
        
        # Wait for prompt
        await reader.readuntil(b'>')
        
        # Start data logging
        log_file = work_dir / "flight_data.csv"
        data_logger = FlightDataLogger(log_file, scenario_config.get("monitoring", {}))
        
        # Run scenario
        start_time = time.time()
        data_points = 0
        
        try:
            # Apply initial settings
            await self._apply_initial_settings(writer, scenario_config)
            
            # Main scenario loop
            max_duration = parameters.get("max_duration_seconds", 600)
            sample_interval = 1.0 / scenario_config.get("monitoring", {}).get("sample_rate_hz", 1)
            
            while time.time() - start_time < max_duration:
                current_time = time.time() - start_time
                
                # Check and apply triggers
                for trigger in scenario_config.get("triggers", []):
                    if trigger["time_seconds"] <= current_time < trigger["time_seconds"] + 1:
                        await self._apply_trigger(writer, trigger)
                
                # Collect data
                data = await self._collect_flight_data(reader, writer)
                data_logger.log_data(current_time, data)
                data_points += 1
                
                # Check termination conditions
                if self._check_termination_conditions(data, parameters):
                    break
                
                await asyncio.sleep(sample_interval)
            
        finally:
            writer.close()
            await writer.wait_closed()
            data_logger.close()
        
        duration = time.time() - start_time
        
        return {
            "duration": duration,
            "data_points": data_points
        }
    
    async def _apply_initial_settings(self, writer, scenario_config: Dict[str, Any]):
        """Apply initial FlightGear settings."""
        initial = scenario_config.get("initial_conditions", {})
        
        # Set fuel
        if "fuel_percent" in initial:
            fuel = initial["fuel_percent"] / 100.0
            writer.write(f"set /consumables/fuel/tank[0]/level-norm {fuel}\r\n".encode())
            writer.write(f"set /consumables/fuel/tank[1]/level-norm {fuel}\r\n".encode())
        
        # Set payload
        if "payload_lbs" in initial:
            writer.write(f"set /sim/weight[0]/weight-lb {initial['payload_lbs']}\r\n".encode())
        
        # Set autopilot if specified
        if "autopilot" in initial:
            ap = initial["autopilot"]
            if ap.get("enabled"):
                writer.write(b"set /autopilot/locks/altitude altitude-hold\r\n")
                writer.write(b"set /autopilot/locks/heading dg-heading-hold\r\n")
                writer.write(b"set /autopilot/locks/speed speed-with-throttle\r\n")
        
        await writer.drain()
    
    async def _apply_trigger(self, writer, trigger: Dict[str, Any]):
        """Apply a scenario trigger."""
        logger.info(f"Applying trigger: {trigger}")
        
        if trigger["action"] == "set_property":
            cmd = f"set {trigger['property']} {trigger['value']}\r\n"
            writer.write(cmd.encode())
            await writer.drain()
    
    async def _collect_flight_data(self, reader, writer) -> Dict[str, Any]:
        """Collect current flight data."""
        data = {}
        
        # Properties to collect
        properties = [
            ("altitude_ft", "/position/altitude-ft"),
            ("latitude_deg", "/position/latitude-deg"),
            ("longitude_deg", "/position/longitude-deg"),
            ("airspeed_kts", "/velocities/airspeed-kt"),
            ("groundspeed_kts", "/velocities/groundspeed-kt"),
            ("vertical_speed_fpm", "/velocities/vertical-speed-fps"),
            ("heading_deg", "/orientation/heading-magnetic-deg"),
            ("pitch_deg", "/orientation/pitch-deg"),
            ("roll_deg", "/orientation/roll-deg"),
            ("engine_n1", "/engines/engine[0]/n1"),
            ("fuel_flow_gph", "/engines/engine[0]/fuel-flow-gph"),
            ("gear_position", "/gear/gear[0]/position-norm")
        ]
        
        for key, prop in properties:
            writer.write(f"get {prop}\r\n".encode())
            await writer.drain()
            
            # Read response
            response = await reader.readuntil(b'\r\n')
            value = response.decode().strip()
            
            try:
                data[key] = float(value.split('=')[-1].strip())
            except:
                data[key] = 0.0
        
        return data
    
    def _check_termination_conditions(self, data: Dict[str, Any],
                                    parameters: Dict[str, Any]) -> bool:
        """Check if scenario should terminate."""
        # Check altitude limits
        if data.get("altitude_ft", 0) < parameters.get("min_altitude_ft", -1000):
            return True
        
        # Check for crash (very low altitude with high vertical speed)
        if (data.get("altitude_ft", 0) < 10 and 
            abs(data.get("vertical_speed_fpm", 0)) > 1000):
            return True
        
        return False
    
    def _upload_results(self, work_dir: Path, output_path: str):
        """Upload scenario results to MinIO."""
        bucket, prefix = output_path.split('/', 1)
        
        # Upload all result files
        for file_path in work_dir.glob('*'):
            if file_path.is_file():
                object_name = f"{prefix}/{file_path.name}"
                self.minio_client.fput_object(bucket, object_name, str(file_path))
                logger.info(f"Uploaded {file_path.name}")


class FlightDataLogger:
    """Logger for flight data."""
    
    def __init__(self, log_file: Path, monitoring_config: Dict[str, Any]):
        self.log_file = log_file
        self.csv_file = None
        self.csv_writer = None
        self.parameters = monitoring_config.get("parameters", [])
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup CSV logger."""
        self.csv_file = open(self.log_file, 'w', newline='')
        fieldnames = ['time_seconds'] + self.parameters
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
        self.csv_writer.writeheader()
    
    def log_data(self, time_seconds: float, data: Dict[str, Any]):
        """Log flight data."""
        row = {'time_seconds': time_seconds}
        for param in self.parameters:
            row[param] = data.get(param, 0.0)
        self.csv_writer.writerow(row)
    
    def close(self):
        """Close logger."""
        if self.csv_file:
            self.csv_file.close()


if __name__ == "__main__":
    # Create and run worker
    worker = FlightGearWorker()
    
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    finally:
        worker.cleanup() 