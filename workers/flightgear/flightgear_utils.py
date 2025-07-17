"""FlightGear utility functions."""
import xml.etree.ElementTree as ET
import csv
import json
import logging
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from geopy.distance import distance

logger = logging.getLogger(__name__)


def parse_scenario_config(scenario_file: Path) -> Dict[str, Any]:
    """Parse FlightGear scenario XML configuration."""
    try:
        tree = ET.parse(scenario_file)
        root = tree.getroot()
        
        config = {
            "aircraft": root.findtext("aircraft", "c172p"),
            "initial_conditions": {},
            "environment": {},
            "triggers": [],
            "monitoring": {}
        }
        
        # Parse initial conditions
        initial = root.find("initial_conditions")
        if initial is not None:
            config["initial_conditions"] = {
                "airport": initial.findtext("airport"),
                "runway": initial.findtext("runway"),
                "altitude_ft": float(initial.findtext("altitude_ft", "0")),
                "airspeed_kts": float(initial.findtext("airspeed_kts", "0")),
                "heading_deg": float(initial.findtext("heading_deg", "0")),
                "fuel_percent": float(initial.findtext("fuel_percent", "100"))
            }
        
        # Parse environment
        env = root.find("environment")
        if env is not None:
            weather = env.find("weather")
            if weather is not None:
                config["environment"]["weather"] = {
                    "wind_speed_kts": float(weather.findtext("wind_speed_kts", "0")),
                    "wind_heading": float(weather.findtext("wind_heading", "0")),
                    "visibility_m": float(weather.findtext("visibility_m", "10000")),
                    "cloud_base_ft": float(weather.findtext("cloud_base_ft", "5000")),
                    "temperature_c": float(weather.findtext("temperature_c", "15"))
                }
        
        # Parse triggers
        triggers = root.find("triggers")
        if triggers is not None:
            for trigger in triggers.findall("trigger"):
                config["triggers"].append({
                    "time_seconds": float(trigger.get("time", "0")),
                    "action": trigger.get("action"),
                    "property": trigger.findtext("property"),
                    "value": trigger.findtext("value")
                })
        
        # Parse monitoring
        monitoring = root.find("monitoring")
        if monitoring is not None:
            config["monitoring"] = {
                "parameters": [p.text for p in monitoring.findall("parameter")],
                "sample_rate_hz": float(monitoring.findtext("sample_rate_hz", "1"))
            }
        
        return config
        
    except Exception as e:
        logger.error(f"Error parsing scenario config: {e}")
        return {}


def generate_flight_plan(waypoints: List[Dict[str, float]], 
                        cruise_altitude_ft: float = 10000) -> List[Dict[str, Any]]:
    """Generate a flight plan from waypoints."""
    flight_plan = []
    
    for i, wp in enumerate(waypoints):
        leg = {
            "waypoint_id": f"WP{i:03d}",
            "latitude": wp["lat"],
            "longitude": wp["lon"],
            "altitude_ft": wp.get("alt", cruise_altitude_ft),
            "speed_kts": wp.get("speed", 250)
        }
        
        # Calculate heading and distance to next waypoint
        if i < len(waypoints) - 1:
            next_wp = waypoints[i + 1]
            
            # Calculate distance
            pos1 = (wp["lat"], wp["lon"])
            pos2 = (next_wp["lat"], next_wp["lon"])
            leg["distance_nm"] = distance(pos1, pos2).nm
            
            # Calculate heading (simplified)
            lat_diff = next_wp["lat"] - wp["lat"]
            lon_diff = next_wp["lon"] - wp["lon"]
            heading = np.degrees(np.arctan2(lon_diff, lat_diff))
            leg["heading_to_next"] = (heading + 360) % 360
        
        flight_plan.append(leg)
    
    return flight_plan


def setup_weather_conditions(weather_params: Dict[str, Any]) -> Dict[str, Any]:
    """Setup weather conditions for scenario."""
    weather = {
        "wind_speed_kts": weather_params.get("wind_speed_kts", 0),
        "wind_heading": weather_params.get("wind_heading", 0),
        "visibility_m": weather_params.get("visibility_m", 10000),
        "cloud_base_ft": weather_params.get("cloud_base_ft", 5000),
        "cloud_coverage": weather_params.get("cloud_coverage", "clear"),
        "temperature_c": weather_params.get("temperature_c", 15),
        "pressure_inhg": weather_params.get("pressure_inhg", 29.92),
        "turbulence": weather_params.get("turbulence", 0)
    }
    
    # Add weather presets
    preset = weather_params.get("preset")
    if preset == "vfr":
        weather.update({
            "visibility_m": 10000,
            "cloud_base_ft": 5000,
            "cloud_coverage": "few"
        })
    elif preset == "ifr":
        weather.update({
            "visibility_m": 1600,
            "cloud_base_ft": 200,
            "cloud_coverage": "overcast"
        })
    elif preset == "storm":
        weather.update({
            "wind_speed_kts": 25,
            "visibility_m": 3000,
            "cloud_base_ft": 1000,
            "cloud_coverage": "overcast",
            "turbulence": 0.7
        })
    
    return weather


def inject_failures(failures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate failure injection commands."""
    commands = []
    
    for failure in failures:
        fail_type = failure["type"]
        trigger_time = failure.get("time_seconds", 60)
        
        if fail_type == "engine_failure":
            engine = failure.get("engine", 0)
            commands.append({
                "time": trigger_time,
                "property": f"/engines/engine[{engine}]/running",
                "value": "false"
            })
            
        elif fail_type == "control_surface":
            surface = failure.get("surface", "aileron")
            commands.append({
                "time": trigger_time,
                "property": f"/controls/flight/{surface}-trim",
                "value": str(failure.get("position", 0))
            })
            
        elif fail_type == "instrument":
            instrument = failure.get("instrument", "altimeter")
            commands.append({
                "time": trigger_time,
                "property": f"/instrumentation/{instrument}/serviceable",
                "value": "false"
            })
            
        elif fail_type == "system":
            system = failure.get("system", "electrical")
            commands.append({
                "time": trigger_time,
                "property": f"/systems/{system}/serviceable",
                "value": "false"
            })
    
    return commands


def analyze_flight_data(data_file: Path, scenario_type: str,
                       criteria: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze flight data based on scenario type."""
    try:
        # Read flight data
        data = []
        with open(data_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert values to float
                data_point = {}
                for key, value in row.items():
                    try:
                        data_point[key] = float(value)
                    except:
                        data_point[key] = value
                data.append(data_point)
        
        if not data:
            return {"error": "No data to analyze"}
        
        analysis = {
            "total_points": len(data),
            "duration_seconds": data[-1]["time_seconds"] - data[0]["time_seconds"],
            "scenario_type": scenario_type
        }
        
        # Scenario-specific analysis
        if scenario_type == "takeoff_performance":
            analysis.update(analyze_takeoff(data, criteria))
        elif scenario_type == "landing_approach":
            analysis.update(analyze_landing(data, criteria))
        elif scenario_type == "emergency_procedures":
            analysis.update(analyze_emergency(data, criteria))
        elif scenario_type == "fuel_efficiency":
            analysis.update(analyze_fuel_efficiency(data, criteria))
        
        # General statistics
        analysis["statistics"] = calculate_statistics(data)
        
        return analysis
        
    except Exception as e:
        logger.error(f"Error analyzing flight data: {e}")
        return {"error": str(e)}


def analyze_takeoff(data: List[Dict[str, float]], 
                   criteria: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze takeoff performance."""
    takeoff_analysis = {}
    
    # Find rotation point (when aircraft leaves ground)
    on_ground = True
    rotation_point = None
    
    for i, point in enumerate(data):
        if on_ground and point.get("altitude_ft", 0) > 50:
            on_ground = False
            rotation_point = i
            break
    
    if rotation_point:
        takeoff_analysis["rotation_time"] = data[rotation_point]["time_seconds"]
        takeoff_analysis["rotation_speed_kts"] = data[rotation_point].get("airspeed_kts", 0)
        takeoff_analysis["rotation_distance_ft"] = estimate_distance(data[:rotation_point])
        
        # Find 50ft crossing
        for i in range(rotation_point, len(data)):
            if data[i].get("altitude_ft", 0) >= 50:
                takeoff_analysis["fifty_ft_time"] = data[i]["time_seconds"]
                takeoff_analysis["fifty_ft_speed_kts"] = data[i].get("airspeed_kts", 0)
                break
        
        # Calculate climb rate
        if len(data) > rotation_point + 10:
            alt_diff = data[rotation_point + 10]["altitude_ft"] - data[rotation_point]["altitude_ft"]
            time_diff = data[rotation_point + 10]["time_seconds"] - data[rotation_point]["time_seconds"]
            takeoff_analysis["initial_climb_rate_fpm"] = (alt_diff / time_diff) * 60
    
    # Check against criteria
    if criteria:
        takeoff_analysis["meets_criteria"] = True
        if "max_rotation_distance" in criteria:
            if takeoff_analysis.get("rotation_distance_ft", float('inf')) > criteria["max_rotation_distance"]:
                takeoff_analysis["meets_criteria"] = False
    
    return {"takeoff": takeoff_analysis}


def analyze_landing(data: List[Dict[str, float]], 
                   criteria: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze landing approach."""
    landing_analysis = {}
    
    # Find touchdown point (when altitude reaches near zero)
    touchdown_point = None
    for i in range(len(data) - 1, -1, -1):
        if data[i].get("altitude_ft", 0) < 10:
            touchdown_point = i
            break
    
    if touchdown_point:
        landing_analysis["touchdown_time"] = data[touchdown_point]["time_seconds"]
        landing_analysis["touchdown_speed_kts"] = data[touchdown_point].get("airspeed_kts", 0)
        landing_analysis["touchdown_vertical_speed_fpm"] = data[touchdown_point].get("vertical_speed_fpm", 0)
        
        # Analyze approach stability
        approach_data = data[max(0, touchdown_point - 100):touchdown_point]
        if approach_data:
            speeds = [p.get("airspeed_kts", 0) for p in approach_data]
            altitudes = [p.get("altitude_ft", 0) for p in approach_data]
            
            landing_analysis["approach_speed_avg"] = np.mean(speeds)
            landing_analysis["approach_speed_std"] = np.std(speeds)
            
            # Calculate glide slope
            if len(altitudes) > 1:
                glide_angle = calculate_glide_angle(approach_data)
                landing_analysis["glide_angle_deg"] = glide_angle
    
    return {"landing": landing_analysis}


def analyze_emergency(data: List[Dict[str, float]], 
                     criteria: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze emergency procedure handling."""
    emergency_analysis = {
        "min_altitude": min(p.get("altitude_ft", 0) for p in data),
        "max_bank_angle": max(abs(p.get("roll_deg", 0)) for p in data),
        "landing_achieved": data[-1].get("altitude_ft", 1000) < 100
    }
    
    # Find engine failure point if applicable
    for i, point in enumerate(data[1:], 1):
        if data[i-1].get("engine_n1", 0) > 50 and point.get("engine_n1", 0) < 10:
            emergency_analysis["engine_failure_time"] = point["time_seconds"]
            emergency_analysis["engine_failure_altitude"] = point.get("altitude_ft", 0)
            
            # Analyze response time
            # Look for control inputs after failure
            for j in range(i, min(i + 50, len(data))):
                if abs(data[j].get("pitch_deg", 0) - data[i].get("pitch_deg", 0)) > 2:
                    emergency_analysis["response_time"] = data[j]["time_seconds"] - point["time_seconds"]
                    break
    
    return {"emergency": emergency_analysis}


def analyze_fuel_efficiency(data: List[Dict[str, float]], 
                           criteria: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze fuel efficiency."""
    fuel_analysis = {}
    
    # Calculate fuel consumption
    fuel_flows = [p.get("fuel_flow_gph", 0) for p in data]
    if fuel_flows:
        fuel_analysis["avg_fuel_flow_gph"] = np.mean(fuel_flows)
        fuel_analysis["total_fuel_used_gal"] = sum(fuel_flows) * (data[-1]["time_seconds"] / 3600)
        
        # Calculate distance traveled
        total_distance = estimate_total_distance(data)
        fuel_analysis["total_distance_nm"] = total_distance
        
        if total_distance > 0:
            fuel_analysis["fuel_efficiency_nm_per_gal"] = (
                total_distance / fuel_analysis["total_fuel_used_gal"]
            )
    
    return {"fuel_efficiency": fuel_analysis}


def calculate_statistics(data: List[Dict[str, float]]) -> Dict[str, Any]:
    """Calculate general flight statistics."""
    stats = {}
    
    # Speed statistics
    speeds = [p.get("airspeed_kts", 0) for p in data if p.get("airspeed_kts", 0) > 0]
    if speeds:
        stats["airspeed"] = {
            "min": min(speeds),
            "max": max(speeds),
            "avg": np.mean(speeds),
            "std": np.std(speeds)
        }
    
    # Altitude statistics
    altitudes = [p.get("altitude_ft", 0) for p in data]
    if altitudes:
        stats["altitude"] = {
            "min": min(altitudes),
            "max": max(altitudes),
            "avg": np.mean(altitudes)
        }
    
    # G-force estimation (from vertical speed changes)
    if len(data) > 1:
        g_forces = []
        for i in range(1, len(data)):
            vs_change = (data[i].get("vertical_speed_fpm", 0) - 
                        data[i-1].get("vertical_speed_fpm", 0))
            time_diff = data[i]["time_seconds"] - data[i-1]["time_seconds"]
            if time_diff > 0:
                # Convert to g-force
                g_force = 1 + (vs_change / 60 * 0.3048) / (9.81 * time_diff)
                g_forces.append(g_force)
        
        if g_forces:
            stats["g_force"] = {
                "min": min(g_forces),
                "max": max(g_forces),
                "avg": np.mean(g_forces)
            }
    
    return stats


def estimate_distance(data: List[Dict[str, float]]) -> float:
    """Estimate distance traveled in feet."""
    distance = 0
    for i in range(1, len(data)):
        # Use average groundspeed
        avg_speed = (data[i].get("groundspeed_kts", 0) + 
                    data[i-1].get("groundspeed_kts", 0)) / 2
        time_diff = data[i]["time_seconds"] - data[i-1]["time_seconds"]
        
        # Convert to feet
        distance += avg_speed * 1.68781 * time_diff
    
    return distance


def estimate_total_distance(data: List[Dict[str, float]]) -> float:
    """Estimate total distance in nautical miles using lat/lon."""
    distance_nm = 0
    
    for i in range(1, len(data)):
        if all(key in data[i] for key in ["latitude_deg", "longitude_deg"]):
            pos1 = (data[i-1]["latitude_deg"], data[i-1]["longitude_deg"])
            pos2 = (data[i]["latitude_deg"], data[i]["longitude_deg"])
            
            try:
                distance_nm += distance(pos1, pos2).nm
            except:
                pass
    
    return distance_nm


def calculate_glide_angle(approach_data: List[Dict[str, float]]) -> float:
    """Calculate glide angle in degrees."""
    if len(approach_data) < 2:
        return 0
    
    # Use first and last points
    alt_diff = approach_data[0]["altitude_ft"] - approach_data[-1]["altitude_ft"]
    
    # Estimate horizontal distance
    distance_ft = estimate_distance(approach_data)
    
    if distance_ft > 0:
        # Calculate angle
        angle_rad = np.arctan(alt_diff / distance_ft)
        return np.degrees(angle_rad)
    
    return 0 