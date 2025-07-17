import sys
import json
import csv
from datetime import datetime
from platformq_shared.processor_utils import run_processor

def extract_flightgear_metadata(filepath):
    """
    Parses a FlightGear flight data log (.csv) and extracts metadata.
    Assumes a standard log format with headers.
    """
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        raise ValueError("Log file is empty or invalid.")

    first_point = rows[0]
    last_point = rows[-1]

    # Assumes standard log fields are present
    aircraft = first_point.get('aircraft', 'Unknown')
    start_time = float(first_point.get('sim_time_sec', 0))
    end_time = float(last_point.get('sim_time_sec', 0))
    duration_sec = end_time - start_time

    metadata = {
        "flightgear_log": {
            "aircraft": aircraft,
            "total_data_points": len(rows),
            "duration_seconds": duration_sec,
            "start_coords": {
                "latitude_deg": first_point.get('latitude_deg'),
                "longitude_deg": first_point.get('longitude_deg'),
                "altitude_ft": first_point.get('altitude_ft'),
            },
            "end_coords": {
                "latitude_deg": last_point.get('latitude_deg'),
                "longitude_deg": last_point.get('longitude_deg'),
                "altitude_ft": last_point.get('altitude_ft'),
            },
        }
    }
    
    return metadata

if __name__ == '__main__':
    run_processor(extract_flightgear_metadata) 