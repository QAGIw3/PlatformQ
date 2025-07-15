import sys
import json
import os
import re
from platformq_shared.processor_utils import run_processor

def parse_control_dict(filepath):
    """
    Parses an OpenFOAM controlDict file to extract key simulation parameters.
    This is a simple parser and does not handle complex nested dictionaries.
    """
    params = {}
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('//') or line == '':
                    continue
                
                # Simple key-value parsing
                match = re.match(r'(\w+)\s+(.*?);', line)
                if match:
                    key, value = match.groups()
                    params[key] = value.strip()
        return params


def extract_openfoam_metadata(case_dir):
    """
    Extracts metadata from an OpenFOAM case directory.
    """
    control_dict_path = os.path.join(case_dir, "system", "controlDict")
    
    if not os.path.exists(control_dict_path):
        raise FileNotFoundError("system/controlDict not found in the provided case directory.")

        control_dict_data = parse_control_dict(control_dict_path)
        
        metadata = {
            "openfoam_case": {
                "application": control_dict_data.get("application"),
                "startTime": control_dict_data.get("startTime"),
                "endTime": control_dict_data.get("endTime"),
                "deltaT": control_dict_data.get("deltaT"),
                "writeControl": control_dict_data.get("writeControl"),
            }
        }
        
    return metadata

if __name__ == '__main__':
        # The input is expected to be a directory containing the case files
    run_processor(extract_openfoam_metadata) 