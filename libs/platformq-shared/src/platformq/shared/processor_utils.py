# -*- coding: utf-8 -*-
"""
This module provides a standardized runner for metadata extraction processors.
"""

import sys
import json
import os

def run_processor(extraction_function):
    """
    A generic runner for metadata extraction processors.

    This function handles common boilerplate tasks:
    - Parsing the input file path from command-line arguments.
    - Checking if the input file exists.
    - Calling the specific extraction function.
    - Handling exceptions and printing errors to stderr in a standard JSON format.
    - Printing the extracted metadata to stdout in a standard JSON format.

    Args:
        extraction_function (callable): The function that performs the specific
            metadata extraction. It must accept a single argument (the filepath)
            and return a dictionary of the extracted metadata.
    """
    filepath = None
    try:
        # A simple arg parsing logic, which can be improved if needed.
        # It supports both `python script.py -- /path/to/file` (for Blender)
        # and `python script.py /path/to/file` (for others).
        args = sys.argv[sys.argv.index("--") + 1:] if "--" in sys.argv else sys.argv[1:]
        if not args:
            raise ValueError("No input file path provided.")
        filepath = args[0]

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        metadata = extraction_function(filepath)
        print(json.dumps(metadata, indent=4))
        sys.exit(0)

    except (ValueError, IndexError, FileNotFoundError) as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        error_context = f"in processor '{os.path.basename(sys.argv[0])}'"
        error_message = f"Failed to process file '{filepath}' {error_context}: {e}"
        print(json.dumps({"error": error_message}), file=sys.stderr)
        sys.exit(1) 