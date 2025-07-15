# Use the official OpenFOAM development image as the base
FROM openfoam/openfoam11-dev

# Copy the extraction script into a working directory
WORKDIR /app
COPY extract_metadata.py .

# The entrypoint will be the python interpreter.
# The script will be able to use OpenFOAM's Python libraries if needed.
ENTRYPOINT ["python3", "extract_metadata.py"] 