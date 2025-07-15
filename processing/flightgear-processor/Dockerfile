# Use a standard lightweight python image
FROM python:3.9-slim

# Copy the extraction script into the container
WORKDIR /app
COPY extract_metadata.py .

# The entrypoint will execute the python script directly.
ENTRYPOINT ["python", "extract_metadata.py"] 