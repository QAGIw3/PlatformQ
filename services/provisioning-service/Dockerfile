# Use an official Python runtime as a parent image
FROM python:3.11-slim

WORKDIR /app

# Copy requirements and shared library
COPY ./requirements.txt /app/
COPY ../shared /app/shared

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and schemas
COPY ./app /app/app
COPY ../../schemas /app/schemas

# Set the entrypoint
CMD ["python", "app/main.py"] 