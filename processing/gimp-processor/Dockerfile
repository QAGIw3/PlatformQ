# Use a standard base image
FROM ubuntu:22.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install GIMP and its Python interpreter
RUN apt-get update && apt-get install -y \
    gimp \
    gimp-python \
    && rm -rf /var/lib/apt/lists/*

# Copy the extraction script into the container
WORKDIR /app
COPY extract_metadata.py .

# The entrypoint will be the gimp-console-batch command, which executes
# a python script within the GIMP environment without launching a GUI.
ENTRYPOINT ["gimp-console-batch", "-i", "-b"] 