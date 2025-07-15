# Use a standard base image
FROM ubuntu:22.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies, add the FreeCAD PPA, and install freecad-cmd
RUN apt-get update && \
    apt-get install -y software-properties-common python3-pip && \
    add-apt-repository ppa:freecad-maintainers/freecad-stable && \
    apt-get update && \
    apt-get install -y freecad-cmd && \
    rm -rf /var/lib/apt/lists/*

# Copy the extraction script into the container
WORKDIR /app
COPY extract_metadata.py .

# The entrypoint will execute the python script using the FreeCAD command-line tool
ENTRYPOINT ["freecadcmd", "extract_metadata.py"] 