FROM platformq/compute-worker-base:latest

# Install FreeCAD and dependencies
RUN apt-get update && apt-get install -y \
    software-properties-common \
    wget \
    libgl1-mesa-glx \
    libglu1-mesa \
    libxmu6 \
    libxi6 \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    libxft2 \
    libxt6 \
    libgomp1 \
    && add-apt-repository ppa:freecad-maintainers/freecad-stable \
    && apt-get update \
    && apt-get install -y freecad freecad-python3 \
    && rm -rf /var/lib/apt/lists/*

# Install FEM and analysis packages
RUN apt-get update && apt-get install -y \
    calculix-ccx \
    gmsh \
    netgen \
    && rm -rf /var/lib/apt/lists/*

# Install additional Python packages for FreeCAD
RUN pip install --no-cache-dir \
    numpy==1.26.2 \
    scipy==1.11.4 \
    matplotlib==3.8.2 \
    vtk==9.3.0 \
    meshio==5.3.4 \
    pyvista==0.42.3

# Copy worker implementation
COPY freecad_worker.py /app/
COPY freecad_utils.py /app/
COPY analysis_templates/ /app/analysis_templates/

# Create data directories
RUN mkdir -p /app/data/models /app/data/meshes /app/data/results

# Set FreeCAD environment
ENV FREECAD_USER_HOME=/app/.FreeCAD
ENV PYTHONPATH="/usr/lib/freecad/lib:${PYTHONPATH}"

# Set working directory
WORKDIR /app

# Run FreeCAD worker
CMD ["python", "/app/freecad_worker.py"] 