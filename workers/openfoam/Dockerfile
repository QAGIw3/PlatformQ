FROM platformq/compute-worker-base:latest

# Install OpenFOAM dependencies
RUN apt-get update && apt-get install -y \
    wget \
    software-properties-common \
    ca-certificates \
    libopenmpi-dev \
    openmpi-bin \
    libscotch-dev \
    libptscotch-dev \
    libfl-dev \
    && rm -rf /var/lib/apt/lists/*

# Add OpenFOAM repository and install
RUN sh -c "wget -O - https://dl.openfoam.org/gpg.key | apt-key add -" && \
    add-apt-repository http://dl.openfoam.org/ubuntu && \
    apt-get update && \
    apt-get install -y openfoam10 && \
    rm -rf /var/lib/apt/lists/*

# Set OpenFOAM environment
ENV FOAM_INST_DIR=/opt/openfoam10
ENV WM_PROJECT_DIR=/opt/openfoam10
RUN echo "source /opt/openfoam10/etc/bashrc" >> ~/.bashrc

# Install Python OpenFOAM utilities
RUN pip install --no-cache-dir \
    PyFoam==2022.9 \
    matplotlib==3.8.2 \
    vtk==9.3.0

# Copy worker implementation
COPY openfoam_worker.py /app/
COPY openfoam_utils.py /app/

# Set working directory
WORKDIR /app

# Run OpenFOAM worker
CMD ["/bin/bash", "-c", "source /opt/openfoam10/etc/bashrc && python /app/openfoam_worker.py"] 