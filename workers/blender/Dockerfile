FROM platformq/compute-worker-base:latest

# Install Blender dependencies
RUN apt-get update && apt-get install -y \
    wget \
    xvfb \
    libxxf86vm1 \
    libxfixes3 \
    libxrender1 \
    libgl1-mesa-glx \
    libxi6 \
    libxkbcommon0 \
    libsm6 \
    && rm -rf /var/lib/apt/lists/*

# Install Blender (latest LTS version)
RUN wget https://download.blender.org/release/Blender3.6/blender-3.6.5-linux-x64.tar.xz && \
    tar -xf blender-3.6.5-linux-x64.tar.xz && \
    mv blender-3.6.5-linux-x64 /opt/blender && \
    rm blender-3.6.5-linux-x64.tar.xz

# Set Blender path
ENV PATH="/opt/blender:${PATH}"
ENV BLENDER_PATH="/opt/blender"

# Install Python packages for Blender
RUN /opt/blender/3.6/python/bin/python3.10 -m pip install \
    numpy==1.24.3 \
    pillow==10.0.0

# Copy worker implementation
COPY blender_worker.py /app/
COPY blender_utils.py /app/
COPY render_scripts/ /app/render_scripts/

# Set working directory
WORKDIR /app

# Use Xvfb for headless rendering
CMD ["python", "/app/blender_worker.py"] 