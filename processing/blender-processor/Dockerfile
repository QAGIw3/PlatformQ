# Use a standard base image
FROM ubuntu:22.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies for Blender
RUN apt-get update && apt-get install -y \
    wget \
    bzip2 \
    libxrender1 \
    libx11-6 \
    libxi6 \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

# Download and extract Blender
# Using a specific, known version for reproducibility
ARG BLENDER_VERSION=3.4.1
ARG BLENDER_URL=https://download.blender.org/release/Blender3.4/blender-${BLENDER_VERSION}-linux-x64.tar.xz

RUN wget ${BLENDER_URL} -O /tmp/blender.tar.xz && \
    mkdir /opt/blender && \
    tar -xf /tmp/blender.tar.xz -C /opt/blender --strip-components=1 && \
    rm /tmp/blender.tar.xz

# Add Blender to the PATH
ENV PATH="/opt/blender:${PATH}"

# Copy the extraction script into the container
WORKDIR /app
COPY extract_metadata.py .

# The entrypoint will be the blender command itself, running in background mode
ENTRYPOINT ["blender", "--background", "--python"] 