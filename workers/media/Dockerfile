FROM platformq/compute-worker-base:latest

# Install media processing dependencies
RUN apt-get update && apt-get install -y \
    # GIMP dependencies
    gimp \
    gimp-python \
    # Audio/Video dependencies
    ffmpeg \
    libavcodec-extra \
    libavformat58 \
    libswscale5 \
    # Audacity dependencies (command line tools)
    sox \
    libsox-fmt-all \
    lame \
    flac \
    vorbis-tools \
    # OpenShot dependencies
    python3-openshot \
    libopenshot17 \
    libopenshot-audio7 \
    # Image processing
    imagemagick \
    libmagickwand-dev \
    # Additional tools
    mediainfo \
    exiftool \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages for media processing
RUN pip install --no-cache-dir \
    Pillow==10.1.0 \
    opencv-python==4.8.1.78 \
    moviepy==1.0.3 \
    pydub==0.25.1 \
    librosa==0.10.1 \
    soundfile==0.12.1 \
    scikit-image==0.22.0 \
    wand==0.6.13 \
    ffmpeg-python==0.2.0

# Copy worker implementation
COPY media_worker.py /app/
COPY media_utils.py /app/
COPY processing_scripts/ /app/processing_scripts/

# Create data directories
RUN mkdir -p /app/data/input /app/data/output /app/data/temp

# Set working directory
WORKDIR /app

# Run media worker
CMD ["python", "/app/media_worker.py"] 