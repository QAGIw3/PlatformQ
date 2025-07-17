FROM platformq/compute-worker-base:latest

# Install FlightGear and dependencies
RUN apt-get update && apt-get install -y \
    software-properties-common \
    wget \
    libgl1-mesa-glx \
    libglu1-mesa \
    libopenal1 \
    libxmu6 \
    libxi6 \
    xvfb \
    && add-apt-repository ppa:saiarcot895/flightgear \
    && apt-get update \
    && apt-get install -y flightgear \
    && rm -rf /var/lib/apt/lists/*

# Install additional Python packages for FlightGear control
RUN pip install --no-cache-dir \
    telnetlib3==2.0.4 \
    aiofiles==23.2.1 \
    geopy==2.4.1 \
    shapely==2.0.2

# Copy worker implementation
COPY flightgear_worker.py /app/
COPY flightgear_utils.py /app/
COPY scenarios/ /app/scenarios/
COPY aircraft_configs/ /app/aircraft_configs/

# Create data directories
RUN mkdir -p /app/data/terrain /app/data/weather /app/data/logs

# Set environment variables
ENV FG_ROOT=/usr/share/games/flightgear
ENV FG_SCENERY=/usr/share/games/flightgear/Scenery
ENV FG_AIRCRAFT=/usr/share/games/flightgear/Aircraft

# Set working directory
WORKDIR /app

# Run FlightGear worker
CMD ["python", "/app/flightgear_worker.py"] 