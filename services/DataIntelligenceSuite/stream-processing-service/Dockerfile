FROM flink:1.17-java11

# Install Python for PyFlink jobs
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/flink/app

# Copy requirements
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy job modules
COPY app/ ./app/
COPY jobs/ ./jobs/

# Copy configuration
COPY conf/flink-conf.yaml /opt/flink/conf/

# Create non-root user
RUN useradd -m -u 1000 flink-user && \
    chown -R flink-user:flink-user /opt/flink

USER flink-user

# Set environment variables
ENV PYTHONPATH=/opt/flink/app:$PYTHONPATH
ENV SERVICE_NAME=stream-processing-service

# Expose ports
EXPOSE 8000 8081 6123

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start both Flink and the service
CMD ["sh", "-c", "/opt/flink/bin/jobmanager.sh start && python3 -m app.main"] 