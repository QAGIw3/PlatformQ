FROM openjdk:11-jre-slim

# Install Python and Spark
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

# Set Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON=python3

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY jobs/ ./jobs/
COPY conf/ ./conf/

# Create non-root user
RUN useradd -m -u 1000 spark-user && \
    chown -R spark-user:spark-user /app /opt/spark

USER spark-user

# Set environment variables
ENV PYTHONPATH=/app:$PYTHONPATH
ENV SERVICE_NAME=batch-processing-service

# Expose ports
EXPOSE 8000 4040 7077

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Start the service
CMD ["python3", "-m", "app.main"] 