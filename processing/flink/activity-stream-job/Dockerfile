# Use the official Flink image
FROM flink:1.17.1-scala_2.12-java11

# Add the Pulsar and S3 connectors
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-pulsar/1.17.1/flink-sql-connector-pulsar-1.17.1.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.1/flink-s3-fs-hadoop-1.17.1.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-gelly/1.17.1/flink-gelly_2.12-1.17.1.jar

# Switch to a non-root user
USER flink

# Copy our job files
WORKDIR /app
COPY ./requirements.txt /app/
RUN pip install -r requirements.txt
COPY ./src /app/src

# Set the entrypoint
CMD ["flink", "run", "-py", "src/main.py"] 