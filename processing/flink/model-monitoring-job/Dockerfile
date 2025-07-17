# Use the official Flink image
FROM flink:1.17.1-scala_2.12-java11

# Add required connectors and libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-pulsar/1.17.1/flink-sql-connector-pulsar-1.17.1.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/1.17.1/flink-connector-jdbc-1.17.1.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

# Install Python dependencies
USER root
RUN apt-get update && \
    apt-get install -y python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements
WORKDIR /app
COPY requirements.txt /app/

# Install Python packages
RUN pip3 install -r requirements.txt

# Copy job files
COPY ./src /app/src

# Switch back to flink user
USER flink

# Set the entrypoint
CMD ["flink", "run", "-py", "src/main.py"] 