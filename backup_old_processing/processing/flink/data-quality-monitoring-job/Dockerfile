FROM flink:1.17.1-java11

# Install Maven for building the job
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/flink/jobs

# Copy pom.xml and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy source code
COPY src ./src

# Build the job
RUN mvn clean package

# Copy the JAR to the Flink job directory
RUN cp target/data-quality-monitoring-job-*.jar /opt/flink/jobs/data-quality-monitoring.jar

# Set the entrypoint to run the job
ENTRYPOINT ["flink", "run", "-c", "com.platformq.flink.quality.DataQualityMonitoringJob", "/opt/flink/jobs/data-quality-monitoring.jar"] 