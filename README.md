# platformQ

This repository contains the source code for platformQ, a multiservice platform for complex data processing and analysis.

## Architecture

The platform is built on a microservices architecture designed for high-throughput, real-time data processing. For a detailed overview of the components and their interactions, please refer to the architecture diagrams generated during the initial design phase.

The core technologies include:
- **API/Services**: Microservices offering APIs for clients.
- **Messaging**: Apache Pulsar
- **Real-time Processing**: Apache Flink
- **Batch Processing**: Apache Spark
- **Orchestration**: Apache Airflow
- **Storage**: Apache Cassandra (NoSQL), JanusGraph (Graph), Minio (Object Store), Apache Ignite (In-Memory)
- **Querying**: Trino
- **Observability**: OpenTelemetry, Jaeger (Tracing), Prometheus (Metrics)

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Launching the Development Environment

All core infrastructure services are managed via Docker Compose. To start the environment, run the following command from the root of the project:

```bash
docker-compose -f infra/docker-compose/docker-compose.yml up -d
```

This will launch all the services in the background. You can view the status of the containers with `docker-compose -f infra/docker-compose/docker-compose.yml ps`.

### Accessing Services

Once the environment is running, you can access the various UIs and endpoints:

- **Pulsar Manager**: http://localhost:8080
- **Trino UI**: http://localhost:8081
- **Minio Console**: http://localhost:9001 (User: `minioadmin`, Pass: `minioadmin`)
- **Prometheus**: http://localhost:9090
- **Jaeger UI**: http://localhost:16686
- **Cassandra (CQL)**: `localhost:9042`
- **JanusGraph (Gremlin)**: `localhost:8182`

To shut down the environment, run:

```bash
docker-compose -f infra/docker-compose/docker-compose.yml down
``` 