# Flink Job: Unified Activity Stream

This Flink job is a core component of the platform's intelligence layer. It consumes raw event streams from multiple services and transforms them into a single, unified, and queryable `activity_stream`.

## Data Pipeline

1.  **Sources**: This job subscribes to multiple Pulsar topic patterns:
    - `persistent://platformq/.*/user-events`
    - `persistent://platformq/.*/document-events`
    - `persistent://platformq/.*/project-events`
    (etc.)

2.  **Transformation**:
    - It uses the Pulsar Schema Registry to automatically deserialize various Avro event schemas.
    - It transforms each raw event into a standardized `ActivityEvent` format.
    - It extracts the `tenant_id` from the source topic name to preserve data partitioning.

3.  **Sinks**: The job performs a "dual sink" to two destinations:
    - **Cassandra (Hot Serving Layer)**: The unified events are written to the `activity_stream` table for fast, real-time queries by the `analytics-service`.
    - **Minio (Data Lake / Cold Storage)**: The raw events are also archived to a Minio bucket in Parquet format for long-term storage and deep, historical analysis with Trino.

## Purpose

This job is the foundation for several key platform features, including:
- The "Mission Control" analytics dashboards.
- The "Proactive Intelligence" and notification services.
- The "Generative BI" and conversational data exploration tools. 