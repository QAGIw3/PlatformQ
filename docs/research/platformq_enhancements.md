# PlatformQ – Novel, Feasible Enhancements (2025-2026)

This short research note lists forward-looking capabilities that can be layered onto the existing PlatformQ stack with **minimal disruption** while leveraging the open-source technologies already adopted in the project rules.

---

## 1. Lakehouse 2.0 – Apache Iceberg on MinIO
* **What**   Introduce Iceberg table format to complement existing Parquet buckets, providing ACID transactions, schema & partition evolution, and time-travel on the object store.
* **Why**    Simplifies multi-engine analytics (Spark, Flink, Trino) and unlocks zero-copy reads/writes for ML training pipelines.
* **Feasibility**   Runs completely on current MinIO + Spark/Flink; no extra services. 20-30 % query acceleration and 90 % S3/MinIO listing cost reduction (ref. MinIO blog).
* **Next Steps**   Pilot an Iceberg catalog in `infra/trino/catalog/`, hook Spark & Flink jobs, migrate one data-domain incrementally.

## 2. Unified Feature Store – Ignite + Pulsar
* **What**   Deploy an Ignite-backed online/offline Feature Store fed by Pulsar streams and persisted to Iceberg tables.
* **Why**    Eliminates feature duplication, guarantees training/inference parity, and offers <5 ms feature look-ups for real-time ML services.
* **Feasibility**   Ignite already configured; add Ignite SQL + thin clients; Pulsar Functions to write/update features. No new licenses.
* **Next Steps**   Create PoC with 1-2 ML models using Ignite Map-based feature retrieval; integrate with MLOps service.

## 3. Advanced Event Processing – Flink CEP / Pattern API
* **What**   Use Flink’s Complex Event Processing library to express fraud-detection, anomaly or IoT patterns in SQL-like DSL.
* **Why**    Provides sub-second detection with exactly-once guarantees over Pulsar streams; replaces hand-coded window logic.
* **Feasibility**   Flink cluster already in `processing/flink/`; add CEP dependency and deploy as Jar; no infra impact.
* **Next Steps**   Draft pattern definitions for Risk-Engine and Real-Time Analytics services; benchmark latency vs. existing Spark Streaming.

## 4. Serverless Data Pipelines – Pulsar Functions
* **What**   Adopt Pulsar Functions to run lightweight stateless transforms (filter/enrich/route) directly on the message bus.
* **Why**    Reduces micro-service boilerplate, cuts egress traffic, scales elastically; supports Java, Python, Go.
* **Feasibility**   Comes with Pulsar; enable Function Worker, define IAM-free processing units; 0–150 MB RAM each.
* **Next Steps**   Move simple log-enrichment Lambda equivalents to Pulsar Functions; monitor with built-in metrics.

## 5. Digital Integration Hub (DIH) Powered by Ignite
* **What**   Aggregate hot slices of disparate datastores into an in-memory, API-centric hub for low-latency read/write workloads.
* **Why**    Off-loads OLTP systems, enables omnichannel 360° asset views, and supports ACID transactions across caches.
* **Feasibility**   Ignite already present; leverage CacheStore & CDC connectors; backups remain in Cassandra/JanusGraph.
* **Next Steps**   Identify high-call-rate APIs (e.g., auth-session, pricing) and front them with DIH layer; track hit ratio.

## 6. Zero-ETL Data Ingestion – Apache SeaTunnel
* **What**   Use SeaTunnel to declaratively move/transform data between Pulsar, Elasticsearch, Cassandra, MinIO, and Iceberg without custom code.
* **Why**    Cuts bespoke Python extractors, standardises pipeline configs, and supports CDC & backfill out-of-box.
* **Feasibility**   Ships as standalone jar; runs on Spark or Flink clusters already deployed.
* **Next Steps**   Convert two existing Airflow DAGs into SeaTunnel jobs; measure dev-time reduction.

## 7. Vector Search & RAG – Elasticsearch 8 + JanusGraph
* **What**   Upgrade Elasticsearch to v8 for native ANN/k-NN, combine with JanusGraph relationship hops for hybrid semantic search & Retrieval-Augmented Generation.
* **Why**    Empowers Gen-AI assistants, fraud graph scoring, supply-chain lineage queries.
* **Feasibility**   Elasticsearch cluster exists; enable vector type mappings; index embeddings produced by Spark ML or SentenceTransformers.
* **Next Steps**   Spin PoC indexing 1 M documents; benchmark recall vs. Milvus; expose via Search-Service API.

## 8. Unified Observability & Lineage – OpenTelemetry + Iceberg metadata
* **What**   Emit OTEL spans from Flink/Spark/Pulsar Functions, store lineage & metrics in Elasticsearch/Grafana; map Iceberg snapshot metadata for data-provenance.
* **Why**    End-to-end traceability accelerates RCA and compliance.
* **Feasibility**   OpenTelemetry Collector already in observability stack; add exporters and Iceberg REST catalog hooks.
* **Next Steps**   Instrument one critical pipeline; visualise in Jaeger & Grafana dashboards.

---
### Effort & Impact Matrix (high-level)
| Enhancement | Effort | Impact |
|-------------|--------|--------|
| Iceberg on MinIO | Medium | High |
| Ignite Feature Store | Medium | High |
| Flink CEP | Low | Medium-High |
| Pulsar Functions | Low | Medium |
| Ignite DIH | Medium | Medium |
| SeaTunnel | Low-Medium | Medium |
| Vector Search & RAG | Medium | High |
| Observability & Lineage | Low | Medium |

*Effort: Low (<2 sprints), Medium (2-4 sprints).*

---

### Immediate Recommendations (next quarter)
1. Stand up an **Iceberg test catalog** and migrate one analytics table.
2. **Enable Pulsar Function Worker** and port a simple enrichment use-case.
3. Conduct a **Flink CEP PoC** for fraud detection with historical replay.
4. Start drafting **Ignite Feature Store** schema and ingestion plan.

These steps deliver quick wins, validate technology fit, and create momentum for the larger roadmap.