# Architectural Decision Record (ADR)

This document records the key architectural decisions made during the development of platformQ.

---

### ADR-001: Microservices over Monolith

- **Decision**: The platform is built as a distributed system of microservices rather than a single monolithic application.
- **Context**: The platform is designed to be a large, multi-functional ecosystem with many independent domains (identity, projects, documents, analytics, etc.).
- **Consequences**:
    - **Pro**: Enables independent development, deployment, and scaling of each service. Teams can work autonomously.
    - **Pro**: Allows for polyglot persistence and technology choices; we can use the best tool for each job (e.g., Cassandra for high-throughput writes, JanusGraph for connected data).
    - **Con**: Introduces significant operational complexity (service discovery, observability, distributed tracing), which required the integration of tools like Istio, Consul, and Jaeger.

---

### ADR-002: Event-Driven over Request/Reply

- **Decision**: Asynchronous, event-driven communication (via Apache Pulsar) is the primary method for inter-service communication, favored over direct synchronous API calls.
- **Context**: Many workflows are not instantaneous and can tolerate eventual consistency. We need a system that is resilient to the failure of individual services.
- **Consequences**:
    - **Pro**: Creates a highly decoupled system. The `auth-service` does not need to know which services care about a `UserCreated` event.
    - **Pro**: Improves resilience. If the `notification-service` is down, user signups are not blocked; the events simply queue in Pulsar until it comes back online.
    - **Con**: Requires careful management of event schemas (leading to the adoption of a Schema Registry) and makes end-to-end reasoning about a workflow more complex.

---

### ADR-003: Centralized Identity (OIDC Provider)

- **Decision**: All authentication and user identity are managed by a single, central `auth-service` which acts as an OpenID Connect (OIDC) provider.
- **Context**: With many integrated applications (Nextcloud, OpenProject, Superset), managing separate user databases for each would be insecure and unmanageable.
- **Consequences**:
    - **Pro**: Enables Single Sign-On (SSO) across the entire platform. Users log in once.
    - **Pro**: Centralizes security policy. All rules about who can log in (e.g., 2FA, SIWE) are managed in one place.
    - **Con**: The `auth-service` becomes a critical, tier-0 service. Its availability is paramount for the entire platform to function.

---

### ADR-004: Service Mesh (Istio) over individual clients

- **Decision**: All service-to-service traffic is managed by a dedicated service mesh layer (Istio) rather than relying on smart clients with libraries for retry logic, timeouts, and mTLS.
- **Context**: In a complex microservices environment, ensuring secure, reliable communication is paramount. Implementing this logic in every service would be repetitive, error-prone, and inconsistent.
- **Consequences**:
    - **Pro**: **Transparent mTLS.** All traffic between services is automatically encrypted without any application code changes, creating a zero-trust network.
    - **Pro**: **Advanced Traffic Management.** Enables powerful, declarative control over traffic for canary releases, A/B testing, and fault injection without redeploying services.
    - **Pro**: **Uniform Observability.** The Istio sidecars provide consistent, high-quality metrics, logs, and traces for all network traffic, which is a huge boon for debugging.
    - **Con**: Adds a new layer of infrastructure to manage and debug. It has a steep learning curve and adds some resource overhead to each pod via the sidecar proxy.

---

### ADR-005: GitOps (GitHub Actions & Helm) over manual deployment

- **Decision**: All deployments and infrastructure changes are managed through a version-controlled Git repository and applied via an automated CI/CD pipeline (GitHub Actions) using Helm.
- **Context**: Manual deployments (`kubectl apply -f ...`) are not repeatable, auditable, or scalable. We need a single source of truth for the desired state of our platform.
- **Why**: It provides a complete, auditable history of every change made to the platform. It enforces a peer-review process (via Pull Requests) for all infrastructure and application changes, improving quality and stability.
- **Consequences**:
    - **Pro**: **Declarative & Auditable.** The state of our production environment is defined entirely by the code in our `main` branch.
    - **Pro**: **Automated & Repeatable.** Eliminates manual deployment errors and makes deploying new services or updating existing ones a fast, reliable process.
    - **Con**: The CI/CD pipeline itself becomes a critical piece of infrastructure that must be maintained and secured. Initial setup is complex.

---

### ADR-006: Lakehouse Architecture (Cassandra + Minio/Parquet) over a single database

- **Decision**: We employ a two-tiered data storage strategy. A high-performance NoSQL database (Cassandra) acts as the "hot" serving layer for real-time APIs, while a low-cost object store (Minio) with an open data format (Parquet) acts as the "cold" data lake for long-term archival and deep analytics.
- **Context**: A single database cannot efficiently serve both low-latency operational queries and high-throughput analytical queries. Trying to do so results in compromises on both performance and cost.
- **Consequences**:
    - **Pro**: **Optimized for Use Case.** Cassandra provides the low-latency reads and writes needed for our live services. The data lake provides cost-effective, infinite storage for historical data.
    - **Pro**: **Decoupled Analytics.** Heavy, long-running analytical queries (via Trino) run against the data lake, preventing them from impacting the performance of the production serving layer.
    - **Con**: Introduces complexity via an ETL/ELT pipeline (our Flink job) that must be maintained to move and transform data between the layers.

---

### ADR-007: Centralized "Golden Path" Tooling (`platformqctl`) over convention alone

- **Decision**: We created a dedicated command-line tool (`platformqctl`) and a service template to automate the creation of new microservices.
- **Context**: As an engineering organization scales, simply documenting conventions is not enough to ensure consistency and quality. Developer velocity can be slowed by the cognitive overhead of setting up a new service correctly.
- **Consequences**:
    - **Pro**: **High Developer Velocity.** A new, production-ready, best-practice service can be scaffolded in seconds.
    - **Pro**: **Enforced Governance.** All new services automatically adhere to our standards for logging, observability, security, and deployment from their first commit.
    - **Con**: The tooling itself becomes a product that must be maintained and versioned. It creates a strong, "opinionated" path that might be less suitable for highly experimental services that need to break convention. 