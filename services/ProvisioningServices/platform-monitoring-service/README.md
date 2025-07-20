# Platform Monitoring Service

Multi-region Prometheus federation service with Thanos integration for long-term storage and global querying of Platform Q metrics.

## Overview

The Platform Monitoring Service provides:
- Multi-region Prometheus federation
- Long-term metric storage via Thanos
- Tenant-isolated metric querying
- Service discovery across regions
- Alert rule management
- Cost estimation based on resource usage

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Region US-E1   │     │  Region EU-W1   │     │  Region AP-SE1  │
│  ┌───────────┐  │     │  ┌───────────┐  │     │  ┌───────────┐  │
│  │Prometheus │  │     │  │Prometheus │  │     │  │Prometheus │  │
│  └─────┬─────┘  │     │  └─────┬─────┘  │     │  └─────┬─────┘  │
│        │        │     │        │        │     │        │        │
│  ┌─────┴─────┐  │     │  ┌─────┴─────┐  │     │  ┌─────┴─────┐  │
│  │  Thanos   │  │     │  │  Thanos   │  │     │  │  Thanos   │  │
│  │  Sidecar  │  │     │  │  Sidecar  │  │     │  │  Sidecar  │  │
│  └─────┬─────┘  │     │  └─────┬─────┘  │     │  └─────┬─────┘  │
└────────┼────────┘     └────────┼────────┘     └────────┼────────┘
         │                       │                       │
         └───────────────────────┴───────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
              ┌─────┴─────┐           ┌───────┴───────┐
              │  Thanos   │           │    MinIO      │
              │   Query   │           │ Object Store  │
              └─────┬─────┘           └───────────────┘
                    │
         ┌──────────┴──────────┐
         │ Platform Monitoring │
         │      Service        │
         └─────────────────────┘
```

## Features

### Multi-Region Federation
- Automatic discovery of regional Prometheus instances
- Federated queries across all regions
- Region-aware service discovery via Consul

### Long-Term Storage
- Thanos integration for unlimited retention
- Multi-resolution compaction (raw, 5m, 1h)
- MinIO object storage backend

### Tenant Isolation
- Metric filtering by tenant_id label
- Per-tenant resource usage tracking
- Cost estimation per tenant

### Service Monitoring
- Platform Q service-specific metrics
- SLO tracking (availability, latency, errors)
- Resource utilization per service

## API Endpoints

### Region Management
- `GET /api/v1/regions` - List all regions
- `POST /api/v1/regions/{region_id}` - Register a region
- `DELETE /api/v1/regions/{region_id}` - Unregister a region
- `GET /api/v1/federation/status` - Federation status

### Metrics Queries
- `POST /api/v1/query` - Execute instant query
- `POST /api/v1/query_range` - Execute range query
- `GET /api/v1/tenants/{tenant_id}/metrics` - Get tenant metrics

### Alert Management
- `POST /api/v1/alerts/rules` - Create alert rule
- `GET /api/v1/alerts/rules` - List alert rules

### Service Discovery
- `GET /api/v1/service-discovery/{service_name}` - Discover service endpoints

## Configuration

Environment variables:
```bash
# Thanos endpoints
THANOS_QUERY_URL=http://thanos-query:10902
THANOS_STORE_URL=http://thanos-store:10901
THANOS_COMPACT_URL=http://thanos-compact:10902

# MinIO configuration
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=thanos-metrics

# Consul configuration
CONSUL_URL=http://consul:8500
CONSUL_TOKEN=<token>

# Regions
REGIONS=["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
```

## Deployment

### Docker
```bash
docker build -t platform-monitoring-service .
docker run -p 9090:9090 --env-file .env platform-monitoring-service
```

### Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: platform-monitoring-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: platform-monitoring-service
  template:
    metadata:
      labels:
        app: platform-monitoring-service
    spec:
      containers:
      - name: monitoring
        image: platform-monitoring-service:latest
        ports:
        - containerPort: 9090
        env:
        - name: THANOS_QUERY_URL
          value: "http://thanos-query:10902"
        # ... other env vars
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:
- `federation_sync_total` - Federation sync operations
- `federation_regions_active` - Active regions count
- `thanos_queries_total` - Thanos query count
- `metrics_aggregation_duration_seconds` - Aggregation latency

## Development

### Setup
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Testing
```bash
pytest tests/
pytest --cov=app tests/
```

### Code Quality
```bash
black app/
mypy app/
ruff check app/
```

## Grafana Dashboards

Pre-built dashboards available in `dashboards/`:
- `platform-overview.json` - Multi-region platform overview
- `tenant-metrics.json` - Per-tenant resource usage
- `service-slos.json` - Service SLO tracking

## Troubleshooting

### Region Not Appearing
1. Check Prometheus health in the region
2. Verify Thanos sidecar is running
3. Check network connectivity
4. Review federation manager logs

### Query Timeouts
1. Reduce query time range
2. Add more specific label selectors
3. Check Thanos Query performance
4. Verify object storage connectivity

### Missing Metrics
1. Verify metric labels include tenant_id
2. Check scrape configuration
3. Ensure targets are healthy
4. Review service discovery 