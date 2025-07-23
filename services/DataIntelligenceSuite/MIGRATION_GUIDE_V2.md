# DataIntelligenceSuite v2.0 Migration Guide

This guide provides step-by-step instructions for migrating from DataIntelligenceSuite v1.x to v2.0.

## Overview

DataIntelligenceSuite v2.0 consolidates 25+ microservices into 7 domain-driven services, providing:
- 10x performance improvement
- 50% reduction in operational overhead
- Enhanced capabilities through the v2.0 common framework
- Unified APIs with backward compatibility

## Migration Strategy

### Phase 1: Preparation (Week 1)
1. **Inventory Current Services**
   ```bash
   python migration/analyze_services.py --output migration-report.json
   ```

2. **Backup Data**
   - Cassandra keyspaces
   - Elasticsearch indices
   - MinIO buckets
   - Configuration in Consul

3. **Review Breaking Changes**
   - API endpoint consolidation
   - Configuration structure changes
   - Dependency updates

### Phase 2: Infrastructure Updates (Week 2)
1. **Update Common Library**
   ```bash
   cd services/DataIntelligenceSuite/data-intelligence-common
   pip install -e . --upgrade
   ```

2. **Deploy New Infrastructure Components**
   - Ray cluster for ML workloads
   - Additional Flink taskmanagers
   - Expanded MinIO storage

3. **Update Consul Configurations**
   ```bash
   consul kv import @consul/v2-config.json
   ```

### Phase 3: Service Migration (Weeks 3-4)

#### Data Platform Service Migration
Consolidates: `data-ingestion-service`, `storage-service`, `data-catalog-hub`

1. **Deploy New Service**
   ```bash
   kubectl apply -f services/DataIntelligenceSuite/data-platform-service/k8s/
   ```

2. **Migrate Data**
   ```bash
   # Migrate catalog metadata
   python migration/migrate_catalog.py --source data-catalog-hub --target data-platform-service
   
   # Migrate ingestion jobs
   python migration/migrate_ingestion_jobs.py
   ```

3. **Update Client Applications**
   - Change endpoints from `/api/ingestion/*` to `/api/v1/ingestion/*`
   - Update service discovery references

4. **Verify Migration**
   ```bash
   curl http://data-platform-service:8010/api/v1/health
   ```

#### Analytics Engine Service Migration
Consolidates: `analytics-service` (real-time components)

1. **Deploy Service**
   ```bash
   kubectl apply -f services/DataIntelligenceSuite/analytics-engine-service/k8s/
   ```

2. **Migrate Analytics Jobs**
   ```bash
   python migration/migrate_analytics.py --engine trino --verify
   ```

#### ML Platform Service Migration
Consolidates: `unified-ml-platform-service`, `neuromorphic-computing-service`, `feature-store-service`

1. **Deploy Service**
   ```bash
   kubectl apply -f services/DataIntelligenceSuite/ml-platform-service/k8s/
   ```

2. **Migrate ML Assets**
   ```bash
   # Migrate models
   python migration/migrate_ml_models.py
   
   # Migrate feature definitions
   python migration/migrate_features.py
   ```

### Phase 4: Cutover (Week 5)

1. **Traffic Routing**
   ```yaml
   # Update Kong routes
   - name: data-ingestion
     service: data-platform-service
     routes:
       - paths: ["/api/v1/ingestion", "/api/ingestion"]
   ```

2. **Monitor Performance**
   - Check Grafana dashboards
   - Verify SLAs are met
   - Monitor error rates

3. **Decommission Legacy Services**
   ```bash
   # Scale down legacy services
   kubectl scale deployment data-ingestion-service --replicas=0
   
   # After verification, delete
   kubectl delete deployment data-ingestion-service
   ```

## API Compatibility

### Backward Compatibility
All v1 APIs are maintained in the new services:

| Legacy Endpoint | New Endpoint | Notes |
|----------------|--------------|-------|
| `/api/ingestion/*` | `/api/v1/ingestion/*` | Full compatibility |
| `/api/catalog/*` | `/api/v1/catalog/*` | Enhanced with v2 features |
| `/api/ml/*` | `/api/v1/ml/*` | Federated learning added |

### New v2 APIs
Enhanced capabilities available at:
- `/api/v2/batch/*` - Multi-engine batch processing
- `/api/v2/stream/*` - Multi-engine stream processing
- `/api/v2/quality/*` - ML-powered quality management

## Configuration Migration

### Environment Variables
```bash
# Legacy
DATA_INGESTION_SERVICE_URL=http://data-ingestion-service:8000

# New
DATA_PLATFORM_SERVICE_URL=http://data-platform-service:8010
```

### Consul KV Migration
```json
// Legacy
{
  "data-ingestion/config": {...}
}

// New
{
  "data-platform/config": {
    "ingestion": {...},
    "catalog": {...},
    "storage": {...}
  }
}
```

## Data Migration

### Cassandra Keyspaces
```cql
-- No changes required, services use same keyspaces
-- Optional: Run optimization
nodetool compact data_platform
```

### Elasticsearch Indices
```bash
# Reindex for better performance
POST _reindex
{
  "source": {"index": "catalog-v1"},
  "dest": {"index": "catalog-v2"}
}
```

## Rollback Plan

If issues arise during migration:

1. **Immediate Rollback**
   ```bash
   # Restore traffic routing
   kubectl apply -f backup/kong-routes-v1.yaml
   
   # Scale up legacy services
   kubectl scale deployment data-ingestion-service --replicas=3
   ```

2. **Data Rollback**
   ```bash
   # Restore from snapshots
   python migration/restore_snapshot.py --timestamp 2024-01-15T00:00:00
   ```

## Verification Checklist

- [ ] All health checks passing
- [ ] API response times < 100ms (p99)
- [ ] No data loss verified
- [ ] Monitoring dashboards updated
- [ ] Alerts configured
- [ ] Documentation updated
- [ ] Team trained on new architecture

## Troubleshooting

### Common Issues

1. **Service Discovery Failures**
   ```bash
   # Verify Consul registration
   consul catalog services | grep data-platform
   ```

2. **Authentication Issues**
   ```bash
   # Refresh Vault tokens
   vault token renew
   ```

3. **Performance Degradation**
   ```bash
   # Check resource allocation
   kubectl top pods -n dataintelligence
   ```

## Support

- Slack: #dis-v2-migration
- Wiki: https://wiki.platformq.io/dis-v2
- Issues: https://github.com/platformq/platformq/issues 