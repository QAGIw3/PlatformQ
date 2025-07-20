# Provisioning Services Refactoring

## Overview

The original monolithic provisioning service has been refactored into six focused microservices, each handling a specific domain of responsibility. This follows the single responsibility principle and enables better scalability, maintainability, and deployment flexibility.

## Service Architecture

### 1. Tenant Provisioning Service (Port 8001)
**Primary Responsibility**: Orchestrates the provisioning of all infrastructure resources for tenants.

**Key Features**:
- Orchestrates provisioning across 11+ infrastructure systems
- Handles rollback on failure
- Supports parallel provisioning for faster deployment
- Integrates with Vault for secrets and Consul for configuration

**API Endpoints**:
- `POST /api/v1/tenants/provision` - Provision resources for new tenant
- `DELETE /api/v1/tenants/{tenant_id}/deprovision` - Deprovision tenant resources
- `GET /api/v1/provisioning/{request_id}` - Get provisioning status
- `POST /api/v1/provisioning/{request_id}/retry` - Retry failed provisioning

### 2. Resource Monitoring Service (Port 8002)
**Primary Responsibility**: Monitors resource usage across all services and infrastructure.

**Key Features**:
- Real-time metrics collection from Prometheus and Kubernetes
- Anomaly detection (CPU, memory, error rates, response times)
- Historical metrics storage in Apache Ignite
- Publishes anomaly events for auto-scaling

**API Endpoints**:
- `GET /api/v1/metrics/service/{service_name}` - Get service metrics
- `GET /api/v1/metrics/cluster` - Get cluster-wide metrics
- `GET /api/v1/metrics/service/{service_name}/history` - Get historical metrics
- `GET /api/v1/anomalies` - Get current anomalies

### 3. Resource Scaling Service (Port 8003)
**Primary Responsibility**: Provides auto-scaling capabilities with predictive scaling.

**Key Features**:
- Horizontal pod auto-scaling
- Vertical scaling support (future)
- Predictive scaling using ML models
- Cost-aware scaling decisions
- Cooldown management

**API Endpoints**:
- `GET /api/v1/policies/{service_name}` - Get scaling policy
- `PUT /api/v1/policies/{service_name}` - Update scaling policy
- `GET /api/v1/decisions` - Get recent scaling decisions
- `GET /api/v1/predictions/{service_name}` - Get load predictions

### 4. Cost Optimization Service (Port 8004)
**Primary Responsibility**: Analyzes costs and provides optimization recommendations.

**Key Features**:
- Cost analysis per service/tenant
- Optimization recommendations (downsizing, spot instances, etc.)
- Cost prediction
- Budget monitoring

**API Endpoints**:
- `GET /api/v1/analysis/{service_name}` - Analyze service costs
- `GET /api/v1/recommendations` - Get optimization recommendations
- `GET /api/v1/reports` - Generate cost reports

### 5. Quota Management Service (Port 8005)
**Primary Responsibility**: Manages resource quotas and usage tracking for tenants.

**Key Features**:
- Tenant quota management by tier
- Real-time usage tracking
- Burst capacity support
- Fair-share resource allocation

**API Endpoints**:
- `GET /api/v1/quotas/{tenant_id}` - Get tenant quota
- `GET /api/v1/usage/{tenant_id}` - Get current usage
- `POST /api/v1/allocations` - Allocate resources

### 6. Compute Provisioning Service (Port 8000)
**Primary Responsibility**: Handles compute resource provisioning across cloud providers.

**Key Features**:
- Multi-cloud support (AWS, Azure, GCP, CloudStack, Kubernetes)
- Spot instance management
- GPU resource allocation
- Integration with derivatives pricing

**API Endpoints**:
- `POST /api/v1/compute/provision` - Provision compute resources
- `GET /api/v1/compute/provision/{allocation_id}` - Get provisioning status
- `DELETE /api/v1/compute/provision/{allocation_id}` - Terminate resources
- `GET /api/v1/compute/capacity` - Get available capacity

## Communication Patterns

### Event-Driven Communication
- **Resource Anomaly Events**: Published by Resource Monitoring Service, consumed by Resource Scaling Service
- **Tenant Lifecycle Events**: Consumed by Tenant Provisioning Service for automated provisioning
- **Scaling Decision Events**: Published by Resource Scaling Service for audit trail

### Synchronous Communication
- Resource Scaling Service → Resource Monitoring Service (get metrics)
- Cost Optimization Service → Resource Monitoring Service (get usage data)
- Tenant Provisioning Service → All infrastructure services (provisioning calls)

## Shared Libraries

### platformq-provisioning-common
- Models: ProvisioningRequest, ProvisioningResult, ResourceType, etc.
- Interfaces: IResourceProvisioner, IProvisioningOrchestrator
- Utilities: Resource naming, validation

### platformq-resource-common
- Models: ResourceMetrics, ScalingDecision, ResourceQuota, etc.
- Interfaces: IResourceMonitor, IScalingEngine, IQuotaManager
- Utilities: Metric calculations

### platformq-cost-common
- Models: CostAnalysis, CostRecommendation, ResourcePricing, etc.
- Interfaces: ICostCalculator, ICostOptimizer
- Utilities: Cost calculations, formatting

## Deployment

### Docker Compose
```bash
docker-compose -f docker-compose.provisioning.yml up -d
```

### Kubernetes
Each service has its own Helm chart in `iac/kubernetes/charts/`.

### Service Dependencies
1. Infrastructure services must be running: Cassandra, MinIO, Pulsar, Ignite, Consul, Vault
2. Resource Monitoring Service should start before Resource Scaling Service
3. Tenant Provisioning Service requires all infrastructure services

## Migration Guide

### For API Consumers
1. Tenant provisioning endpoints moved to `tenant-provisioning-service:8001`
2. Metrics endpoints moved to `resource-monitoring-service:8002`
3. Scaling policy management moved to `resource-scaling-service:8003`
4. Compute provisioning remains at `provisioning-service:8000`

### For Developers
1. Import shared libraries instead of duplicating models
2. Use service discovery via Consul for inter-service communication
3. Publish events to Pulsar for async communication
4. Store transient data in Ignite caches

## Benefits of Refactoring

1. **Scalability**: Each service can be scaled independently
2. **Maintainability**: Smaller, focused codebases are easier to understand
3. **Reliability**: Failure isolation - one service failure doesn't affect others
4. **Development Velocity**: Teams can work on services independently
5. **Technology Flexibility**: Services can use different technologies if needed
6. **Deployment Flexibility**: Services can be deployed/updated independently

## Future Enhancements

1. **Vertical Scaling**: Implement vertical pod autoscaling in Resource Scaling Service
2. **Advanced ML Models**: Improve predictive scaling with time-series forecasting
3. **Multi-Region Support**: Extend provisioning to support cross-region deployments
4. **Cost Anomaly Detection**: Add ML-based cost anomaly detection
5. **Capacity Planning**: Add capacity planning features to Quota Management Service 