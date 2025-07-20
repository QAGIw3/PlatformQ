# Platform Q Crossplane Resources

This directory contains Crossplane compositions and configurations for provisioning Platform Q resources through Kubernetes-native APIs.

## Overview

Crossplane enables infrastructure provisioning using Kubernetes-style declarative APIs. Platform Q uses Crossplane to:

- Abstract infrastructure complexity across OpenStack and Kubernetes
- Provide self-service resource provisioning for tenants
- Enforce multi-tenant isolation and quotas
- Integrate with CloudKitty and OpenMeter for usage tracking

## Architecture

```
┌─────────────────────┐
│   Tenant Claims     │  (User-created resources)
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│  Composite Resources│  (XRs - Abstract resources)
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│   Compositions      │  (Resource templates)
└──────────┬──────────┘
           │
┌──────────▼──────────┐
│  Managed Resources  │  (Actual cloud resources)
└─────────────────────┘
```

## Available Resources

### Compute Instances
- **XRD**: `ComputeInstance` - Virtual machines on OpenStack
- **Claim**: `ComputeInstanceClaim` - User-facing API
- **Plans**: small, medium, large, xlarge, 2xlarge, gpu.large

### Platform Services
- **XRD**: `PlatformService` - Platform Q services (Cassandra, Ignite, etc.)
- **Claim**: `PlatformServiceClaim` - User-facing API
- **Services**: cassandra, ignite, pulsar, minio, elasticsearch, janusgraph

## Installation

### Prerequisites
1. Kubernetes cluster (1.20+)
2. Crossplane installed (v1.11+)
3. OpenStack cloud with admin access
4. Platform Q services deployed

### Deploy Crossplane Resources

```bash
# Install Crossplane (if not already installed)
kubectl create namespace crossplane-system
helm repo add crossplane-stable https://charts.crossplane.io/stable
helm install crossplane --namespace crossplane-system crossplane-stable/crossplane

# Wait for Crossplane to be ready
kubectl wait --for=condition=healthy --timeout=300s provider/crossplane-provider-kubernetes

# Create OpenStack credentials secret
kubectl create secret generic openstack-credentials \
  --namespace crossplane-system \
  --from-literal=credentials='{
    "auth_url": "https://keystone.example.com:5000/v3",
    "username": "platform-broker",
    "password": "secret",
    "project_name": "service",
    "project_domain_name": "default",
    "user_domain_name": "default",
    "region_name": "RegionOne"
  }'

# Deploy Platform Q Crossplane resources
kubectl apply -k crossplane/

# Verify installation
kubectl get xrd  # Should show compute and platform service XRDs
kubectl get compositions  # Should show available compositions
```

## Usage Examples

### Provision a Compute Instance

```yaml
apiVersion: platformq.io/v1alpha1
kind: ComputeInstanceClaim
metadata:
  name: my-server
  namespace: tenant-namespace
spec:
  instanceType: medium
  osImage: ubuntu-22.04
  publicIpEnabled: true
  tenantId: "12345"
  customerId: "customer-001"
  resellerId: "reseller-001"
  writeConnectionSecretToRef:
    name: my-server-connection
```

Apply the claim:
```bash
kubectl apply -f my-compute-claim.yaml
kubectl get computeinstanceclaim my-server -w
```

### Provision a Platform Service

```yaml
apiVersion: platformq.io/v1alpha1
kind: PlatformServiceClaim
metadata:
  name: my-database
  namespace: tenant-namespace
spec:
  serviceType: cassandra
  plan: standard
  tenantId: "12345"
  customerId: "customer-001"
  resellerId: "reseller-001"
  cassandra:
    replicationFactor: 3
  writeConnectionSecretToRef:
    name: my-database-connection
```

### Access Connection Secrets

```bash
# Get connection details
kubectl get secret my-server-connection -o yaml

# Extract specific values
kubectl get secret my-server-connection -o jsonpath='{.data.privateIp}' | base64 -d
kubectl get secret my-server-connection -o jsonpath='{.data.publicIp}' | base64 -d
```

## Multi-Tenancy

Platform Q enforces multi-tenancy through:

1. **Namespace Isolation**: Each tenant gets a dedicated namespace
2. **RBAC**: Tenants can only create claims in their namespace
3. **Resource Quotas**: Limits on compute, storage, and services
4. **Network Isolation**: Dedicated networks per tenant
5. **Usage Tracking**: All resources tagged with tenant metadata

### Tenant Onboarding

```bash
# Create tenant namespace
kubectl create namespace tenant-12345

# Apply RBAC
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: tenant-user
  namespace: tenant-12345
rules:
- apiGroups: ["platformq.io"]
  resources: ["computeinstanceclaims", "platformserviceclaims"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list"]
EOF

# Create resource quota
kubectl apply -f - <<EOF
apiVersion: v1
kind: ResourceQuota
metadata:
  name: tenant-quota
  namespace: tenant-12345
spec:
  hard:
    computeinstanceclaims.platformq.io: "10"
    platformserviceclaims.platformq.io: "20"
EOF
```

## Monitoring and Troubleshooting

### Check Resource Status

```bash
# List all claims
kubectl get computeinstanceclaims,platformserviceclaims --all-namespaces

# Check claim status
kubectl describe computeinstanceclaim my-server

# View events
kubectl get events --field-selector involvedObject.name=my-server

# Check composite resource
kubectl get computeinstance -l crossplane.io/claim-name=my-server
```

### Common Issues

1. **Claim Stuck in Pending**
   - Check provider credentials: `kubectl describe providerconfig openstack-provider`
   - View composition errors: `kubectl describe composition compute.platformq.io`
   - Check resource quotas: `kubectl describe resourcequota -n tenant-namespace`

2. **Connection Secret Missing**
   - Ensure claim specifies `writeConnectionSecretToRef`
   - Check if managed resource is ready
   - Verify RBAC allows secret creation

3. **Resource Not Created in Cloud**
   - Check provider logs: `kubectl logs -n crossplane-system deployment/provider-openstack`
   - Verify cloud credentials and permissions
   - Check for quota limits in OpenStack

### Debug Mode

Enable debug logging:
```bash
kubectl edit providerconfig openstack-provider
# Add under spec:
#   debugLog: true
```

## Customization

### Modify Compositions

1. Edit composition files in `compositions/`
2. Apply changes: `kubectl apply -f compositions/compute-composition.yaml`
3. New claims will use updated composition

### Add New Instance Types

Edit `compute-composition.yaml` and add to the flavor mapping:
```yaml
transforms:
- type: map
  map:
    small: "2"
    medium: "3"
    large: "4"
    # Add new type:
    3xlarge: "8"  # Map to OpenStack flavor ID
```

### Configure Defaults

Edit XRDs to change default values:
```yaml
properties:
  instanceType:
    type: string
    default: medium  # Change default size
```

## Best Practices

1. **Use Claims**: Always use claims (not composite resources directly)
2. **Set Resource Limits**: Configure appropriate quotas per tenant
3. **Monitor Usage**: Integrate with Platform Q monitoring
4. **Backup Secrets**: Regularly backup connection secrets
5. **Version Control**: Track claim definitions in Git
6. **Naming Convention**: Use consistent naming (e.g., `{app}-{env}-{component}`)

## Integration with Platform Q Services

### Service Broker Integration

The Platform Service Broker can create Crossplane claims programmatically:

```python
# Example: Create compute instance via broker
claim = {
    "apiVersion": "platformq.io/v1alpha1",
    "kind": "ComputeInstanceClaim",
    "metadata": {
        "name": f"osb-{instance_id}",
        "namespace": f"tenant-{tenant_id}"
    },
    "spec": {
        "instanceType": plan_id.replace("compute-", ""),
        "osImage": parameters.get("os_image", "ubuntu-22.04"),
        "tenantId": tenant_id,
        "customerId": customer_id,
        "resellerId": reseller_id
    }
}
k8s_client.create_namespaced_custom_object(
    group="platformq.io",
    version="v1alpha1",
    namespace=claim["metadata"]["namespace"],
    plural="computeinstanceclaims",
    body=claim
)
```

### Usage Reporting

All Crossplane resources automatically report usage to CloudKitty and OpenMeter through:
- CronJobs that collect metrics
- Annotations on managed resources
- Integration with provider APIs

## Security Considerations

1. **Credentials**: Store provider credentials in Kubernetes secrets with encryption at rest
2. **RBAC**: Implement least-privilege access for tenants
3. **Network Policies**: Restrict pod-to-pod communication
4. **Admission Control**: Use OPA/Kyverno for policy enforcement
5. **Audit Logging**: Enable audit logging for all API calls

## Contributing

To add new resource types:

1. Create XRD in `compositions/`
2. Create composition with resource templates
3. Add example claims in `claims/`
4. Update this README
5. Test thoroughly with multiple tenants

## Support

- **Documentation**: https://docs.platformq.io/crossplane
- **Issues**: https://github.com/platformq/crossplane-resources/issues
- **Slack**: #platform-q-crossplane 