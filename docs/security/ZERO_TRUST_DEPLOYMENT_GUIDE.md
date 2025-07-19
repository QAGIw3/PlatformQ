# Zero Trust Security Architecture Deployment Guide

## Overview

This guide provides step-by-step instructions for deploying PlatformQ's comprehensive Zero Trust Security Architecture, including:

- **mTLS Service Mesh** with Consul Connect
- **API Gateway** with OAuth2/OIDC using Kong
- **Automated Secrets Rotation** with HashiCorp Vault
- **Policy as Code** with Open Policy Agent (OPA)

## Architecture Components

```mermaid
graph TB
    subgraph "External Traffic"
        Client[Client Application]
        API[API Client]
    end
    
    subgraph "Edge Layer"
        Kong[Kong API Gateway]
        WAF[Web Application Firewall]
    end
    
    subgraph "Service Mesh"
        Consul[Consul]
        Envoy[Envoy Proxies]
    end
    
    subgraph "Services"
        Auth[Auth Service]
        Digital[Digital Asset Service]
        Blockchain[Blockchain Gateway]
        Data[Data Platform]
    end
    
    subgraph "Security Infrastructure"
        Vault[HashiCorp Vault]
        OPA[Open Policy Agent]
        SIEM[SIEM/Logging]
    end
    
    Client --> Kong
    API --> Kong
    Kong --> Auth
    Kong --> Envoy
    Envoy --> Digital
    Envoy --> Blockchain
    Envoy --> Data
    
    Consul --> Envoy
    Vault --> Kong
    Vault --> Services
    OPA --> Services
    Services --> SIEM
```

## Prerequisites

- Kubernetes cluster (1.24+) or Docker Compose environment
- Helm 3.x installed
- kubectl configured
- DNS configured for your domain

## Phase 1: Deploy Core Security Infrastructure

### 1.1 Deploy HashiCorp Vault

```bash
# Add Vault Helm repository
helm repo add hashicorp https://helm.releases.hashicorp.com

# Create namespace
kubectl create namespace security

# Deploy Vault with HA configuration
helm install vault hashicorp/vault \
  --namespace security \
  --values vault-values.yaml
```

**vault-values.yaml:**
```yaml
server:
  ha:
    enabled: true
    replicas: 3
    raft:
      enabled: true
  
  ingress:
    enabled: true
    hosts:
      - host: vault.platformq.io
    tls:
      - secretName: vault-tls
        hosts:
          - vault.platformq.io

  dataStorage:
    enabled: true
    size: 10Gi
    storageClass: fast-ssd

ui:
  enabled: true
  serviceType: ClusterIP
```

### 1.2 Initialize Vault

```bash
# Initialize Vault
kubectl exec -n security vault-0 -- vault operator init \
  -key-shares=5 \
  -key-threshold=3 \
  -format=json > vault-keys.json

# Unseal Vault (repeat for each pod)
for i in 0 1 2; do
  kubectl exec -n security vault-$i -- vault operator unseal $UNSEAL_KEY_1
  kubectl exec -n security vault-$i -- vault operator unseal $UNSEAL_KEY_2
  kubectl exec -n security vault-$i -- vault operator unseal $UNSEAL_KEY_3
done

# Login to Vault
export VAULT_TOKEN=$(cat vault-keys.json | jq -r '.root_token')
kubectl exec -n security vault-0 -- vault login $VAULT_TOKEN
```

### 1.3 Configure Vault

```bash
# Run initialization script
kubectl cp infra/docker-compose/vault/init-scripts/init-platformq.sh \
  security/vault-0:/tmp/init-platformq.sh

kubectl exec -n security vault-0 -- /bin/sh /tmp/init-platformq.sh
```

### 1.4 Deploy Consul

```bash
# Add Consul Helm repository
helm repo add hashicorp https://helm.releases.hashicorp.com

# Deploy Consul with Connect enabled
helm install consul hashicorp/consul \
  --namespace security \
  --values consul-values.yaml
```

**consul-values.yaml:**
```yaml
global:
  name: consul
  datacenter: platformq-dc1
  
  tls:
    enabled: true
    enableAutoEncrypt: true
    
  acls:
    manageSystemACLs: true
    
  gossipEncryption:
    autoGenerate: true

server:
  replicas: 3
  storage: 10Gi
  storageClass: fast-ssd
  
  connect: true
  
connectInject:
  enabled: true
  default: true
  
  transparentProxy:
    defaultEnabled: true
    
  consulNamespaces:
    consulDestinationNamespace: default
    
controller:
  enabled: true
  
ui:
  enabled: true
  service:
    type: ClusterIP
    
meshGateway:
  enabled: true
  replicas: 2
```

## Phase 2: Deploy API Gateway with OIDC

### 2.1 Deploy Kong with OIDC Plugin

```bash
# Create Kong namespace
kubectl create namespace kong

# Deploy Kong
helm install kong kong/kong \
  --namespace kong \
  --values kong-values.yaml
```

**kong-values.yaml:**
```yaml
image:
  repository: kong
  tag: "3.5"
  
env:
  database: "postgres"
  pg_host: "postgres.data"
  pg_password:
    valueFrom:
      secretKeyRef:
        name: kong-postgres
        key: password
        
  plugins: "bundled,oidc"
  
proxy:
  type: LoadBalancer
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
    
admin:
  enabled: true
  type: ClusterIP
  
postgresql:
  enabled: false  # Use external PostgreSQL
  
migrations:
  preUpgrade: true
  postUpgrade: true
  
plugins:
  configMaps:
    - name: kong-plugin-oidc-auth
      pluginName: oidc
```

### 2.2 Configure Kong Plugins

```bash
# Apply OIDC configuration
kubectl apply -f iac/kubernetes/kong/plugins/oidc-auth.yaml

# Configure routes
kubectl apply -f iac/kubernetes/kong/_routes.yaml

# Configure services
kubectl apply -f iac/kubernetes/kong/_services.yaml
```

## Phase 3: Enable mTLS with Consul Connect

### 3.1 Configure Service Defaults

```bash
# Apply service defaults for all services
kubectl apply -f - <<EOF
apiVersion: consul.hashicorp.com/v1alpha1
kind: ServiceDefaults
metadata:
  name: global
spec:
  protocol: http
  meshGateway:
    mode: local
  expose:
    checks: true
    paths:
      - path: /metrics
        protocol: http
EOF
```

### 3.2 Configure Service Intentions

```bash
# Allow API Gateway to all services
kubectl apply -f - <<EOF
apiVersion: consul.hashicorp.com/v1alpha1
kind: ServiceIntentions
metadata:
  name: api-gateway-intentions
spec:
  destination:
    name: "*"
  sources:
    - name: kong
      action: allow
EOF

# Configure service-specific intentions
for service in auth-service digital-asset-service blockchain-gateway; do
  kubectl apply -f - <<EOF
apiVersion: consul.hashicorp.com/v1alpha1
kind: ServiceIntentions
metadata:
  name: ${service}-intentions
spec:
  destination:
    name: ${service}
  sources:
    - name: kong
      action: allow
    - name: prometheus
      action: allow
EOF
done
```

### 3.3 Update Service Deployments

Update each service deployment to include Consul annotations:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: digital-asset-service
spec:
  template:
    metadata:
      annotations:
        consul.hashicorp.com/connect-inject: "true"
        consul.hashicorp.com/connect-service: "digital-asset-service"
        consul.hashicorp.com/connect-service-port: "8000"
        consul.hashicorp.com/transparent-proxy: "true"
```

## Phase 4: Deploy OPA for Authorization

### 4.1 Deploy OPA

```bash
# Deploy OPA
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opa
  namespace: security
spec:
  replicas: 3
  selector:
    matchLabels:
      app: opa
  template:
    metadata:
      labels:
        app: opa
    spec:
      containers:
      - name: opa
        image: openpolicyagent/opa:0.59.0-envoy
        ports:
        - containerPort: 8181
        args:
          - "run"
          - "--server"
          - "--config-file=/config/config.yaml"
          - "--addr=0.0.0.0:8181"
          - "--diagnostic-addr=0.0.0.0:8282"
          - "--set=plugins.envoy_ext_authz_grpc.addr=:9191"
          - "--set=plugins.envoy_ext_authz_grpc.query=data.platformq.authz.allow"
          - "--set=decision_logs.console=true"
        volumeMounts:
        - name: config
          mountPath: /config
      volumes:
      - name: config
        configMap:
          name: opa-config
---
apiVersion: v1
kind: Service
metadata:
  name: opa
  namespace: security
spec:
  selector:
    app: opa
  ports:
  - name: http
    port: 8181
  - name: grpc
    port: 9191
EOF
```

### 4.2 Configure OPA Policies

```bash
# Create OPA configuration
kubectl create configmap opa-config -n security --from-file=config.yaml

# Load initial policies
kubectl exec -n security opa-0 -- \
  curl -X PUT http://localhost:8181/v1/policies/rbac \
  -H "Content-Type: text/plain" \
  --data-binary @/policies/rbac.rego
```

## Phase 5: Configure Secrets Rotation

### 5.1 Deploy Security Service

```bash
# Build and deploy security service
docker build -t platformq/security-service:latest services/security-service
docker push platformq/security-service:latest

kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: security-service
  namespace: security
spec:
  replicas: 2
  selector:
    matchLabels:
      app: security-service
  template:
    metadata:
      labels:
        app: security-service
    spec:
      serviceAccountName: security-service
      containers:
      - name: security-service
        image: platformq/security-service:latest
        env:
        - name: VAULT_ADDR
          value: "http://vault:8200"
        - name: CONSUL_ADDR
          value: "http://consul:8500"
        - name: OPA_ADDR
          value: "http://opa:8181"
EOF
```

### 5.2 Configure Rotation Policies

```bash
# Configure database credential rotation
vault write database/config/postgresql \
  plugin_name=postgresql-database-plugin \
  allowed_roles="*" \
  connection_url="postgresql://{{username}}:{{password}}@postgres:5432/platformq" \
  username="vault_admin" \
  password="vault_password"

# Configure automatic rotation
vault write database/roles/app-readwrite \
  db_name=postgresql \
  creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}';" \
  default_ttl="24h" \
  max_ttl="72h"
```

## Phase 6: Update Services with Security Integration

### 6.1 Update Service Code

Update each service to use the enhanced security middleware:

```python
from fastapi import FastAPI
from platformq_shared.middleware.security_middleware import SecurityMiddleware
from platformq_shared.vault.vault_client import VaultClient, VaultConfig
from platformq_shared.consul.consul_client import ConsulClient, ConsulConfig
from platformq_shared.authorization.opa_client import OPAClient, OPAConfig
from platformq_shared.service_mesh import ServiceMeshIntegration, ServiceMeshConfig

# Initialize security components
vault_client = VaultClient(VaultConfig(
    host=os.getenv("VAULT_ADDR", "vault.security"),
    token=os.getenv("VAULT_TOKEN")
))

consul_client = ConsulClient(ConsulConfig(
    host=os.getenv("CONSUL_ADDR", "consul.security")
))

opa_client = OPAClient(OPAConfig(
    host=os.getenv("OPA_ADDR", "opa.security")
))

service_mesh = ServiceMeshIntegration(
    ServiceMeshConfig(service_name="digital-asset-service"),
    consul_client,
    vault_client
)

# Initialize FastAPI app
app = FastAPI()

# Add security middleware
app.add_middleware(
    SecurityMiddleware,
    service_name="digital-asset-service",
    vault_client=vault_client,
    consul_client=consul_client,
    opa_client=opa_client,
    service_mesh=service_mesh,
    enable_auth=True,
    enable_authz=True,
    enable_audit=True
)

# Initialize service mesh on startup
@app.on_event("startup")
async def startup():
    await vault_client.initialize()
    await consul_client.initialize()
    await opa_client.initialize()
    await service_mesh.initialize()
```

## Phase 7: Monitoring and Validation

### 7.1 Verify mTLS Communication

```bash
# Check Consul intentions
consul intention list

# Verify service mesh proxies
kubectl get pods -l consul.hashicorp.com/connect-inject-status=injected

# Test service-to-service communication
kubectl exec -it deployment/digital-asset-service -- \
  curl -v http://auth-service:8000/health
```

### 7.2 Verify API Gateway

```bash
# Test OIDC authentication
curl -X POST https://api.platformq.io/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","password":"password"}'

# Test authenticated request
curl https://api.platformq.io/assets/api/v1/assets \
  -H "Authorization: Bearer $TOKEN"
```

### 7.3 Monitor Security Events

```bash
# View OPA decision logs
kubectl logs -n security deployment/opa -f | jq .

# View Vault audit logs
kubectl exec -n security vault-0 -- vault audit list

# Check security metrics
curl http://prometheus:9090/api/v1/query?query=security_authz_decisions_total
```

## Security Checklist

- [ ] All services communicate via mTLS
- [ ] API Gateway enforces authentication
- [ ] OPA policies are loaded and active
- [ ] Secrets rotation is configured
- [ ] Audit logging is enabled
- [ ] Security headers are present
- [ ] Rate limiting is active
- [ ] Monitoring dashboards show security metrics
- [ ] Incident response procedures documented
- [ ] Security scanning integrated in CI/CD

## Troubleshooting

### mTLS Issues

```bash
# Check Envoy proxy logs
kubectl logs $POD -c envoy-sidecar

# Verify certificates
openssl s_client -connect service:port -showcerts
```

### Authorization Failures

```bash
# Check OPA policies
curl http://opa:8181/v1/policies

# Test policy evaluation
curl -X POST http://opa:8181/v1/data/platformq/authz \
  -H "Content-Type: application/json" \
  -d '{"input":{"user":"test","resource":"assets","action":"read"}}'
```

### Secrets Rotation Problems

```bash
# Check rotation status
vault read database/creds/app-readwrite

# Verify service account permissions
vault policy read security-service-policy
```

## Next Steps

1. **Security Hardening**: Implement CIS benchmarks
2. **Compliance**: Enable SOC2/HIPAA compliance features
3. **Disaster Recovery**: Test backup and restore procedures
4. **Penetration Testing**: Schedule security assessments
5. **Security Training**: Train development team on secure practices 