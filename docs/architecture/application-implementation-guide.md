# PlatformQ Application Implementation Guide

## Overview

This guide provides detailed implementation patterns and code examples for separating PlatformQ into the modular application suite. It covers service extraction, data separation, event integration, and cross-application communication.

## Service Extraction Patterns

### 1. Shared Libraries Refactoring

Create application-specific shared libraries from the monolithic `platformq-shared`:

```python
# platformq-digital-asset-shared/
├── src/
│   ├── asset_events.py      # Asset-specific events
│   ├── asset_models.py      # Shared asset data models
│   ├── asset_clients.py     # Client for asset services
│   └── asset_utils.py       # Asset-specific utilities

# platformq-intelligence-shared/
├── src/
│   ├── ml_events.py         # ML/AI events
│   ├── ml_models.py         # ML data models
│   ├── ml_clients.py        # ML service clients
│   └── ml_utils.py          # ML utilities
```

### 2. Event Schema Separation

Split the unified event schema into application domains:

```python
# Asset Events (Avro Schema)
{
  "namespace": "platformq.asset.events",
  "type": "record",
  "name": "AssetCreated",
  "fields": [
    {"name": "asset_id", "type": "string"},
    {"name": "creator_id", "type": "string"},
    {"name": "asset_type", "type": "string"},
    {"name": "metadata", "type": "map", "values": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "app_context", "type": {
      "type": "record",
      "name": "AppContext",
      "fields": [
        {"name": "app_id", "type": "string"},
        {"name": "tenant_id", "type": "string"},
        {"name": "correlation_id", "type": "string"}
      ]
    }}
  ]
}
```

### 3. Cross-Application Client Pattern

Implement smart clients that handle both local and remote service calls:

```python
# platformq_intelligence_shared/src/ml_clients.py
from typing import Optional, Dict, Any
import httpx
from platformq_shared.service_client import ServiceClient
from platformq_shared.cache import DistributedCache

class IntelligenceClient:
    """Client for Intelligence Platform services"""
    
    def __init__(self, 
                 deployment_mode: str = "integrated",
                 service_url: Optional[str] = None,
                 cache: Optional[DistributedCache] = None):
        self.deployment_mode = deployment_mode
        self.cache = cache
        
        if deployment_mode == "integrated":
            # Direct service call within same cluster
            self.client = ServiceClient("unified-ml-platform-service")
        else:
            # Remote API call to standalone deployment
            self.client = httpx.AsyncClient(
                base_url=service_url or "https://intelligence.platformq.io"
            )
    
    async def train_model(self, 
                         dataset_id: str,
                         model_config: Dict[str, Any]) -> str:
        """Train a model using the Intelligence Platform"""
        
        # Check cache for existing model
        if self.cache:
            cache_key = f"model:{dataset_id}:{hash(str(model_config))}"
            cached_model = await self.cache.get(cache_key)
            if cached_model:
                return cached_model["model_id"]
        
        # Make the service call
        if self.deployment_mode == "integrated":
            response = await self.client.post(
                "/api/v1/training/jobs",
                json={
                    "dataset_id": dataset_id,
                    "config": model_config
                }
            )
        else:
            # Add authentication for remote calls
            headers = await self._get_auth_headers()
            response = await self.client.post(
                "/api/v1/training/jobs",
                json={
                    "dataset_id": dataset_id,
                    "config": model_config
                },
                headers=headers
            )
        
        model_id = response.json()["model_id"]
        
        # Cache the result
        if self.cache:
            await self.cache.set(
                cache_key,
                {"model_id": model_id},
                ttl=3600
            )
        
        return model_id
```

## Data Separation Strategies

### 1. Logical Data Separation

For shared infrastructure deployments:

```python
# Data access layer with tenant/app isolation
class MultiAppDataAccess:
    """Data access with application-level isolation"""
    
    def __init__(self, app_id: str, tenant_id: str):
        self.app_id = app_id
        self.tenant_id = tenant_id
        self.row_security_context = {
            "app_id": app_id,
            "tenant_id": tenant_id
        }
    
    async def query_cassandra(self, query: str, params: list) -> list:
        """Execute Cassandra query with app/tenant filtering"""
        # Automatically add app and tenant filters
        filtered_query = f"""
            {query} 
            AND app_id = ? 
            AND tenant_id = ?
            ALLOW FILTERING
        """
        params.extend([self.app_id, self.tenant_id])
        
        return await self.cassandra_client.execute(
            filtered_query, 
            params
        )
    
    async def query_trino(self, sql: str) -> pd.DataFrame:
        """Execute Trino query with row-level security"""
        # Trino automatically applies row-level security
        # based on the authenticated context
        return await self.trino_client.query(
            sql,
            catalog="unified_catalog",
            schema=f"{self.app_id}_{self.tenant_id}",
            session_properties={
                "security.context": json.dumps(self.row_security_context)
            }
        )
```

### 2. Physical Data Separation

For standalone deployments with sensitive data:

```yaml
# TradeStream Platform data infrastructure
apiVersion: v1
kind: ConfigMap
metadata:
  name: tradestream-data-config
data:
  cassandra-keyspaces: |
    CREATE KEYSPACE IF NOT EXISTS tradestream_financial
    WITH replication = {
      'class': 'NetworkTopologyStrategy',
      'dc1': 3,
      'dc2': 3
    }
    AND durable_writes = true;
    
    CREATE KEYSPACE IF NOT EXISTS tradestream_market_data
    WITH replication = {
      'class': 'NetworkTopologyStrategy',
      'dc1': 2
    };
  
  minio-buckets: |
    # Encrypted buckets for financial data
    mc mb minio/tradestream-orders --with-lock
    mc mb minio/tradestream-transactions --with-lock
    mc mb minio/tradestream-compliance --with-lock
    
    # Set encryption
    mc encrypt set sse-s3 minio/tradestream-orders
    mc encrypt set sse-s3 minio/tradestream-transactions
    mc encrypt set sse-s3 minio/tradestream-compliance
```

## Event Integration Patterns

### 1. Cross-Application Event Router

```python
# event_router_service/app/cross_app_router.py
from typing import Dict, List, Set
from platformq_shared.events import BaseEvent
import pulsar

class CrossApplicationEventRouter:
    """Routes events between applications based on subscriptions"""
    
    def __init__(self):
        self.app_subscriptions: Dict[str, Set[str]] = {
            # App -> Event types it subscribes to
            "digital-asset-nexus": {
                "AssetCreated", "AssetUpdated", "AssetSold",
                "ModelDeployed",  # From Intelligence Platform
                "ComplianceVerified"  # From Compliance Center
            },
            "intelligence-platform": {
                "AssetCreated",  # For training data
                "DataQualityReportGenerated",  # From DataVerse
                "ComputeAllocated"  # From Collaboration
            },
            "tradestream-platform": {
                "ComplianceVerified",
                "RiskScoreUpdated",
                "MarketDataUpdated"
            }
        }
        
        self.event_mappings: Dict[str, List[str]] = {}
        self._build_event_mappings()
    
    def _build_event_mappings(self):
        """Build reverse mapping of event -> apps"""
        for app, events in self.app_subscriptions.items():
            for event in events:
                if event not in self.event_mappings:
                    self.event_mappings[event] = []
                self.event_mappings[event].append(app)
    
    async def route_event(self, event: BaseEvent):
        """Route event to interested applications"""
        event_type = event.__class__.__name__
        
        # Get interested applications
        target_apps = self.event_mappings.get(event_type, [])
        
        # Publish to application-specific topics
        for app in target_apps:
            topic = f"persistent://platformq/{app}/events"
            
            # Add routing metadata
            event.metadata["routed_to"] = app
            event.metadata["routed_at"] = datetime.utcnow().isoformat()
            
            await self.publish_to_topic(topic, event)
```

### 2. Event Translation Layer

Handle schema evolution between applications:

```python
# Event translation for backward compatibility
class EventTranslator:
    """Translates events between application versions"""
    
    def __init__(self):
        self.translators = {
            ("v1", "v2"): self._translate_v1_to_v2,
            ("v2", "v3"): self._translate_v2_to_v3
        }
    
    async def translate_event(self, 
                            event: Dict[str, Any],
                            source_version: str,
                            target_version: str) -> Dict[str, Any]:
        """Translate event between schema versions"""
        
        if source_version == target_version:
            return event
        
        # Find translation path
        path = self._find_translation_path(source_version, target_version)
        
        # Apply translations in sequence
        translated = event
        for i in range(len(path) - 1):
            translator_key = (path[i], path[i + 1])
            translator = self.translators.get(translator_key)
            if translator:
                translated = translator(translated)
        
        return translated
    
    def _translate_v1_to_v2(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Example translation from v1 to v2"""
        # v2 added app_context field
        return {
            **event,
            "app_context": {
                "app_id": "legacy",
                "tenant_id": event.get("tenant_id", "default"),
                "correlation_id": event.get("correlation_id", str(uuid.uuid4()))
            }
        }
```

## Authentication Federation

### 1. Hybrid Authentication Service

```python
# auth_service/app/federation/hybrid_auth.py
from typing import Optional, Dict, Any
from fastapi import HTTPException
import jwt

class HybridAuthProvider:
    """Supports both centralized and federated authentication"""
    
    def __init__(self):
        self.app_auth_configs = {
            "digital-asset-nexus": {
                "mode": "centralized",
                "issuer": "https://auth.platformq.io"
            },
            "tradestream-platform": {
                "mode": "federated",
                "issuer": "https://tradestream.platformq.io/auth",
                "trust_central": True
            },
            "compliance-center": {
                "mode": "isolated",
                "issuer": "https://compliance.platformq.io/auth",
                "trust_central": False
            }
        }
    
    async def authenticate(self, 
                         token: str,
                         app_id: str) -> Dict[str, Any]:
        """Authenticate token based on app configuration"""
        
        config = self.app_auth_configs.get(app_id)
        if not config:
            raise HTTPException(401, "Unknown application")
        
        if config["mode"] == "centralized":
            # Validate against central auth
            return await self._validate_central_token(token)
        
        elif config["mode"] == "federated":
            # Try app-specific auth first
            try:
                return await self._validate_app_token(token, config["issuer"])
            except:
                if config.get("trust_central"):
                    # Fall back to central auth
                    return await self._validate_central_token(token)
                raise
        
        elif config["mode"] == "isolated":
            # Only validate against app-specific auth
            return await self._validate_app_token(token, config["issuer"])
    
    async def federate_identity(self, 
                              central_token: str,
                              target_app: str) -> str:
        """Exchange central token for app-specific token"""
        
        # Validate central token
        identity = await self._validate_central_token(central_token)
        
        # Get app configuration
        config = self.app_auth_configs.get(target_app)
        
        if config["mode"] == "centralized":
            # No federation needed
            return central_token
        
        # Create app-specific token
        app_claims = {
            "sub": identity["sub"],
            "email": identity["email"],
            "roles": self._map_roles(identity["roles"], target_app),
            "iss": config["issuer"],
            "aud": target_app,
            "federated_from": "https://auth.platformq.io"
        }
        
        return self._create_token(app_claims, target_app)
```

### 2. Service-to-Service Authentication

```python
# Service mesh authentication for deep integration
class ServiceMeshAuth:
    """mTLS-based service authentication"""
    
    def __init__(self):
        self.service_identities = {
            "digital-asset-nexus": {
                "spiffe_id": "spiffe://platformq.io/app/digital-asset-nexus",
                "allowed_apis": ["asset-api", "search-api", "storage-api"]
            },
            "intelligence-platform": {
                "spiffe_id": "spiffe://platformq.io/app/intelligence-platform",
                "allowed_apis": ["ml-api", "training-api", "inference-api"]
            }
        }
    
    async def create_service_token(self, 
                                 source_app: str,
                                 target_app: str,
                                 requested_apis: List[str]) -> str:
        """Create short-lived service token for API access"""
        
        source_config = self.service_identities.get(source_app)
        if not source_config:
            raise ValueError(f"Unknown source app: {source_app}")
        
        # Verify requested APIs are allowed
        allowed = set(source_config["allowed_apis"])
        requested = set(requested_apis)
        if not requested.issubset(allowed):
            raise ValueError(f"Unauthorized APIs: {requested - allowed}")
        
        # Create service token
        claims = {
            "sub": source_config["spiffe_id"],
            "aud": f"spiffe://platformq.io/app/{target_app}",
            "apis": requested_apis,
            "exp": datetime.utcnow() + timedelta(minutes=5)
        }
        
        return jwt.encode(claims, self.service_key, algorithm="RS256")
```

## Deployment Automation

### 1. Application Helm Chart Structure

```yaml
# charts/digital-asset-nexus/values.yaml
global:
  deployment:
    mode: integrated  # integrated | standalone | edge
  
  integration:
    pulsar:
      enabled: true
      url: pulsar://pulsar:6650
    
    trino:
      enabled: true
      catalogs:
        - asset_catalog
        - shared_catalog
    
    auth:
      mode: centralized  # centralized | federated | isolated
      issuer: https://auth.platformq.io

applications:
  digitalAssetService:
    enabled: true
    replicas: 3
    resources:
      requests:
        memory: "2Gi"
        cpu: "1"
      limits:
        memory: "4Gi"
        cpu: "2"
  
  searchService:
    enabled: true
    replicas: 2
    features:
      - asset-search
      - full-text
      - faceted
    
  storageService:
    enabled: true
    replicas: 2
    storage:
      mode: shared  # shared | dedicated
      encryption: true

dataStores:
  cassandra:
    mode: shared  # shared | dedicated
    keyspaces:
      - digital_assets
      - asset_metadata
  
  elasticsearch:
    mode: shared
    indices:
      - assets
      - asset_reviews
  
  minio:
    mode: dedicated
    buckets:
      - name: asset-files
        encryption: true
        versioning: true
```

### 2. GitOps Deployment Pipeline

```yaml
# .github/workflows/deploy-application.yml
name: Deploy PlatformQ Application

on:
  push:
    branches: [main]
    paths:
      - 'applications/*/deploy/**'

jobs:
  detect-changes:
    runs-on: ubuntu-latest
    outputs:
      apps: ${{ steps.detect.outputs.apps }}
    steps:
      - uses: actions/checkout@v3
      
      - id: detect
        run: |
          # Detect which applications changed
          APPS=$(git diff --name-only HEAD^ HEAD | 
                 grep "^applications/" | 
                 cut -d'/' -f2 | 
                 sort -u | 
                 jq -R -s -c 'split("\n")[:-1]')
          echo "apps=$APPS" >> $GITHUB_OUTPUT

  deploy-application:
    needs: detect-changes
    if: needs.detect-changes.outputs.apps != '[]'
    strategy:
      matrix:
        app: ${{ fromJson(needs.detect-changes.outputs.apps) }}
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to Staging
        run: |
          # Deploy using Helm
          helm upgrade --install \
            ${{ matrix.app }} \
            ./charts/${{ matrix.app }} \
            -f ./applications/${{ matrix.app }}/deploy/staging/values.yaml \
            --namespace ${{ matrix.app }}-staging \
            --create-namespace
      
      - name: Run Integration Tests
        run: |
          # Test cross-application integration
          ./scripts/test-integration.sh ${{ matrix.app }} staging
      
      - name: Deploy to Production
        if: success()
        run: |
          # Deploy to production with approval
          ./scripts/deploy-production.sh ${{ matrix.app }}
```

## Monitoring and Observability

### 1. Cross-Application Tracing

```python
# Distributed tracing configuration
from opentelemetry import trace
from opentelemetry.propagate import inject, extract

class CrossAppTracer:
    """Traces requests across application boundaries"""
    
    def __init__(self, app_id: str):
        self.app_id = app_id
        self.tracer = trace.get_tracer(
            f"platformq.{app_id}",
            "1.0.0"
        )
    
    async def trace_cross_app_call(self, 
                                 target_app: str,
                                 operation: str,
                                 request_func):
        """Trace a call to another application"""
        
        with self.tracer.start_as_current_span(
            f"cross_app_call.{target_app}.{operation}",
            attributes={
                "source.app": self.app_id,
                "target.app": target_app,
                "operation": operation
            }
        ) as span:
            # Inject trace context
            headers = {}
            inject(headers)
            
            try:
                result = await request_func(headers)
                span.set_attribute("result.status", "success")
                return result
            except Exception as e:
                span.set_attribute("result.status", "error")
                span.record_exception(e)
                raise
```

### 2. Application Health Dashboard

```yaml
# Grafana dashboard for application suite
{
  "dashboard": {
    "title": "PlatformQ Application Suite",
    "panels": [
      {
        "title": "Application Status",
        "targets": [
          {
            "expr": "up{job=~'digital-asset-nexus|intelligence-platform|tradestream-platform|dataverse-hub|collaboration-workspace|compliance-center'}"
          }
        ]
      },
      {
        "title": "Cross-Application Event Flow",
        "targets": [
          {
            "expr": "rate(platformq_events_routed_total[5m])"
          }
        ]
      },
      {
        "title": "API Gateway Traffic",
        "targets": [
          {
            "expr": "sum by (app) (rate(kong_http_requests_total[5m]))"
          }
        ]
      },
      {
        "title": "Data Sync Lag",
        "targets": [
          {
            "expr": "platformq_data_sync_lag_seconds"
          }
        ]
      }
    ]
  }
}
```

## Testing Strategies

### 1. Cross-Application Integration Tests

```python
# tests/integration/test_cross_app_workflow.py
import pytest
from platformq_test_framework import (
    ApplicationSuite,
    TestDataGenerator,
    EventValidator
)

@pytest.mark.integration
class TestCrossApplicationWorkflows:
    """Test workflows that span multiple applications"""
    
    @pytest.fixture
    async def app_suite(self):
        """Set up application suite for testing"""
        suite = ApplicationSuite()
        
        # Start required applications
        await suite.start_application("digital-asset-nexus")
        await suite.start_application("intelligence-platform")
        await suite.start_application("compliance-center")
        
        yield suite
        
        await suite.stop_all()
    
    async def test_asset_ml_compliance_workflow(self, app_suite):
        """Test: Create asset → Train model → Verify compliance"""
        
        # 1. Create digital asset
        asset = await app_suite.digital_asset.create_asset({
            "type": "dataset",
            "name": "test-dataset",
            "format": "csv",
            "size": 1024000
        })
        
        # 2. Verify asset event propagated
        await EventValidator.wait_for_event(
            "AssetCreated",
            timeout=5,
            app="intelligence-platform"
        )
        
        # 3. Train model using the asset
        model = await app_suite.intelligence.train_model({
            "dataset_id": asset["id"],
            "algorithm": "random_forest",
            "parameters": {"n_estimators": 100}
        })
        
        # 4. Verify model compliance
        compliance = await app_suite.compliance.verify_model({
            "model_id": model["id"],
            "asset_id": asset["id"],
            "checks": ["data_privacy", "bias_detection"]
        })
        
        # 5. Verify compliance event
        await EventValidator.wait_for_event(
            "ComplianceVerified",
            timeout=5,
            app="digital-asset-nexus"
        )
        
        # Assertions
        assert asset["status"] == "active"
        assert model["status"] == "trained"
        assert compliance["status"] == "compliant"
```

### 2. Application Isolation Tests

```python
# Test that applications work in isolation
async def test_standalone_deployment(app_config):
    """Test application works without other apps"""
    
    # Deploy only the target application
    app = await deploy_application(
        "intelligence-platform",
        mode="standalone",
        dependencies=[]
    )
    
    # Test core functionality
    dataset = await app.create_local_dataset(test_data)
    model = await app.train_model(dataset)
    prediction = await app.predict(model, test_input)
    
    # Verify graceful degradation
    # Should work without asset service
    with pytest.warns(IntegrationWarning):
        await app.import_asset("asset-123")  # Falls back to API
```

## Performance Optimization

### 1. Cross-Application Caching

```python
# Distributed cache for cross-app data
class CrossApplicationCache:
    """Cache frequently accessed cross-app data"""
    
    def __init__(self, app_id: str):
        self.app_id = app_id
        self.local_cache = {}  # In-memory cache
        self.shared_cache = IgniteClient()  # Distributed cache
        
        # Cache configuration per data type
        self.cache_config = {
            "user_profile": {
                "ttl": 3600,
                "scope": "shared",
                "refresh_on_access": True
            },
            "asset_metadata": {
                "ttl": 7200,
                "scope": "shared",
                "refresh_on_access": False
            },
            "ml_model": {
                "ttl": 86400,
                "scope": "app",
                "refresh_on_access": True
            }
        }
    
    async def get_cross_app_data(self, 
                               data_type: str,
                               key: str,
                               source_app: str) -> Any:
        """Get data from another application with caching"""
        
        cache_key = f"{source_app}:{data_type}:{key}"
        config = self.cache_config.get(data_type, {})
        
        # Check local cache first
        if cache_key in self.local_cache:
            entry = self.local_cache[cache_key]
            if entry["expires"] > time.time():
                if config.get("refresh_on_access"):
                    asyncio.create_task(
                        self._refresh_cache(cache_key, source_app)
                    )
                return entry["data"]
        
        # Check distributed cache
        if config.get("scope") == "shared":
            cached = await self.shared_cache.get(cache_key)
            if cached:
                self.local_cache[cache_key] = cached
                return cached["data"]
        
        # Fetch from source application
        data = await self._fetch_from_app(
            source_app, 
            data_type, 
            key
        )
        
        # Cache the result
        await self._cache_data(
            cache_key, 
            data, 
            config.get("ttl", 3600)
        )
        
        return data
```

### 2. Batch Event Processing

```python
# Optimize cross-app event processing
class BatchEventProcessor:
    """Process events in batches for efficiency"""
    
    def __init__(self, app_id: str):
        self.app_id = app_id
        self.event_buffer = defaultdict(list)
        self.batch_size = 100
        self.batch_timeout = 1.0  # seconds
        
    async def process_cross_app_events(self):
        """Process events from other applications"""
        
        consumer = await self.create_consumer(
            f"persistent://platformq/{self.app_id}/events"
        )
        
        while True:
            # Collect events into batches
            batch_start = time.time()
            events = []
            
            while len(events) < self.batch_size:
                try:
                    msg = await consumer.receive_async(
                        timeout_millis=100
                    )
                    events.append(msg)
                    
                    # Check timeout
                    if time.time() - batch_start > self.batch_timeout:
                        break
                        
                except TimeoutError:
                    if events:  # Process partial batch
                        break
                    continue
            
            if events:
                # Process batch
                await self._process_event_batch(events)
                
                # Acknowledge all messages
                for msg in events:
                    await consumer.acknowledge_async(msg)
```

## Conclusion

This implementation guide provides the technical foundation for separating PlatformQ into a modular application suite. The patterns ensure:

1. **Flexibility**: Applications can run together or separately
2. **Integration**: Deep real-time data sharing when needed
3. **Security**: Proper isolation for sensitive data
4. **Performance**: Optimized cross-application communication
5. **Maintainability**: Clear boundaries and contracts between applications

Follow these patterns to successfully transform PlatformQ from a monolithic platform into a flexible, scalable application suite.