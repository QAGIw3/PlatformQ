# Migration Guide: Service Refactoring

## Overview

This guide provides step-by-step instructions for migrating from the old `simulation-service` and `cad-collaboration-service` to the new refactored architecture with three specialized services.

## New Architecture

### Services Created

1. **Collaboration Platform Service** (`collaboration-platform-service`)
   - Unified real-time collaboration framework
   - Supports multiple domains (simulation, CAD, and extensible for others)
   - Handles WebSocket connections, session management, and operation synchronization

2. **State Management Service** (`state-management-service`)
   - Centralized Apache Ignite management
   - Provides high-performance distributed state storage
   - Supports ACID transactions and SQL queries

3. **Compute Allocation Service** (`compute-allocation-service`)
   - Multi-provider compute resource management
   - Cost optimization with spot/reserved pricing
   - SLA derivatives and futures contracts for capacity

## Migration Steps

### 1. Update Dependencies

All services now use the refactored clients:

```python
# Old way (simulation-service)
from app.ignite_manager import IgniteManager

# New way
from app.clients import StateManagementClient, ComputeAllocationClient
```

### 2. Update WebSocket Endpoints

The WebSocket endpoints have been unified:

```javascript
// Old endpoints
ws://localhost:8011/ws/simulation/{simulation_id}
ws://localhost:8012/ws/collaborate/{session_id}

// New unified endpoint
ws://localhost:8017/ws/collaborate/{session_id}?user_id=USER_ID&user_name=NAME
```

### 3. Session Creation

Sessions now require specifying a domain type:

```python
# Old way (simulation)
POST /api/v1/simulations
{
  "name": "My Simulation",
  "type": "agent_based"
}

# New way
POST /api/v1/sessions
{
  "domain_type": "simulation",  # or "cad"
  "project_name": "My Simulation",
  "description": "Agent-based simulation"
}
```

### 4. Operation Handling

Operations now use a unified format:

```python
# Old way (CAD)
{
  "type": "transform",
  "object_id": "obj123",
  "transform": {...}
}

# New way
{
  "type": "update",  # Generic operation type
  "subtype": "transform",  # Domain-specific subtype
  "data": {
    "object_id": "obj123",
    "transform": {...}
  }
}
```

### 5. State Access

State is now managed centrally:

```python
# Old way - Direct Ignite access
cache = self.ignite_manager.get_cache("simulation_states")
state = cache.get(simulation_id)

# New way - Through State Management Service
state = await state_client.get("simulation_states", simulation_id)
```

### 6. Compute Allocation

Compute resources are now allocated through a dedicated service:

```python
# Old way - Embedded in simulation service
resources = self.allocate_compute_resources(simulation)

# New way - Through Compute Allocation Service
allocation = await compute_client.allocate(
    workload_type="simulation",
    workload_id=session_id,
    requirements={
        "cpu_cores": 4,
        "memory_gb": 16,
        "gpu_required": True
    },
    strategy="BALANCED",
    duration_hours=4
)
```

## API Mapping

### Simulation Service Endpoints

| Old Endpoint | New Endpoint | Notes |
|-------------|--------------|-------|
| `POST /api/v1/simulations` | `POST /api/v1/sessions` | Add `domain_type: "simulation"` |
| `GET /api/v1/simulations/{id}` | `GET /api/v1/sessions/{id}` | Same response format |
| `DELETE /api/v1/simulations/{id}` | `DELETE /api/v1/sessions/{id}` | Same behavior |
| `POST /api/v1/simulations/{id}/agents` | WebSocket operation | Use operation with `subtype: "add_agent"` |
| `GET /api/v1/simulations/{id}/metrics` | `GET /api/v1/sessions/{id}/state` | Metrics in state data |

### CAD Service Endpoints

| Old Endpoint | New Endpoint | Notes |
|-------------|--------------|-------|
| `POST /api/v1/sessions` | `POST /api/v1/sessions` | Add `domain_type: "cad"` |
| `GET /api/v1/models/{id}` | `GET /api/v1/sessions/{id}/state` | Model data in state |
| `POST /api/v1/models/{id}/optimize` | WebSocket operation | Use operation with `subtype: "optimize_mesh"` |

## Docker Compose Changes

Update your docker-compose commands:

```bash
# Old way
docker-compose up simulation-service cad-collaboration-service

# New way
docker-compose -f docker-compose.yml -f docker-compose.services.yml up \
  state-management-service \
  compute-allocation-service \
  collaboration-platform-service
```

## Environment Variables

Update your environment configuration:

```bash
# Old
IGNITE_ADDRESSES=ignite:10800
PULSAR_BROKER_URL=pulsar://pulsar:6650

# New
STATE_SERVICE_URL=http://state-management-service:8000
COMPUTE_SERVICE_URL=http://compute-allocation-service:8000
PULSAR_URL=pulsar://pulsar:6650
```

## Testing the Migration

1. **Health Checks**: Verify all services are healthy
   ```bash
   curl http://localhost:8015/health  # State Management
   curl http://localhost:8016/health  # Compute Allocation
   curl http://localhost:8017/health  # Collaboration Platform
   ```

2. **Create Test Session**:
   ```bash
   curl -X POST http://localhost:8017/api/v1/sessions \
     -H "Content-Type: application/json" \
     -d '{"domain_type": "simulation", "project_name": "Test"}'
   ```

3. **Test WebSocket Connection**:
   ```javascript
   const ws = new WebSocket('ws://localhost:8017/ws/collaborate/SESSION_ID?user_id=test&user_name=Test');
   ws.onmessage = (event) => console.log('Received:', event.data);
   ```

## Rollback Plan

If issues arise during migration:

1. Keep old services running in parallel during transition
2. Use feature flags to switch between old/new endpoints
3. Maintain data compatibility layer if needed
4. Old services can be removed once migration is verified

## Benefits of New Architecture

1. **Unified Collaboration**: Single platform for all collaborative workloads
2. **Better Resource Utilization**: Centralized compute allocation with cost optimization
3. **Improved State Management**: ACID transactions, SQL queries, better caching
4. **Extensibility**: Easy to add new collaboration domains
5. **Performance**: Optimized state synchronization and resource allocation
6. **Cost Tracking**: Built-in cost forecasting and optimization

## Support

For migration assistance, please refer to:
- Service documentation in `/docs/services/`
- Integration tests in `/tests/integration/`
- Example client code in `/examples/refactored-services/` 