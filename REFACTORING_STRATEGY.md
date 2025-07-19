# PlatformQ Service Refactoring Strategy

## Overview

This document outlines the refactoring strategy to merge simulation-service and cad-collaboration-service into a unified platform with centralized state management and compute allocation.

## Architecture Changes

### 1. Service Consolidation

#### Before:
- `simulation-service`: Handles simulations with its own state management
- `cad-collaboration-service`: Handles CAD collaboration with duplicate state management
- Compute allocation embedded in simulation service
- Direct Ignite integration in each service

#### After:
- `collaboration-platform-service`: Unified collaboration platform for all domains
- `state-management-service`: Centralized state management using Apache Ignite
- `compute-allocation-service`: Shared compute resource allocation
- Services communicate through well-defined APIs

### 2. New Service Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Applications                        │
├─────────────────────────────────────────────────────────────┤
│                    Kong API Gateway                           │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────────┐  ┌────────────────────────────┐   │
│  │ Collaboration        │  │ Other Platform Services    │   │
│  │ Platform Service     │  │ (Data, ML, Analytics...)   │   │
│  └──────────┬──────────┘  └──────────┬─────────────────┘   │
├─────────────┴─────────────────────────┴─────────────────────┤
│              Shared Infrastructure Services                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────┐  │
│  │ State Management │  │ Compute Alloc.  │  │Event Router│  │
│  │ Service (Ignite) │  │ Service         │  │ (Pulsar)   │  │
│  └─────────────────┘  └─────────────────┘  └────────────┘  │
├─────────────────────────────────────────────────────────────┤
│              Platform Infrastructure                          │
│  ┌─────────────┐  ┌─────────┐  ┌────────┐  ┌────────────┐ │
│  │   Vault     │  │ Consul  │  │ MinIO  │  │ Cassandra  │ │
│  └─────────────┘  └─────────┘  └────────┘  └────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Patterns

### 1. Domain Adapter Pattern

The collaboration platform uses a plugin-based architecture where each domain (simulation, CAD, etc.) implements a common interface:

```python
class BaseDomainAdapter(ABC):
    @abstractmethod
    def validate_operation(self, operation: DomainOperation) -> Tuple[bool, Optional[str]]
    
    @abstractmethod
    def apply_operation(self, operation: DomainOperation, state: DomainState) -> DomainState
    
    @abstractmethod
    def get_resource_requirements(self, state: DomainState) -> Dict[str, Any]
```

### 2. Event-Driven State Synchronization

All state changes flow through Pulsar events for consistency:

```
User Operation → Collaboration Platform → State Service → Pulsar Event → Other Services
                                     ↑                            ↓
                                     └────────────────────────────┘
```

### 3. Digital Integration Hub (DIH) Pattern

The state management service implements DIH for high-performance data access:
- Write-through caching to persistent stores
- Read-through with automatic loading
- Near-caching for frequently accessed data
- SQL queries across cached data

## Implementation Plan

### Phase 1: Extract Shared Services (Week 1-2)

1. **State Management Service**
   - Extract Ignite management from both services
   - Implement generic cache management API
   - Add transaction support
   - Implement DIH patterns from data-platform-service

2. **Compute Allocation Service**
   - Extract compute allocation from simulation service
   - Add support for CAD workloads
   - Integrate with derivatives engine
   - Implement multi-provider support

### Phase 2: Build Collaboration Platform (Week 3-4)

1. **Core Platform**
   - Implement domain adapter interface
   - Create unified WebSocket protocol
   - Build session management
   - Implement CRDT manager

2. **Domain Adapters**
   - Port simulation logic to adapter
   - Port CAD logic to adapter
   - Create plugin system for new domains

### Phase 3: Integration & Migration (Week 5-6)

1. **Service Integration**
   - Connect to state management service
   - Integrate compute allocation
   - Set up event routing
   - Implement monitoring

2. **Data Migration**
   - Migrate existing sessions
   - Transfer state data
   - Update client applications

## API Changes

### State Management Service APIs

```python
# Cache operations
POST   /api/v1/caches                    # Create cache
GET    /api/v1/caches/{cache}/keys/{key} # Get value
PUT    /api/v1/caches/{cache}/keys/{key} # Put value
POST   /api/v1/caches/{cache}/query      # Query cache

# Transaction operations
POST   /api/v1/transactions              # Start transaction
PUT    /api/v1/transactions/{txId}       # Commit/rollback
```

### Compute Allocation Service APIs

```python
# Resource allocation
POST   /api/v1/allocations               # Request resources
GET    /api/v1/allocations/{id}          # Get allocation status
DELETE /api/v1/allocations/{id}          # Release resources

# Cost management
GET    /api/v1/pricing/current           # Current spot prices
POST   /api/v1/contracts/futures         # Create futures contract
```

### Collaboration Platform APIs

```python
# Session management
POST   /api/v1/sessions                  # Create session
POST   /api/v1/sessions/{id}/join        # Join session
WS     /ws/collaborate/{session_id}      # WebSocket endpoint

# Domain operations
POST   /api/v1/domains/{domain}/operate  # Domain-specific operation
GET    /api/v1/domains/{domain}/state    # Get domain state
```

## Integration Patterns

### 1. State Consistency

Following the data-platform-service pattern:
- All state changes go through state management service
- Event sourcing for audit and replay
- Optimistic concurrency with version checking
- Eventual consistency with Pulsar events

### 2. Resource Allocation

Automatic allocation based on workload:
```python
@session_started.handler
async def allocate_resources(event):
    requirements = domain_adapter.get_resource_requirements(event.state)
    allocation = await compute_service.allocate(
        workload_type=event.domain_type,
        requirements=requirements
    )
```

### 3. Event Routing

Using event-router-service patterns:
- Domain-specific event namespaces
- Dead letter queue for failed operations
- Event enrichment with metadata
- Configurable routing rules

## Performance Optimizations

### 1. Adaptive Update Rates
- 60Hz capability with throttling
- Client-specific rate limiting
- Viewport-based updates
- Progressive state loading

### 2. State Optimization
- CRDT compaction
- Operation batching
- Delta compression
- LOD for large datasets

### 3. Resource Efficiency
- Automatic GPU allocation
- Spot instance usage
- Multi-region deployment
- Cost-based routing

## Monitoring & Observability

### Key Metrics
- Collaboration session count
- Active user count
- Operation throughput
- State synchronization latency
- Resource utilization
- Cost per session

### Dashboards
- Real-time collaboration overview
- Resource usage by domain
- Cost tracking
- Performance analytics

## Security Considerations

### 1. Authentication & Authorization
- JWT tokens via Kong gateway
- Per-domain permissions
- Operation-level authorization
- Audit logging

### 2. Data Protection
- Encryption at rest (Vault)
- TLS for all communications
- State isolation per tenant
- GDPR compliance

## Benefits

1. **Reduced Complexity**: Single collaboration platform instead of multiple services
2. **Better Resource Utilization**: Shared compute allocation with cost optimization
3. **Improved Consistency**: Centralized state management with ACID guarantees
4. **Flexibility**: Plugin architecture for new collaboration types
5. **Cost Efficiency**: Multi-provider compute with futures contracts
6. **Scalability**: Horizontal scaling of each service independently
7. **Maintainability**: Clear separation of concerns

## Migration Checklist

- [ ] Deploy state management service
- [ ] Deploy compute allocation service
- [ ] Deploy collaboration platform service
- [ ] Update Kong routes
- [ ] Migrate existing sessions
- [ ] Update client applications
- [ ] Monitor for issues
- [ ] Deprecate old services
- [ ] Clean up resources

## Rollback Plan

1. Keep old services running during migration
2. Use feature flags for gradual rollout
3. Maintain data sync between old and new
4. Quick switch back via Kong routing
5. Full rollback procedure documented 