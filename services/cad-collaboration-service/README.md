# CAD Collaboration Service

Real-time collaborative CAD/3D editing service with CRDT-based conflict resolution for PlatformQ.

## Features

- **Real-time Collaboration**: Multiple users can edit 3D models simultaneously
- **Conflict-Free Replicated Data Types (CRDTs)**: Automatic conflict resolution for concurrent edits
- **WebSocket Support**: Low-latency real-time communication
- **Apache Ignite Integration**: In-memory caching of active 3D scenes
- **Apache Flink Integration**: Real-time mesh optimization and LOD generation
- **Version Control**: Complete history and diff tracking via Cassandra
- **Physics Simulation**: Integration with existing simulation engine

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌────────────┐
│   Frontend  │────▶│  WebSocket   │────▶│   Ignite   │
│  Three.js   │     │   Gateway    │     │   Cache    │
└─────────────┘     └──────────────┘     └────────────┘
                            │
                            ▼
                    ┌──────────────┐
                    │     CRDT     │
                    │ Synchronizer │
                    └──────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌─────────────┐    ┌──────────────┐    ┌────────────┐
│   Pulsar    │    │  Cassandra   │    │   Flink    │
│   Events    │    │   History    │    │ Optimizer  │
└─────────────┘    └──────────────┘    └────────────┘
```

## Components

### Frontend (React + Three.js)
- `CADCollaborationView.jsx`: Main 3D editing interface
- Real-time cursor tracking
- Transform controls (move, rotate, scale)
- User presence awareness

### Backend Services
- **cad-collaboration-service**: Main coordination service
- **digital-asset-service**: Extended with CAD session management
- **Flink Mesh Optimization Job**: Real-time mesh processing

### Data Storage
- **Ignite**: Active geometry caching, user presence, operation logs
- **Cassandra**: Version history, operation logs, collaboration analytics
- **MinIO**: 3D model snapshots and optimized meshes

## API Endpoints

### Digital Asset Service Extensions

```
POST   /api/v1/assets/{asset_id}/cad-sessions
GET    /api/v1/assets/{asset_id}/cad-sessions/active
POST   /api/v1/cad-sessions/{session_id}/join
WS     /api/v1/ws/cad-sessions/{session_id}
POST   /api/v1/cad-sessions/{session_id}/checkpoint
POST   /api/v1/assets/{asset_id}/geometry-versions
```

### CAD Collaboration Service

```
POST   /api/v1/sessions/{session_id}/sync
GET    /api/v1/sessions/{session_id}/state
POST   /api/v1/sessions/{session_id}/merge
GET    /api/v1/sessions/{session_id}/presence
POST   /api/v1/sessions/{session_id}/presence
POST   /api/v1/optimize/mesh
POST   /api/v1/optimize/lod
GET    /api/v1/optimize/status/{request_id}
WS     /api/v1/ws/sync/{session_id}
```

## Event Schemas

### GeometryOperationEvent
- Real-time geometry changes with CRDT metadata
- Vector clocks for causal ordering
- Parent operation tracking for conflict resolution

### MeshOptimizationRequest/Result
- Async mesh processing requests
- Support for decimation, repair, remeshing, smoothing
- LOD generation with configurable levels

### CADSessionEvent
- Session lifecycle events
- User join/leave notifications
- Checkpoint creation events

## CRDT Implementation

The `Geometry3DCRDT` class implements:
- Add/Remove/Modify operations for vertices, edges, faces
- Vector clock-based causality tracking
- Automatic conflict resolution
- State serialization/deserialization

## Deployment

### Helm Installation

```bash
helm install cad-collab ./services/cad-collaboration-service/helm \
  --namespace platformq \
  --values custom-values.yaml
```

### Required Infrastructure

1. **Apache Ignite Cluster**
   ```bash
   kubectl apply -f infra/ignite/cad-cache-config.xml
   ```

2. **Cassandra Keyspace**
   ```bash
   kubectl exec -it cassandra-0 -- cqlsh -f /scripts/create_cassandra_schema.cql
   ```

3. **Flink Job Deployment**
   ```bash
   flink run -c com.platformq.flink.mesh.MeshOptimizationJob \
     /opt/flink/mesh-optimization-job.jar
   ```

## Configuration

Key configuration options in `values.yaml`:

```yaml
config:
  ignite:
    hosts: ["ignite-0:10800", "ignite-1:10800"]
  
  crdt:
    syncInterval: 1         # seconds
    checkpointInterval: 100 # operations
    
  meshOptimization:
    requestTimeout: 300     # seconds
    maxConcurrentRequests: 50
```

## Performance Considerations

- **Ignite Cache**: Configured with 4GB max memory per node
- **WebSocket Connections**: Max 1000 concurrent per pod
- **CRDT Checkpoints**: Every 100 operations to limit memory usage
- **Mesh Optimization**: Async processing via Flink for non-blocking edits

## Development

### Local Setup

1. Install dependencies:
   ```bash
   cd services/cad-collaboration-service
   pip install -r requirements.txt
   ```

2. Run locally:
   ```bash
   uvicorn app.main:app --reload --port 8001
   ```

3. Frontend development:
   ```bash
   cd frontend
   npm install three
   npm run dev
   ```

### Testing

```bash
# Backend tests
pytest services/cad-collaboration-service/tests/

# Frontend tests
npm test -- src/features/cad/
```

## Monitoring

- Prometheus metrics exposed at `/metrics`
- Key metrics:
  - Active sessions count
  - Operations per second
  - CRDT state size
  - Mesh optimization queue length
  - WebSocket connection count

## Future Enhancements

1. **Advanced Mesh Operations**
   - Boolean operations (union, difference, intersection)
   - Parametric modeling support
   - Constraint-based editing

2. **Enhanced Collaboration**
   - Voice/video integration
   - Annotation system
   - Review workflows

3. **Performance Optimizations**
   - GPU-accelerated mesh processing
   - Progressive mesh streaming
   - Adaptive LOD based on viewport

4. **AI Integration**
   - Auto-complete for modeling operations
   - Intelligent mesh optimization
   - Anomaly detection in edits 