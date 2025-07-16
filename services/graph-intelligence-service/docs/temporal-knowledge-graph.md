# Temporal Knowledge Graph Enhancement

Extension of the Graph Intelligence Service with temporal reasoning and causal inference capabilities for "what-if" analysis.

## Overview

The Temporal Knowledge Graph (TKG) enhancement adds time-aware reasoning to JanusGraph, enabling:
- Historical state tracking of all graph entities
- Causal relationship discovery
- What-if scenario simulation
- Time-based pattern detection
- Predictive failure analysis

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Temporal Knowledge Graph                    │
├─────────────────┬───────────────────┬───────────────────────┤
│  Temporal Layer │  Causal Inference │  Scenario Engine      │
├─────────────────┴───────────────────┴───────────────────────┤
│                        JanusGraph                             │
│  ┌──────────┐  ┌──────────────┐  ┌─────────────────────┐   │
│  │ Vertices │  │     Edges     │  │  Temporal Index     │   │
│  │  + Time  │  │  + Causality  │  │ (Elasticsearch)     │   │
│  └──────────┘  └──────────────┘  └─────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Temporal Data Model

Enhanced vertex and edge properties:

```groovy
// Temporal Vertex Properties
vertex.property('valid_from', timestamp)
vertex.property('valid_to', timestamp)
vertex.property('version', version_id)
vertex.property('state_vector', serialized_state)

// Causal Edge Properties
edge.property('causal_strength', 0.0-1.0)
edge.property('lag_time', milliseconds)
edge.property('confidence', 0.0-1.0)
edge.property('discovered_at', timestamp)
```

### 2. Causal Discovery Engine

Implements multiple causal inference algorithms:

- **PC Algorithm**: Constraint-based causal discovery
- **GES (Greedy Equivalence Search)**: Score-based discovery
- **LiNGAM**: Linear non-Gaussian acyclic models
- **Transfer Entropy**: Information-theoretic causality

### 3. What-If Scenario Engine

```python
class ScenarioEngine:
    def simulate_failure(self, component_id: str, failure_time: datetime) -> CascadeAnalysis
    def predict_impact(self, change: Dict, horizon: timedelta) -> ImpactPrediction
    def find_root_cause(self, symptom: Dict, time_window: Tuple[datetime, datetime]) -> List[CausalChain]
    def optimize_intervention(self, target_state: Dict) -> List[Intervention]
```

## API Extensions

### REST API Endpoints

```
# Temporal Queries
GET    /api/v1/graph/temporal/snapshot?timestamp={iso_timestamp}
GET    /api/v1/graph/temporal/evolution/{entity_id}?start={start}&end={end}
POST   /api/v1/graph/temporal/query

# Causal Analysis
POST   /api/v1/causal/discover
GET    /api/v1/causal/chains/{entity_id}
POST   /api/v1/causal/strength

# What-If Scenarios
POST   /api/v1/scenarios/simulate
GET    /api/v1/scenarios/{scenario_id}/results
POST   /api/v1/scenarios/compare
```

### gRPC Extensions

```protobuf
service TemporalGraphService {
    // Temporal operations
    rpc GetTemporalSnapshot(TemporalSnapshotRequest) returns (GraphSnapshot);
    rpc GetEntityEvolution(EntityEvolutionRequest) returns (stream EntityState);
    
    // Causal inference
    rpc DiscoverCausalRelationships(CausalDiscoveryRequest) returns (CausalGraph);
    rpc SimulateIntervention(InterventionRequest) returns (SimulationResult);
    
    // What-if analysis
    rpc PredictCascadeFailure(FailureScenarioRequest) returns (CascadeAnalysis);
    rpc FindRootCause(SymptomAnalysisRequest) returns (RootCauseAnalysis);
}
```

## Implementation Details

### Temporal Indexing Strategy

```python
# Bi-temporal indexing for valid time and transaction time
class TemporalIndex:
    def __init__(self, elasticsearch_client):
        self.es = elasticsearch_client
        self.index_name = "janusgraph_temporal"
        
    def index_temporal_change(self, entity_id, change_type, timestamp, state):
        doc = {
            "entity_id": entity_id,
            "change_type": change_type,
            "valid_time": timestamp,
            "transaction_time": datetime.utcnow(),
            "state": state,
            "state_hash": hashlib.sha256(json.dumps(state).encode()).hexdigest()
        }
        self.es.index(index=self.index_name, body=doc)
```

### Causal Discovery Implementation

```python
from causalnex import structure
import networkx as nx
import pandas as pd

class CausalDiscoveryEngine:
    def discover_causes(self, 
                       temporal_data: pd.DataFrame,
                       target_variable: str,
                       method: str = "pc") -> nx.DiGraph:
        """
        Discover causal relationships from temporal data
        """
        if method == "pc":
            # PC algorithm implementation
            causal_graph = structure.pc_algorithm(
                temporal_data,
                alpha=0.05,
                max_cond_vars=5
            )
        elif method == "ges":
            # Greedy Equivalence Search
            causal_graph = structure.ges(
                temporal_data,
                score_type="bic"
            )
        elif method == "transfer_entropy":
            # Information-theoretic approach
            causal_graph = self._transfer_entropy_discovery(temporal_data)
            
        return self._enhance_with_temporal_lags(causal_graph, temporal_data)
```

### What-If Simulation Engine

```python
class WhatIfSimulator:
    def __init__(self, temporal_graph, causal_model):
        self.graph = temporal_graph
        self.causal_model = causal_model
        
    async def simulate_component_failure(self, 
                                       component_id: str,
                                       failure_time: datetime,
                                       failure_type: str = "complete") -> Dict:
        """
        Simulate cascading effects of component failure
        """
        # 1. Mark component as failed at given time
        failed_state = self._apply_failure(component_id, failure_type)
        
        # 2. Propagate effects through causal graph
        cascade_queue = PriorityQueue()
        cascade_queue.put((failure_time, component_id, failed_state))
        
        affected_components = {}
        
        while not cascade_queue.empty():
            current_time, current_id, current_state = cascade_queue.get()
            
            # Find causally dependent components
            dependencies = self.causal_model.get_dependents(current_id)
            
            for dep_id, causal_strength, lag_time in dependencies:
                impact_time = current_time + timedelta(milliseconds=lag_time)
                impact_magnitude = self._calculate_impact(
                    current_state, 
                    causal_strength,
                    self.graph.get_vertex(dep_id)
                )
                
                if impact_magnitude > self.impact_threshold:
                    new_state = self._apply_impact(dep_id, impact_magnitude)
                    cascade_queue.put((impact_time, dep_id, new_state))
                    affected_components[dep_id] = {
                        "impact_time": impact_time,
                        "impact_magnitude": impact_magnitude,
                        "new_state": new_state
                    }
                    
        return {
            "initial_failure": component_id,
            "failure_time": failure_time,
            "cascade_analysis": affected_components,
            "total_affected": len(affected_components),
            "critical_path": self._find_critical_path(affected_components)
        }
```

## Configuration

```yaml
temporal_knowledge_graph:
  # Temporal settings
  temporal:
    retention_days: 365
    snapshot_interval: 3600  # seconds
    version_limit: 1000      # max versions per entity
    
  # Causal discovery
  causal:
    discovery_methods:
      - pc
      - ges
      - transfer_entropy
    confidence_threshold: 0.7
    min_observations: 100
    max_lag_days: 7
    
  # Scenario simulation
  simulation:
    max_cascade_depth: 10
    impact_threshold: 0.1
    parallel_scenarios: 50
    monte_carlo_runs: 1000
    
  # Performance tuning
  performance:
    cache_temporal_queries: true
    batch_size: 1000
    async_indexing: true
```

## Use Cases

### 1. Component Failure Analysis
```python
# What happens if the authentication service fails?
result = await tkg.simulate_component_failure(
    component_id="auth-service",
    failure_time=datetime.utcnow(),
    failure_type="complete"
)

print(f"Critical services affected: {result['critical_path']}")
print(f"Estimated recovery time: {result['recovery_estimate']}")
```

### 2. Performance Degradation Root Cause
```python
# Why did response times spike yesterday?
root_causes = await tkg.find_root_cause(
    symptom={
        "metric": "response_time",
        "anomaly_type": "spike",
        "severity": 0.8
    },
    time_window=(yesterday_start, yesterday_end)
)

for cause in root_causes:
    print(f"Probable cause: {cause['entity']} ({cause['confidence']})")
    print(f"Causal chain: {' -> '.join(cause['chain'])}")
```

### 3. Predictive Maintenance
```python
# When will this component likely fail?
prediction = await tkg.predict_failure(
    component_id="storage-node-3",
    horizon_days=30,
    confidence_level=0.95
)

print(f"Failure probability: {prediction['probability']}")
print(f"Expected failure date: {prediction['expected_date']}")
print(f"Preventive actions: {prediction['recommendations']}")
```

## Integration with Existing Services

### 1. Enhanced Graph Intelligence API
```python
# Extension to existing graph-intelligence-service
class EnhancedGraphIntelligenceService(GraphIntelligenceService):
    def __init__(self):
        super().__init__()
        self.temporal_engine = TemporalKnowledgeGraph()
        self.causal_engine = CausalInferenceEngine()
        
    async def get_temporal_insights(self, insight_type: str, params: Dict):
        if insight_type == "evolution":
            return await self.temporal_engine.get_entity_evolution(params)
        elif insight_type == "causality":
            return await self.causal_engine.discover_causes(params)
        elif insight_type == "whatif":
            return await self.temporal_engine.simulate_scenario(params)
```

### 2. Event Stream Integration
```python
# Automatic temporal tracking of all platform events
@app.on_event("startup")
async def setup_temporal_tracking():
    consumer = pulsar_client.subscribe(
        "persistent://public/default/platform-events",
        subscription_name="temporal-indexer"
    )
    
    async def index_temporal_changes():
        while True:
            msg = consumer.receive()
            event = json.loads(msg.data())
            
            # Extract entity changes and index temporally
            await temporal_index.index_change(
                entity_id=event.get("entity_id"),
                change_type=event.get("event_type"),
                timestamp=event.get("timestamp"),
                state=event.get("new_state")
            )
            
            consumer.acknowledge(msg)
```

## Performance Optimization

### 1. Temporal Query Caching
- Cache frequently accessed time slices
- Pre-compute common temporal aggregations
- Use materialized views for time-range queries

### 2. Distributed Causal Discovery
- Parallelize causal algorithms across Spark
- Incremental causal model updates
- Approximate algorithms for large graphs

### 3. Scenario Simulation Optimization
- GPU acceleration for Monte Carlo simulations
- Memoization of sub-graph simulations
- Adaptive sampling for large cascades 