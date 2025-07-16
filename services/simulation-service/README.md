# Simulation Service

## Purpose

The **Simulation Service** provides the API for defining, managing, and launching large-scale agent-based simulations with **real-time multi-user collaboration**. It serves as the user-facing entry point to the platform's simulation engine, allowing multiple users to collaboratively edit, run, and analyze simulations in real-time.

Its primary responsibilities are:
-   Providing an API to create and manage simulation definitions, including agent types and their initial states.
-   **Enabling real-time collaborative editing** of simulations through WebSocket connections.
-   Managing simulation collaboration sessions with CRDT-based conflict resolution.
-   Triggering the start of simulations and publishing corresponding events.
-   Processing agent interactions and state updates through Apache Flink.
-   Tracking simulation lineage and history in JanusGraph.
-   Providing real-time access to the state of agents within a running simulation.
-   Supporting simulation branching, checkpointing, and restoration.

## Architecture

### Real-Time Collaboration Stack

```
┌─────────────────────────────────────────────────────────────┐
│                    WebSocket Clients (60Hz)                   │
├─────────────────────────┬───────────────────────────────────┤
│   Simulation Service    │        Collaboration Layer         │
│  ┌─────────────────┐   │  ┌──────────────────────────────┐ │
│  │ REST API        │   │  │ SimulationCRDT               │ │
│  │ WebSocket API   │   │  │ - Parameters                 │ │
│  │ Checkpoints     │   │  │ - Agents                     │ │
│  └─────────────────┘   │  │ - Control Flow               │ │
│                        │  │ - Branching                  │ │
│                        │  └──────────────────────────────┘ │
├────────────────────────┴───────────────────────────────────┤
│                    State Management Layer                     │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │   Ignite     │  │  Cassandra   │  │     MinIO       │  │
│  │ (Hot State)  │  │ (Warm State) │  │ (Cold Storage)  │  │
│  └──────────────┘  └──────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                    Processing Layer                           │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │    Flink     │  │  JanusGraph  │  │     Pulsar      │  │
│  │ (Analytics)  │  │  (Lineage)   │  │   (Events)      │  │
│  └──────────────┘  └──────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## API Endpoints

### General

-   `GET /`: A simple health check endpoint that returns a status message, indicating the service is running.

### Simulation Management

-   `POST /api/v1/simulations`: Creates a new simulation definition. Requires a JSON payload with `simulation_name` and a list of `agent_definitions`.
-   `POST /api/v1/simulations/{simulation_id}/start`: Initiates a simulation run for a given `simulation_id`. Publishes a `SimulationStartedEvent`.
-   `POST /api/v1/simulations/{simulation_id}/complete`: (Internal) An endpoint for simulation workers to report the completion of a simulation run. Publishes a `SimulationRunCompleted` event.
-   `GET /api/v1/simulations/{simulation_id}/state`: Returns the real-time state of all agents for a running simulation.

### Collaboration Endpoints

-   `POST /api/v1/simulations/{simulation_id}/collaborate`: Creates a new collaboration session for a simulation.
-   `GET /api/v1/simulations/{simulation_id}/sessions`: Lists active collaboration sessions for a simulation.
-   `WS /api/v1/ws/collaborate/{session_id}`: WebSocket endpoint for real-time collaboration.
-   `POST /api/v1/simulations/{simulation_id}/checkpoints`: Creates a checkpoint of the current simulation state.
-   `GET /api/v1/simulations/{simulation_id}/checkpoints`: Lists available checkpoints.
-   `POST /api/v1/simulations/{simulation_id}/restore/{checkpoint_id}`: Restores simulation from a checkpoint.

## WebSocket Protocol

### Connection
```javascript
const ws = new WebSocket('ws://localhost:8000/api/v1/ws/collaborate/{session_id}?user_id={user_id}');
```

### Message Types

#### Client → Server
```javascript
// Parameter change
{
  "type": "operation",
  "data": {
    "type": "set_parameter",
    "data": {
      "name": "gravity",
      "value": 9.8,
      "data_type": "float"
    }
  }
}

// Add agent
{
  "type": "operation",
  "data": {
    "type": "add_agent",
    "data": {
      "agent_type": "particle",
      "position": [0, 0, 0],
      "properties": {...}
    }
  }
}

// Control simulation
{
  "type": "operation",
  "data": {
    "type": "start_simulation"  // or pause_simulation, step_simulation, reset_simulation
  }
}
```

#### Server → Client
```javascript
// Initial state
{
  "type": "initial_state",
  "data": {
    "simulation_id": "...",
    "current_tick": 0,
    "parameters": {...},
    "agent_count": 100,
    "active_agents": [...]
  }
}

// State update (60Hz)
{
  "type": "state_update",
  "simulation_tick": 1234,
  "simulation_state": "running",
  "agents": [...],  // Limited to 1000 agents
  "metrics": {...}
}

// Operations batch
{
  "type": "operations_batch",
  "operations": [...]
}

// User events
{
  "type": "user_joined",  // or user_left
  "user_id": "...",
  "active_users": [...]
}
```

## Event-Driven Architecture

### Events Consumed

-   **Topic**: `simulation-collaboration-events`
-   **Schema**: `SimulationCollaborationEvent`
-   **Description**: Processed by Flink for state aggregation and lineage tracking.

### Events Produced

-   **Topic Base**: `simulation-control-events`
-   **Schema**: `SimulationStartedEvent`
-   **Description**: Published when a user initiates a simulation run, signaling the simulation engine to begin processing.

-   **Topic Base**: `simulation-lifecycle-events`
-   **Schema**: `SimulationRunCompleted`
-   **Description**: Published by simulation workers upon completion of a simulation run, indicating its status (success/failure) and providing a log URI.

-   **Topic Base**: `simulation-state-events`
-   **Schema**: `SimulationStateEvent`
-   **Description**: Periodic state snapshots for processing and analytics.

## CRDT Implementation

The service uses a custom `SimulationCRDT` that supports:

- **Parameter Management**: Conflict-free parameter updates with history tracking
- **Agent Operations**: Add, remove, and modify agents with automatic conflict resolution
- **Control Flow**: Prioritized simulation control commands (pause > play > step)
- **Branching**: Create and switch between simulation branches
- **Vector Clocks**: Causal ordering of operations

## Performance Characteristics

- **Update Frequency**: 60Hz real-time updates
- **Max Concurrent Users**: 50 per simulation session
- **Max Agents**: 100,000 per simulation
- **State Broadcast**: Limited to 1,000 agents per update for performance
- **Operation Buffer**: 100ms flush interval for batching
- **Checkpoint Interval**: Every 1,000 simulation ticks

## How to Run Locally

1.  **Install Requirements**:
    Navigate to the `services/simulation-service` directory and install the Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Start Dependencies**:
    Ensure Ignite, Cassandra, JanusGraph, and Pulsar are running:
    ```bash
    docker-compose -f infra/docker-compose/docker-compose.yml up -d ignite cassandra janusgraph pulsar
    ```

3.  **Configure Ignite**:
    Apply the simulation cache configuration:
    ```bash
    kubectl apply -f infra/ignite/simulation-cache-config.xml
    ```

4.  **Set Environment Variables**:
    ```bash
    export IGNITE_HOSTS=localhost:10800
    export PULSAR_URL=pulsar://localhost:6650
    export CASSANDRA_HOSTS=localhost
    export JANUSGRAPH_URL=ws://localhost:8182/gremlin
    ```

5.  **Run the Service**:
    Start the FastAPI application using Uvicorn from the `services/simulation-service` directory:
    ```bash
    uvicorn app.main:app --host 0.0.0.0 --port 8000 --ws websockets
    ```

## Flink Jobs

The service works with dedicated Flink jobs for processing:

1. **Simulation State Processor**: Aggregates metrics, calculates tick rates, and monitors performance
2. **Agent Interaction Processor**: Detects agent collisions and proximity interactions
3. **Lineage Tracker**: Sends operation history to JanusGraph for provenance tracking

Deploy the Flink job:
```bash
flink run -c simulation_collaboration_job processing/flink/simulation-collaboration-job/src/main.py
```

## JanusGraph Schema

The service tracks simulation lineage with the following graph structure:

### Vertices
- **simulation**: Root simulation definition
- **simulation_session**: Active collaboration session
- **simulation_branch**: Branch in simulation history
- **simulation_parameter**: Parameter with history
- **simulation_agent**: Agent lifecycle tracking
- **simulation_checkpoint**: Saved state snapshots
- **simulation_operation**: User actions
- **simulation_result**: Computed metrics

### Edges
- **HAS_SESSION**: simulation → simulation_session
- **IN_BRANCH**: simulation_session → simulation_branch
- **BRANCHED_FROM**: simulation_branch → simulation_branch
- **PERFORMED_OPERATION**: user → simulation_operation
- **PRODUCED_RESULT**: simulation_session → simulation_result

## Monitoring

Key metrics exposed at `/metrics`:

- `simulation_active_sessions`: Number of active collaboration sessions
- `simulation_connected_users`: Total connected WebSocket clients
- `simulation_operations_per_second`: Operation processing rate
- `simulation_state_broadcast_latency`: Time to broadcast state updates
- `simulation_agent_count`: Total agents across all simulations

## Dependencies

-   `FastAPI`: Web framework for building the API.
-   `websockets`: WebSocket support for real-time collaboration.
-   `pyignite`: Apache Ignite client for state caching.
-   `numpy`: Numerical operations for agent calculations.
-   `SQLAlchemy`: ORM for database interactions.
-   `platformq_shared.base_service`: Shared library for common service functionalities, including security and event publishing.
-   `platformq_shared.events`: Defines the event schemas used by the service.
-   `platformq_shared.crdts.simulation_crdt`: CRDT implementation for conflict resolution. 