"""
Repository for Simulation Service

Uses the enhanced repository patterns from platformq-shared.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from cassandra.cluster import Session as CassandraSession

from platformq_shared import CassandraRepository, QueryBuilder
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    SimulationCreatedEvent,
    SimulationStartedEvent,
    SimulationCompletedEvent,
    SimulationFailedEvent,
    AgentCreatedEvent,
    AgentStateUpdatedEvent
)

from .schemas import (
    SimulationCreate,
    SimulationUpdate,
    AgentDefinitionCreate,
    AgentStateCreate,
    SimulationFilter
)

logger = logging.getLogger(__name__)


class SimulationRepository(CassandraRepository):
    """Repository for simulations"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(
            db_session_factory,
            table_name="simulations",
            partition_keys=["tenant_id"],
            clustering_keys=["simulation_id"]
        )
        self.event_publisher = event_publisher
        
    def create(self, simulation_data: SimulationCreate, tenant_id: uuid.UUID) -> Dict[str, Any]:
        """Create a new simulation"""
        with self.get_session() as session:
            simulation_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO simulations (
                    tenant_id, simulation_id, simulation_name, 
                    description, simulation_type, status, 
                    created_by, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            session.execute(session.prepare(query), [
                tenant_id,
                simulation_id,
                simulation_data.simulation_name,
                simulation_data.description,
                simulation_data.simulation_type,
                'defined',
                simulation_data.created_by,
                now,
                now
            ])
            
            simulation = {
                "simulation_id": simulation_id,
                "simulation_name": simulation_data.simulation_name,
                "description": simulation_data.description,
                "simulation_type": simulation_data.simulation_type,
                "status": "defined",
                "created_at": now
            }
            
            # Publish event
            if self.event_publisher:
                event = SimulationCreatedEvent(
                    simulation_id=str(simulation_id),
                    simulation_name=simulation_data.simulation_name,
                    simulation_type=simulation_data.simulation_type,
                    tenant_id=str(tenant_id),
                    created_by=simulation_data.created_by,
                    created_at=now.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/simulation-created-events",
                    event=event
                )
                
            logger.info(f"Created simulation {simulation_id}: {simulation_data.simulation_name}")
            return simulation
            
    def get(self, tenant_id: uuid.UUID, simulation_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Get simulation by ID"""
        with self.get_session() as session:
            query = "SELECT * FROM simulations WHERE tenant_id = ? AND simulation_id = ?"
            result = session.execute(session.prepare(query), [tenant_id, simulation_id]).one()
            
            if result:
                return dict(result)
            return None
            
    def update_status(self, tenant_id: uuid.UUID, simulation_id: uuid.UUID,
                     status: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Update simulation status"""
        with self.get_session() as session:
            now = datetime.now(timezone.utc)
            
            query = """
                UPDATE simulations 
                SET status = ?, updated_at = ?, metadata = ?
                WHERE tenant_id = ? AND simulation_id = ?
            """
            
            session.execute(session.prepare(query), [
                status,
                now,
                metadata or {},
                tenant_id,
                simulation_id
            ])
            
            # Publish appropriate event
            if self.event_publisher:
                if status == "running":
                    event = SimulationStartedEvent(
                        simulation_id=str(simulation_id),
                        tenant_id=str(tenant_id),
                        started_at=now.isoformat()
                    )
                    topic = f"persistent://platformq/{tenant_id}/simulation-started-events"
                elif status == "completed":
                    event = SimulationCompletedEvent(
                        simulation_id=str(simulation_id),
                        tenant_id=str(tenant_id),
                        completed_at=now.isoformat(),
                        results_summary=metadata.get("results_summary", {}) if metadata else {}
                    )
                    topic = f"persistent://platformq/{tenant_id}/simulation-completed-events"
                elif status == "failed":
                    event = SimulationFailedEvent(
                        simulation_id=str(simulation_id),
                        tenant_id=str(tenant_id),
                        error_message=metadata.get("error", "Unknown error") if metadata else "Unknown error",
                        failed_at=now.isoformat()
                    )
                    topic = f"persistent://platformq/{tenant_id}/simulation-failed-events"
                else:
                    return True
                    
                self.event_publisher.publish_event(topic=topic, event=event)
                
            return True
            
    def list_by_tenant(self, tenant_id: uuid.UUID, status: Optional[str] = None,
                      limit: int = 100) -> List[Dict[str, Any]]:
        """List simulations for tenant"""
        with self.get_session() as session:
            if status:
                query = """
                    SELECT * FROM simulations 
                    WHERE tenant_id = ? AND status = ?
                    LIMIT ?
                """
                results = session.execute(session.prepare(query), [tenant_id, status, limit])
            else:
                query = "SELECT * FROM simulations WHERE tenant_id = ? LIMIT ?"
                results = session.execute(session.prepare(query), [tenant_id, limit])
                
            return [dict(row) for row in results]
            
    def get_active_simulations(self, tenant_id: uuid.UUID) -> List[Dict[str, Any]]:
        """Get running simulations"""
        return self.list_by_tenant(tenant_id, status="running")


class AgentDefinitionRepository(CassandraRepository):
    """Repository for agent definitions"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(
            db_session_factory,
            table_name="agent_definitions",
            partition_keys=["tenant_id", "simulation_id"],
            clustering_keys=["agent_definition_id"]
        )
        self.event_publisher = event_publisher
        
    def create(self, agent_data: AgentDefinitionCreate, tenant_id: uuid.UUID,
              simulation_id: uuid.UUID) -> Dict[str, Any]:
        """Create agent definition"""
        with self.get_session() as session:
            agent_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO agent_definitions (
                    tenant_id, simulation_id, agent_definition_id,
                    agent_type_name, behavior_rules, initial_state_distribution,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            session.execute(session.prepare(query), [
                tenant_id,
                simulation_id,
                agent_id,
                agent_data.agent_type_name,
                agent_data.behavior_rules,
                agent_data.initial_state_distribution,
                now
            ])
            
            agent_def = {
                "agent_definition_id": agent_id,
                "agent_type_name": agent_data.agent_type_name,
                "behavior_rules": agent_data.behavior_rules,
                "initial_state_distribution": agent_data.initial_state_distribution,
                "created_at": now
            }
            
            # Publish event
            if self.event_publisher:
                event = AgentCreatedEvent(
                    agent_id=str(agent_id),
                    simulation_id=str(simulation_id),
                    agent_type=agent_data.agent_type_name,
                    tenant_id=str(tenant_id),
                    created_at=now.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/agent-created-events",
                    event=event
                )
                
            return agent_def
            
    def get_by_simulation(self, tenant_id: uuid.UUID,
                         simulation_id: uuid.UUID) -> List[Dict[str, Any]]:
        """Get all agent definitions for a simulation"""
        with self.get_session() as session:
            query = """
                SELECT * FROM agent_definitions 
                WHERE tenant_id = ? AND simulation_id = ?
            """
            results = session.execute(session.prepare(query), [tenant_id, simulation_id])
            return [dict(row) for row in results]


class AgentStateRepository(CassandraRepository):
    """Repository for agent states"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(
            db_session_factory,
            table_name="agent_states",
            partition_keys=["tenant_id", "simulation_id", "agent_id"],
            clustering_keys=["state_timestamp"]
        )
        self.event_publisher = event_publisher
        
    def create_state(self, state_data: AgentStateCreate, tenant_id: uuid.UUID,
                    simulation_id: uuid.UUID, agent_id: uuid.UUID) -> Dict[str, Any]:
        """Record agent state"""
        with self.get_session() as session:
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO agent_states (
                    tenant_id, simulation_id, agent_id,
                    state_timestamp, state_data, metadata
                ) VALUES (?, ?, ?, ?, ?, ?)
            """
            
            session.execute(session.prepare(query), [
                tenant_id,
                simulation_id,
                agent_id,
                now,
                state_data.state_data,
                state_data.metadata or {}
            ])
            
            state = {
                "agent_id": agent_id,
                "state_timestamp": now,
                "state_data": state_data.state_data,
                "metadata": state_data.metadata
            }
            
            # Publish event
            if self.event_publisher:
                event = AgentStateUpdatedEvent(
                    agent_id=str(agent_id),
                    simulation_id=str(simulation_id),
                    state_data=state_data.state_data,
                    timestamp=now.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/agent-state-events",
                    event=event
                )
                
            return state
            
    def get_latest_state(self, tenant_id: uuid.UUID, simulation_id: uuid.UUID,
                        agent_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """Get latest agent state"""
        with self.get_session() as session:
            query = """
                SELECT * FROM agent_states 
                WHERE tenant_id = ? AND simulation_id = ? AND agent_id = ?
                ORDER BY state_timestamp DESC
                LIMIT 1
            """
            result = session.execute(session.prepare(query), [
                tenant_id, simulation_id, agent_id
            ]).one()
            
            if result:
                return dict(result)
            return None
            
    def get_state_history(self, tenant_id: uuid.UUID, simulation_id: uuid.UUID,
                         agent_id: uuid.UUID, limit: int = 100) -> List[Dict[str, Any]]:
        """Get agent state history"""
        with self.get_session() as session:
            query = """
                SELECT * FROM agent_states 
                WHERE tenant_id = ? AND simulation_id = ? AND agent_id = ?
                ORDER BY state_timestamp DESC
                LIMIT ?
            """
            results = session.execute(session.prepare(query), [
                tenant_id, simulation_id, agent_id, limit
            ])
            return [dict(row) for row in results]


class SimulationResultRepository(CassandraRepository):
    """Repository for simulation results"""
    
    def __init__(self, db_session_factory):
        super().__init__(
            db_session_factory,
            table_name="simulation_results",
            partition_keys=["tenant_id", "simulation_id"],
            clustering_keys=["result_id"]
        )
        
    def store_result(self, tenant_id: uuid.UUID, simulation_id: uuid.UUID,
                    result_type: str, result_data: Dict[str, Any],
                    storage_path: Optional[str] = None) -> Dict[str, Any]:
        """Store simulation result"""
        with self.get_session() as session:
            result_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO simulation_results (
                    tenant_id, simulation_id, result_id,
                    result_type, result_data, storage_path,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            session.execute(session.prepare(query), [
                tenant_id,
                simulation_id,
                result_id,
                result_type,
                result_data,
                storage_path,
                now
            ])
            
            return {
                "result_id": result_id,
                "result_type": result_type,
                "created_at": now
            }
            
    def get_results(self, tenant_id: uuid.UUID,
                   simulation_id: uuid.UUID) -> List[Dict[str, Any]]:
        """Get all results for a simulation"""
        with self.get_session() as session:
            query = """
                SELECT * FROM simulation_results 
                WHERE tenant_id = ? AND simulation_id = ?
            """
            results = session.execute(session.prepare(query), [tenant_id, simulation_id])
            return [dict(row) for row in results] 