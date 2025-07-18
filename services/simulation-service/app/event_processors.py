"""
Event Processors for Simulation Service

Handles simulation lifecycle events and federated simulation coordination.
"""

import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

from platformq_shared import (
    EventProcessor,
    event_handler,
    batch_event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients
)
from platformq_events import (
    SimulationStartRequestEvent,
    SimulationStopRequestEvent,
    SimulationDataUpdatedEvent,
    AgentActionEvent,
    MultiPhysicsUpdateEvent,
    FederatedSimulationRequestEvent,
    CollaborationJoinEvent,
    CollaborationUpdateEvent,
    MLModelDeployedEvent,
    SimulationCheckpointEvent
)

from .repository import (
    SimulationRepository,
    AgentDefinitionRepository,
    AgentStateRepository,
    SimulationResultRepository
)
from .collaboration import SimulationCollaborationManager
from .ignite_manager import SimulationIgniteManager
from .federated_ml_integration import SimulationMLOrchestrator

logger = logging.getLogger(__name__)


class SimulationLifecycleProcessor(EventProcessor):
    """Process simulation lifecycle events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 simulation_repo: SimulationRepository,
                 agent_def_repo: AgentDefinitionRepository,
                 agent_state_repo: AgentStateRepository,
                 result_repo: SimulationResultRepository,
                 ignite_manager: SimulationIgniteManager,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.simulation_repo = simulation_repo
        self.agent_def_repo = agent_def_repo
        self.agent_state_repo = agent_state_repo
        self.result_repo = result_repo
        self.ignite_manager = ignite_manager
        self.service_clients = service_clients
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting simulation lifecycle processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping simulation lifecycle processor")
        
    @event_handler("persistent://platformq/*/simulation-start-request-events", SimulationStartRequestEvent)
    async def handle_simulation_start(self, event: SimulationStartRequestEvent, msg):
        """Start a simulation"""
        try:
            # Verify simulation exists and is in correct state
            simulation = self.simulation_repo.get(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id)
            )
            
            if not simulation:
                logger.error(f"Simulation not found: {event.simulation_id}")
                return ProcessingResult(status=ProcessingStatus.FAILED)
                
            if simulation["status"] != "defined":
                logger.warning(f"Simulation {event.simulation_id} in wrong state: {simulation['status']}")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Get agent definitions
            agent_defs = self.agent_def_repo.get_by_simulation(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id)
            )
            
            # Initialize simulation in Ignite
            cache_name = f"sim_{event.simulation_id}"
            await self.ignite_manager.create_simulation_cache(cache_name)
            
            # Initialize agents
            for agent_def in agent_defs:
                # Create initial agent states
                initial_state = json.loads(agent_def["initial_state_distribution"])
                
                # Generate agent instances based on distribution
                num_agents = initial_state.get("count", 10)
                for i in range(num_agents):
                    agent_id = uuid.uuid4()
                    
                    # Store in Ignite for fast access
                    await self.ignite_manager.put_agent_state(
                        cache_name,
                        str(agent_id),
                        {
                            "agent_type": agent_def["agent_type_name"],
                            "state": initial_state.get("initial_values", {}),
                            "rules": agent_def["behavior_rules"]
                        }
                    )
                    
                    # Record initial state
                    self.agent_state_repo.create_state(
                        state_data={
                            "state_data": initial_state.get("initial_values", {}),
                            "metadata": {"initialized": True}
                        },
                        tenant_id=uuid.UUID(event.tenant_id),
                        simulation_id=uuid.UUID(event.simulation_id),
                        agent_id=agent_id
                    )
                    
            # Update simulation status
            self.simulation_repo.update_status(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id),
                "running",
                metadata={
                    "started_at": datetime.utcnow().isoformat(),
                    "ignite_cache": cache_name,
                    "total_agents": sum(json.loads(d["initial_state_distribution"]).get("count", 10) for d in agent_defs)
                }
            )
            
            logger.info(f"Started simulation {event.simulation_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error starting simulation: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/simulation-stop-request-events", SimulationStopRequestEvent)
    async def handle_simulation_stop(self, event: SimulationStopRequestEvent, msg):
        """Stop a running simulation"""
        try:
            # Update status
            self.simulation_repo.update_status(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id),
                "stopping",
                metadata={"stop_requested_at": datetime.utcnow().isoformat()}
            )
            
            # Collect final results
            cache_name = f"sim_{event.simulation_id}"
            final_states = await self.ignite_manager.get_all_agent_states(cache_name)
            
            # Store results
            self.result_repo.store_result(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id),
                "final_states",
                {"agent_states": final_states},
                storage_path=f"simulations/{event.simulation_id}/final_states.json"
            )
            
            # Cleanup Ignite cache
            await self.ignite_manager.destroy_simulation_cache(cache_name)
            
            # Update final status
            self.simulation_repo.update_status(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id),
                "completed",
                metadata={
                    "stopped_at": datetime.utcnow().isoformat(),
                    "results_stored": True
                }
            )
            
            logger.info(f"Stopped simulation {event.simulation_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error stopping simulation: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @batch_event_handler(
        "persistent://platformq/*/agent-action-events",
        AgentActionEvent,
        max_batch_size=100,
        max_wait_time_ms=1000
    )
    async def handle_agent_actions(self, events: List[AgentActionEvent], msgs):
        """Process batch of agent actions"""
        try:
            # Group by simulation for efficiency
            by_simulation = {}
            for event in events:
                sim_id = event.simulation_id
                if sim_id not in by_simulation:
                    by_simulation[sim_id] = []
                by_simulation[sim_id].append(event)
                
            # Process each simulation's actions
            for sim_id, sim_events in by_simulation.items():
                cache_name = f"sim_{sim_id}"
                
                # Batch update agent states in Ignite
                updates = {}
                for event in sim_events:
                    # Get current state
                    current_state = await self.ignite_manager.get_agent_state(
                        cache_name,
                        event.agent_id
                    )
                    
                    if current_state:
                        # Apply action
                        new_state = self._apply_action(
                            current_state,
                            event.action,
                            event.parameters
                        )
                        updates[event.agent_id] = new_state
                        
                # Batch update in Ignite
                await self.ignite_manager.batch_update_agents(cache_name, updates)
                
                # Sample some updates to record in history
                sample_size = min(10, len(sim_events))
                for event in sim_events[:sample_size]:
                    self.agent_state_repo.create_state(
                        state_data={
                            "state_data": updates.get(event.agent_id, {}),
                            "metadata": {
                                "action": event.action,
                                "parameters": event.parameters
                            }
                        },
                        tenant_id=uuid.UUID(event.tenant_id),
                        simulation_id=uuid.UUID(sim_id),
                        agent_id=uuid.UUID(event.agent_id)
                    )
                    
            logger.info(f"Processed {len(events)} agent actions")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing agent actions: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/simulation-checkpoint-events", SimulationCheckpointEvent)
    async def handle_checkpoint(self, event: SimulationCheckpointEvent, msg):
        """Create simulation checkpoint"""
        try:
            cache_name = f"sim_{event.simulation_id}"
            
            # Get all current states
            all_states = await self.ignite_manager.get_all_agent_states(cache_name)
            
            # Store checkpoint
            checkpoint_data = {
                "checkpoint_id": str(uuid.uuid4()),
                "simulation_id": event.simulation_id,
                "timestamp": datetime.utcnow().isoformat(),
                "agent_states": all_states,
                "metadata": event.metadata or {}
            }
            
            # Store in MinIO
            storage_path = f"simulations/{event.simulation_id}/checkpoints/{checkpoint_data['checkpoint_id']}.json"
            
            # Store reference in database
            self.result_repo.store_result(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id),
                "checkpoint",
                checkpoint_data,
                storage_path=storage_path
            )
            
            logger.info(f"Created checkpoint for simulation {event.simulation_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error creating checkpoint: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    def _apply_action(self, current_state: Dict[str, Any], action: str,
                     parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Apply action to agent state"""
        # This is a simplified action processor
        # In reality, this would use the behavior rules
        new_state = current_state.copy()
        
        if action == "move":
            position = new_state.get("position", {"x": 0, "y": 0})
            position["x"] += parameters.get("dx", 0)
            position["y"] += parameters.get("dy", 0)
            new_state["position"] = position
            
        elif action == "interact":
            interactions = new_state.get("interactions", 0)
            new_state["interactions"] = interactions + 1
            
        elif action == "update_property":
            property_name = parameters.get("property")
            value = parameters.get("value")
            if property_name:
                new_state[property_name] = value
                
        new_state["last_action"] = action
        new_state["last_update"] = datetime.utcnow().isoformat()
        
        return new_state


class FederatedSimulationProcessor(EventProcessor):
    """Process federated simulation events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 simulation_repo: SimulationRepository,
                 ml_orchestrator: SimulationMLOrchestrator,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.simulation_repo = simulation_repo
        self.ml_orchestrator = ml_orchestrator
        self.service_clients = service_clients
        
    @event_handler("persistent://platformq/*/federated-simulation-request-events", FederatedSimulationRequestEvent)
    async def handle_federated_request(self, event: FederatedSimulationRequestEvent, msg):
        """Initialize federated simulation"""
        try:
            # Create parent simulation
            parent_sim = self.simulation_repo.create(
                simulation_data={
                    "simulation_name": event.simulation_name,
                    "description": f"Federated simulation with {len(event.participant_nodes)} nodes",
                    "simulation_type": "federated",
                    "created_by": event.initiator_id
                },
                tenant_id=uuid.UUID(event.tenant_id)
            )
            
            # Initialize ML orchestrator for federated learning
            if event.enable_federated_ml:
                await self.ml_orchestrator.initialize_federation(
                    str(parent_sim["simulation_id"]),
                    event.participant_nodes,
                    event.ml_config
                )
                
            # Notify participant nodes
            for node in event.participant_nodes:
                # Send invitation to join federation
                await self.service_clients.post(
                    node["service_url"],
                    "/api/v1/simulations/join-federation",
                    json={
                        "federation_id": str(parent_sim["simulation_id"]),
                        "coordinator_url": event.coordinator_url,
                        "role": node["role"],
                        "config": event.federation_config
                    }
                )
                
            logger.info(f"Initialized federated simulation {parent_sim['simulation_id']}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error initializing federated simulation: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/ml-model-deployed-events", MLModelDeployedEvent)
    async def handle_ml_model_deployed(self, event: MLModelDeployedEvent, msg):
        """Handle ML model deployment in simulation"""
        try:
            # Update simulation with deployed model
            simulation = self.simulation_repo.get(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id)
            )
            
            if not simulation:
                logger.warning(f"Simulation {event.simulation_id} not found")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Deploy model to simulation agents
            if simulation["status"] == "running":
                cache_name = f"sim_{event.simulation_id}"
                
                # Update agent behaviors with new model
                await self.ml_orchestrator.deploy_model_to_agents(
                    cache_name,
                    event.model_id,
                    event.model_version,
                    event.target_agent_types
                )
                
                logger.info(f"Deployed model {event.model_id} to simulation {event.simulation_id}")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error deploying ML model: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


class MultiPhysicsSimulationProcessor(EventProcessor):
    """Process multi-physics simulation events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 simulation_repo: SimulationRepository,
                 result_repo: SimulationResultRepository,
                 ignite_manager: SimulationIgniteManager):
        super().__init__(service_name, pulsar_url)
        self.simulation_repo = simulation_repo
        self.result_repo = result_repo
        self.ignite_manager = ignite_manager
        
    @event_handler("persistent://platformq/*/multi-physics-update-events", MultiPhysicsUpdateEvent)
    async def handle_physics_update(self, event: MultiPhysicsUpdateEvent, msg):
        """Handle multi-physics simulation updates"""
        try:
            # Store physics calculation results
            self.result_repo.store_result(
                uuid.UUID(event.tenant_id),
                uuid.UUID(event.simulation_id),
                f"physics_{event.physics_type}",
                {
                    "timestep": event.timestep,
                    "results": event.results,
                    "solver": event.solver_info
                }
            )
            
            # Update simulation state if needed
            if event.requires_state_update:
                cache_name = f"sim_{event.simulation_id}"
                
                # Apply physics results to affected agents/entities
                for entity_id, physics_data in event.entity_updates.items():
                    current_state = await self.ignite_manager.get_agent_state(
                        cache_name,
                        entity_id
                    )
                    
                    if current_state:
                        # Update with physics data
                        current_state["physics_state"] = physics_data
                        await self.ignite_manager.put_agent_state(
                            cache_name,
                            entity_id,
                            current_state
                        )
                        
            logger.info(f"Processed {event.physics_type} update for simulation {event.simulation_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing physics update: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


class CollaborationEventProcessor(EventProcessor):
    """Process simulation collaboration events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 collab_manager: SimulationCollaborationManager):
        super().__init__(service_name, pulsar_url)
        self.collab_manager = collab_manager
        
    @event_handler("persistent://platformq/*/collaboration-join-events", CollaborationJoinEvent)
    async def handle_collaboration_join(self, event: CollaborationJoinEvent, msg):
        """Handle user joining simulation collaboration"""
        try:
            await self.collab_manager.add_participant(
                event.simulation_id,
                event.user_id,
                event.user_name,
                event.role
            )
            
            logger.info(f"User {event.user_id} joined simulation {event.simulation_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling collaboration join: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/collaboration-update-events", CollaborationUpdateEvent)
    async def handle_collaboration_update(self, event: CollaborationUpdateEvent, msg):
        """Handle collaborative updates to simulation"""
        try:
            # Process collaborative change
            await self.collab_manager.process_update(
                event.simulation_id,
                event.user_id,
                event.update_type,
                event.update_data
            )
            
            # Broadcast to other participants
            await self.collab_manager.broadcast_update(
                event.simulation_id,
                event.user_id,
                event.update_type,
                event.update_data
            )
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling collaboration update: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            ) 