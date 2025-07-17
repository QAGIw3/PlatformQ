"""WebSocket endpoints for real-time multi-physics simulation updates."""
import asyncio
import json
import logging
import time
from typing import Dict, Any, Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from datetime import datetime

from app.ignite_manager import SimulationIgniteManager
from app.api.deps import verify_websocket_token

router = APIRouter()
logger = logging.getLogger(__name__)

# Active WebSocket connections per simulation
simulation_connections: Dict[str, Dict[str, WebSocket]] = {}


class MultiPhysicsWebSocketManager:
    """Manages WebSocket connections for multi-physics simulations."""
    
    def __init__(self, ignite_manager: SimulationIgniteManager):
        self.ignite = ignite_manager
        self.active_simulations: Dict[str, Dict[str, Any]] = {}
        self.update_interval = 0.1  # 10Hz updates
        
    async def connect(self, simulation_id: str, client_id: str, websocket: WebSocket):
        """Connect a client to simulation updates."""
        await websocket.accept()
        
        if simulation_id not in simulation_connections:
            simulation_connections[simulation_id] = {}
        
        simulation_connections[simulation_id][client_id] = websocket
        
        # Send initial state
        await self.send_initial_state(simulation_id, websocket)
        
        # Start update loop if first connection
        if len(simulation_connections[simulation_id]) == 1:
            asyncio.create_task(self.simulation_update_loop(simulation_id))
    
    async def disconnect(self, simulation_id: str, client_id: str):
        """Disconnect a client from simulation updates."""
        if simulation_id in simulation_connections:
            simulation_connections[simulation_id].pop(client_id, None)
            
            # Clean up if no more connections
            if not simulation_connections[simulation_id]:
                del simulation_connections[simulation_id]
    
    async def send_initial_state(self, simulation_id: str, websocket: WebSocket):
        """Send initial simulation state to newly connected client."""
        try:
            # Get simulation state from Ignite
            state = await self.ignite.get_simulation_state(f"multi_physics_{simulation_id}")
            
            if state:
                await websocket.send_json({
                    "type": "initial_state",
                    "simulation_id": simulation_id,
                    "data": state,
                    "timestamp": datetime.utcnow().isoformat()
                })
            else:
                await websocket.send_json({
                    "type": "error",
                    "message": "Simulation not found",
                    "simulation_id": simulation_id
                })
        except Exception as e:
            logger.error(f"Error sending initial state: {e}")
    
    async def simulation_update_loop(self, simulation_id: str):
        """Main loop for sending simulation updates."""
        logger.info(f"Starting update loop for simulation {simulation_id}")
        
        while simulation_id in simulation_connections:
            try:
                # Get latest simulation state
                state = await self.ignite.get_simulation_state(f"multi_physics_{simulation_id}")
                
                if state:
                    # Extract relevant updates
                    update = self.extract_simulation_update(state)
                    
                    # Send to all connected clients
                    await self.broadcast_update(simulation_id, update)
                
                await asyncio.sleep(self.update_interval)
                
            except Exception as e:
                logger.error(f"Error in update loop: {e}")
                await asyncio.sleep(1)  # Back off on error
    
    def extract_simulation_update(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant update information from simulation state."""
        return {
            "type": "simulation_update",
            "timestamp": datetime.utcnow().isoformat(),
            "iteration": state.get("iteration", 0),
            "converged": state.get("converged", False),
            "domains": {
                domain_id: {
                    "status": domain.get("status"),
                    "metrics": {
                        "max_temperature": domain.get("output_data", {}).get("max_temperature"),
                        "max_stress": domain.get("output_data", {}).get("max_stress"),
                        "max_displacement": domain.get("output_data", {}).get("max_displacement"),
                        "convergence_residual": domain.get("convergence_residual")
                    }
                }
                for domain_id, domain in state.get("domains", {}).items()
            },
            "coupling_metrics": state.get("coupling_metrics", {}),
            "optimization": {
                "objective_value": state.get("optimization", {}).get("objective_value"),
                "improvement": state.get("optimization", {}).get("improvement"),
                "gradient_norm": state.get("optimization", {}).get("gradient_norm")
            } if state.get("optimization") else None
        }
    
    async def broadcast_update(self, simulation_id: str, update: Dict[str, Any]):
        """Broadcast update to all connected clients."""
        if simulation_id not in simulation_connections:
            return
        
        disconnected_clients = []
        
        for client_id, websocket in simulation_connections[simulation_id].items():
            try:
                await websocket.send_json(update)
            except:
                disconnected_clients.append(client_id)
        
        # Clean up disconnected clients
        for client_id in disconnected_clients:
            await self.disconnect(simulation_id, client_id)
    
    async def handle_client_message(self, simulation_id: str, client_id: str, 
                                  message: Dict[str, Any]):
        """Handle messages from clients."""
        msg_type = message.get("type")
        
        if msg_type == "update_parameter":
            # Update simulation parameter
            param_name = message.get("parameter")
            param_value = message.get("value")
            
            await self.update_simulation_parameter(
                simulation_id, param_name, param_value
            )
            
        elif msg_type == "pause_simulation":
            await self.pause_simulation(simulation_id)
            
        elif msg_type == "resume_simulation":
            await self.resume_simulation(simulation_id)
            
        elif msg_type == "request_checkpoint":
            await self.create_checkpoint(simulation_id)
    
    async def update_simulation_parameter(self, simulation_id: str, 
                                        param_name: str, param_value: Any):
        """Update a simulation parameter in real-time."""
        try:
            # Update in Ignite
            state = await self.ignite.get_simulation_state(f"multi_physics_{simulation_id}")
            if state:
                state["parameters"][param_name] = param_value
                await self.ignite.save_simulation_state(
                    f"multi_physics_{simulation_id}", state
                )
                
                # Broadcast parameter change
                await self.broadcast_update(simulation_id, {
                    "type": "parameter_updated",
                    "parameter": param_name,
                    "value": param_value,
                    "timestamp": datetime.utcnow().isoformat()
                })
        except Exception as e:
            logger.error(f"Error updating parameter: {e}")
    
    async def pause_simulation(self, simulation_id: str):
        """Pause a running simulation."""
        # Implementation would interact with the orchestrator
        await self.broadcast_update(simulation_id, {
            "type": "simulation_paused",
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def resume_simulation(self, simulation_id: str):
        """Resume a paused simulation."""
        # Implementation would interact with the orchestrator
        await self.broadcast_update(simulation_id, {
            "type": "simulation_resumed",
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def create_checkpoint(self, simulation_id: str):
        """Create a checkpoint of current simulation state."""
        checkpoint_id = f"checkpoint_{int(time.time())}"
        
        # Save checkpoint to Ignite
        state = await self.ignite.get_simulation_state(f"multi_physics_{simulation_id}")
        if state:
            await self.ignite.save_checkpoint(
                f"multi_physics_{simulation_id}",
                checkpoint_id,
                state
            )
            
            await self.broadcast_update(simulation_id, {
                "type": "checkpoint_created",
                "checkpoint_id": checkpoint_id,
                "timestamp": datetime.utcnow().isoformat()
            })


# Global WebSocket manager instance
ws_manager: Optional[MultiPhysicsWebSocketManager] = None


def get_ws_manager():
    """Get WebSocket manager instance."""
    global ws_manager
    if ws_manager is None:
        # This will be initialized in app startup
        raise RuntimeError("WebSocket manager not initialized")
    return ws_manager


@router.websocket("/ws/{simulation_id}")
async def websocket_multi_physics(
    websocket: WebSocket,
    simulation_id: str,
    client_id: str = None
):
    """WebSocket endpoint for real-time multi-physics simulation updates."""
    if not client_id:
        client_id = f"client_{int(time.time() * 1000)}"
    
    manager = get_ws_manager()
    
    try:
        await manager.connect(simulation_id, client_id, websocket)
        
        # Handle incoming messages
        while True:
            message = await websocket.receive_json()
            await manager.handle_client_message(simulation_id, client_id, message)
            
    except WebSocketDisconnect:
        logger.info(f"Client {client_id} disconnected from simulation {simulation_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await manager.disconnect(simulation_id, client_id)


@router.websocket("/ws/{simulation_id}/collaborative")
async def websocket_collaborative_multi_physics(
    websocket: WebSocket,
    simulation_id: str,
    user_id: str
):
    """WebSocket endpoint for collaborative multi-physics simulation editing."""
    # This integrates with the existing collaboration manager
    # but focuses on multi-physics specific operations
    
    from app.collaboration import CollaborationSession
    
    await websocket.accept()
    
    try:
        # Create or join collaborative session
        session_id = f"multi_physics_{simulation_id}"
        
        # Use existing collaboration infrastructure
        # but with multi-physics specific operations
        
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")
            
            if msg_type == "add_domain":
                # Handle collaborative domain addition
                domain_data = message.get("data")
                # Process and broadcast to other users
                
            elif msg_type == "modify_coupling":
                # Handle collaborative coupling modification
                coupling_data = message.get("data")
                # Process and broadcast
                
            elif msg_type == "update_optimization":
                # Handle collaborative optimization updates
                opt_data = message.get("data")
                # Process and broadcast
                
    except WebSocketDisconnect:
        logger.info(f"User {user_id} disconnected from collaborative session")
    except Exception as e:
        logger.error(f"Collaborative WebSocket error: {e}")


def initialize_ws_manager(ignite_manager: SimulationIgniteManager):
    """Initialize the WebSocket manager."""
    global ws_manager
    ws_manager = MultiPhysicsWebSocketManager(ignite_manager)
    logger.info("Multi-physics WebSocket manager initialized") 