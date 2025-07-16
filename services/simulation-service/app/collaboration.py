"""
Real-time simulation collaboration module

Handles WebSocket connections, CRDT synchronization, and Ignite state management
for multi-user simulation collaboration.
"""

import asyncio
from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta
import json
import logging
import time
import uuid
from collections import defaultdict

from fastapi import WebSocket, WebSocketDisconnect
import numpy as np

from platformq_shared.crdts.simulation_crdt import (
    SimulationCRDT, SimulationOperation, SimulationOperationType
)
from platformq_shared.events import SimulationCollaborationEvent, SimulationStateEvent
from platformq_shared.event_publisher import EventPublisher
from .ignite_manager import SimulationIgniteManager

logger = logging.getLogger(__name__)


class CollaborationSession:
    """Manages a collaborative simulation session"""
    
    def __init__(self, session_id: str, simulation_id: str, ignite_manager: SimulationIgniteManager):
        self.session_id = session_id
        self.simulation_id = simulation_id
        self.ignite = ignite_manager
        
        # Connected users
        self.users: Dict[str, WebSocket] = {}
        self.user_replicas: Dict[str, SimulationCRDT] = {}
        
        # Master CRDT instance
        self.master_crdt = SimulationCRDT("master", simulation_id)
        
        # Performance tracking
        self.last_state_broadcast = time.time()
        self.broadcast_interval = 1.0 / 60  # 60Hz
        self.operations_buffer: List[SimulationOperation] = []
        self.buffer_flush_interval = 0.1  # 100ms
        
        # Metrics
        self.metrics = {
            "operations_per_second": 0,
            "active_users": 0,
            "agent_count": 0,
            "simulation_fps": 0
        }
        
        self.running = False
        self._sync_task = None
        self._metrics_task = None
    
    async def start(self):
        """Start the collaboration session"""
        self.running = True
        
        # Load existing state from Ignite if available
        await self._load_state()
        
        # Start background tasks
        self._sync_task = asyncio.create_task(self._sync_loop())
        self._metrics_task = asyncio.create_task(self._metrics_loop())
        
        logger.info(f"Started collaboration session {self.session_id}")
    
    async def stop(self):
        """Stop the collaboration session"""
        self.running = False
        
        # Cancel background tasks
        if self._sync_task:
            self._sync_task.cancel()
        if self._metrics_task:
            self._metrics_task.cancel()
        
        # Save final state
        await self._save_state()
        
        # Disconnect all users
        for websocket in self.users.values():
            await websocket.close()
        
        logger.info(f"Stopped collaboration session {self.session_id}")
    
    async def add_user(self, user_id: str, websocket: WebSocket):
        """Add a user to the session"""
        self.users[user_id] = websocket
        self.user_replicas[user_id] = SimulationCRDT(user_id, self.simulation_id)
        
        # Send initial state
        state = self.master_crdt.get_state_snapshot()
        await websocket.send_json({
            "type": "initial_state",
            "data": state
        })
        
        # Notify other users
        await self._broadcast_user_event("user_joined", user_id)
        
        logger.info(f"User {user_id} joined session {self.session_id}")
    
    async def remove_user(self, user_id: str):
        """Remove a user from the session"""
        if user_id in self.users:
            del self.users[user_id]
            del self.user_replicas[user_id]
            
            # Notify other users
            await self._broadcast_user_event("user_left", user_id)
            
            logger.info(f"User {user_id} left session {self.session_id}")
    
    async def handle_operation(self, user_id: str, operation_data: Dict[str, Any]):
        """Handle an operation from a user"""
        try:
            # Create operation from data
            operation = self._create_operation_from_data(user_id, operation_data)
            
            # Apply to user's CRDT
            user_crdt = self.user_replicas[user_id]
            user_crdt._apply_operation(operation)
            
            # Add to buffer for batched processing
            self.operations_buffer.append(operation)
            
            # Send acknowledgment
            websocket = self.users.get(user_id)
            if websocket:
                await websocket.send_json({
                    "type": "operation_ack",
                    "operation_id": operation.id
                })
            
        except Exception as e:
            logger.error(f"Error handling operation from {user_id}: {e}")
            if user_id in self.users:
                await self.users[user_id].send_json({
                    "type": "error",
                    "message": str(e)
                })
    
    async def _sync_loop(self):
        """Main synchronization loop"""
        last_buffer_flush = time.time()
        
        while self.running:
            try:
                current_time = time.time()
                
                # Flush operations buffer periodically
                if current_time - last_buffer_flush >= self.buffer_flush_interval:
                    await self._flush_operations_buffer()
                    last_buffer_flush = current_time
                
                # Broadcast state updates at target frequency
                if current_time - self.last_state_broadcast >= self.broadcast_interval:
                    await self._broadcast_state_update()
                    self.last_state_broadcast = current_time
                
                # Small sleep to prevent CPU spinning
                await asyncio.sleep(0.001)
                
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                await asyncio.sleep(0.1)
    
    async def _flush_operations_buffer(self):
        """Process buffered operations"""
        if not self.operations_buffer:
            return
        
        # Apply operations to master CRDT
        for operation in self.operations_buffer:
            self.master_crdt._apply_operation(operation)
        
        # Broadcast operations to all users
        operations_data = [
            {
                "id": op.id,
                "type": op.operation_type.value,
                "data": op.data,
                "vector_clock": op.vector_clock.clock,
                "simulation_tick": op.simulation_tick
            }
            for op in self.operations_buffer
        ]
        
        await self._broadcast({
            "type": "operations_batch",
            "operations": operations_data
        })
        
        # Clear buffer
        self.operations_buffer.clear()
    
    async def _broadcast_state_update(self):
        """Broadcast incremental state updates"""
        # Get active agents (limited for performance)
        active_agents = [
            {
                "id": agent.id,
                "position": agent.position.tolist(),
                "velocity": agent.velocity.tolist()
            }
            for agent in list(self.master_crdt.agents.values())[:1000]
            if not agent.deleted
        ]
        
        update = {
            "type": "state_update",
            "simulation_tick": self.master_crdt.current_tick,
            "simulation_state": self.master_crdt.simulation_state,
            "agents": active_agents,
            "metrics": self.metrics
        }
        
        await self._broadcast(update)
    
    async def _broadcast(self, message: Dict[str, Any]):
        """Broadcast message to all connected users"""
        disconnected = []
        
        for user_id, websocket in self.users.items():
            try:
                await websocket.send_json(message)
            except:
                disconnected.append(user_id)
        
        # Remove disconnected users
        for user_id in disconnected:
            await self.remove_user(user_id)
    
    async def _broadcast_user_event(self, event_type: str, user_id: str):
        """Broadcast user join/leave events"""
        await self._broadcast({
            "type": event_type,
            "user_id": user_id,
            "active_users": list(self.users.keys())
        })
    
    async def _metrics_loop(self):
        """Update metrics periodically"""
        while self.running:
            try:
                self.metrics.update({
                    "active_users": len(self.users),
                    "agent_count": len([a for a in self.master_crdt.agents.values() if not a.deleted]),
                    "simulation_fps": 60 if self.master_crdt.simulation_state == "running" else 0
                })
                
                # Save metrics to Ignite
                await self.ignite.update_session_metrics(self.session_id, self.metrics)
                
                await asyncio.sleep(1.0)
                
            except Exception as e:
                logger.error(f"Error in metrics loop: {e}")
    
    async def _load_state(self):
        """Load session state from Ignite"""
        state_data = await self.ignite.get_simulation_state(self.session_id)
        if state_data:
            self.master_crdt = SimulationCRDT.deserialize(state_data)
            logger.info(f"Loaded existing state for session {self.session_id}")
    
    async def _save_state(self):
        """Save session state to Ignite"""
        state_data = self.master_crdt.serialize()
        await self.ignite.save_simulation_state(self.session_id, state_data)
        logger.info(f"Saved state for session {self.session_id}")
    
    def _create_operation_from_data(self, user_id: str, data: Dict[str, Any]) -> SimulationOperation:
        """Create operation object from WebSocket data"""
        op_type = SimulationOperationType(data["type"])
        
        return SimulationOperation(
            id=data.get("id", str(uuid.uuid4())),
            replica_id=user_id,
            operation_type=op_type,
            timestamp=time.time(),
            simulation_tick=self.master_crdt.current_tick,
            vector_clock=self.user_replicas[user_id].vector_clock,
            target_id=data.get("target_id"),
            data=data.get("data", {}),
            parent_operations=data.get("parent_operations", []),
            branch_id=data.get("branch_id", "main")
        )


class SimulationCollaborationManager:
    """Manages multiple collaboration sessions"""
    
    def __init__(self, ignite_manager: SimulationIgniteManager, event_publisher: EventPublisher):
        self.ignite = ignite_manager
        self.publisher = event_publisher
        self.sessions: Dict[str, CollaborationSession] = {}
        self._cleanup_task = None
        self.running = False
    
    async def start(self):
        """Start the collaboration manager"""
        self.running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Started simulation collaboration manager")
    
    async def stop(self):
        """Stop the collaboration manager"""
        self.running = False
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        # Stop all sessions
        for session in list(self.sessions.values()):
            await session.stop()
        
        logger.info("Stopped simulation collaboration manager")
    
    async def create_session(self, simulation_id: str, user_id: str) -> str:
        """Create a new collaboration session"""
        session_id = str(uuid.uuid4())
        
        session = CollaborationSession(session_id, simulation_id, self.ignite)
        await session.start()
        
        self.sessions[session_id] = session
        
        # Publish session created event
        await self._publish_session_event(
            session_id, simulation_id, "SESSION_CREATED", user_id
        )
        
        logger.info(f"Created collaboration session {session_id} for simulation {simulation_id}")
        return session_id
    
    async def join_session(self, session_id: str, user_id: str, websocket: WebSocket):
        """Join an existing session"""
        if session_id not in self.sessions:
            raise ValueError(f"Session {session_id} not found")
        
        session = self.sessions[session_id]
        await session.add_user(user_id, websocket)
        
        # Publish user joined event
        await self._publish_session_event(
            session_id, session.simulation_id, "USER_JOINED", user_id
        )
    
    async def leave_session(self, session_id: str, user_id: str):
        """Leave a session"""
        if session_id in self.sessions:
            session = self.sessions[session_id]
            await session.remove_user(user_id)
            
            # Publish user left event
            await self._publish_session_event(
                session_id, session.simulation_id, "USER_LEFT", user_id
            )
            
            # Close session if no users left
            if not session.users:
                await self.close_session(session_id)
    
    async def close_session(self, session_id: str):
        """Close a collaboration session"""
        if session_id in self.sessions:
            session = self.sessions[session_id]
            await session.stop()
            del self.sessions[session_id]
            
            # Publish session closed event
            await self._publish_session_event(
                session_id, session.simulation_id, "SESSION_CLOSED", None
            )
            
            logger.info(f"Closed collaboration session {session_id}")
    
    async def handle_websocket_message(self, session_id: str, user_id: str, message: Dict[str, Any]):
        """Handle incoming WebSocket message"""
        if session_id not in self.sessions:
            raise ValueError(f"Session {session_id} not found")
        
        session = self.sessions[session_id]
        
        message_type = message.get("type")
        if message_type == "operation":
            await session.handle_operation(user_id, message.get("data", {}))
        else:
            logger.warning(f"Unknown message type: {message_type}")
    
    async def _cleanup_loop(self):
        """Periodically clean up inactive sessions"""
        while self.running:
            try:
                # Check for inactive sessions (no users for 5 minutes)
                inactive_sessions = []
                current_time = time.time()
                
                for session_id, session in self.sessions.items():
                    if not session.users and (current_time - session.last_state_broadcast) > 300:
                        inactive_sessions.append(session_id)
                
                # Close inactive sessions
                for session_id in inactive_sessions:
                    await self.close_session(session_id)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
    
    async def _publish_session_event(self, session_id: str, simulation_id: str, 
                                   event_type: str, user_id: Optional[str]):
        """Publish session lifecycle event"""
        # Implementation depends on your event schema
        # This is a placeholder
        logger.info(f"Session event: {event_type} for session {session_id}") 