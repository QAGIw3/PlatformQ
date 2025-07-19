"""
Session Manager for Collaboration Platform

Manages collaboration sessions across all domains.
"""

import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
import uuid
import logging

from fastapi import WebSocket, WebSocketDisconnect
from platformq_shared.events import EventPublisher

from ..domains.base import (
    BaseDomainAdapter, DomainRegistry, DomainOperation, 
    DomainState, OperationType
)
from ..clients import StateManagementClient, ComputeAllocationClient


logger = logging.getLogger(__name__)


class UserPresence:
    """Track user presence in a session"""
    
    def __init__(self, user_id: str, name: str):
        self.user_id = user_id
        self.name = name
        self.websocket: Optional[WebSocket] = None
        self.joined_at = datetime.utcnow()
        self.last_seen = datetime.utcnow()
        self.status = "active"
        self.metadata: Dict[str, Any] = {}
    
    def update_activity(self):
        """Update last seen timestamp"""
        self.last_seen = datetime.utcnow()
    
    def is_active(self) -> bool:
        """Check if user is still active"""
        return (datetime.utcnow() - self.last_seen) < timedelta(seconds=30)


class CollaborationSession:
    """Represents an active collaboration session"""
    
    def __init__(self, 
                 session_id: str,
                 domain_type: str,
                 domain_adapter: BaseDomainAdapter,
                 state_client: StateManagementClient,
                 compute_client: ComputeAllocationClient,
                 event_publisher: EventPublisher):
        self.session_id = session_id
        self.domain_type = domain_type
        self.domain_adapter = domain_adapter
        self.state_client = state_client
        self.compute_client = compute_client
        self.event_publisher = event_publisher
        
        self.users: Dict[str, UserPresence] = {}
        self.created_at = datetime.utcnow()
        self.last_activity = datetime.utcnow()
        self.operation_buffer: List[DomainOperation] = []
        self.sync_interval = 0.1  # 100ms
        self.state_version = 0
        self.metadata: Dict[str, Any] = {}
        
        # Background tasks
        self._sync_task: Optional[asyncio.Task] = None
        self._metrics_task: Optional[asyncio.Task] = None
        self._presence_task: Optional[asyncio.Task] = None
        
        # Compute allocation
        self.allocation_id: Optional[str] = None
        self.resource_allocated = False
    
    async def start(self):
        """Start the session and background tasks"""
        # Initialize state
        initial_state = DomainState(
            session_id=self.session_id,
            domain_type=self.domain_type,
            version=0,
            data={},
            metadata={
                "created_at": self.created_at.isoformat(),
                "domain_capabilities": self.domain_adapter.get_capabilities()
            }
        )
        
        # Save initial state
        await self.state_client.put(
            cache="collaboration_states",
            key=self.session_id,
            value=initial_state.to_dict()
        )
        
        # Start background tasks
        self._sync_task = asyncio.create_task(self._sync_loop())
        self._metrics_task = asyncio.create_task(self._metrics_loop())
        self._presence_task = asyncio.create_task(self._presence_loop())
        
        # Publish session started event
        await self.event_publisher.publish(
            topic="collaboration-session-events",
            event_type="SessionStarted",
            data={
                "session_id": self.session_id,
                "domain_type": self.domain_type,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Started collaboration session {self.session_id}")
    
    async def stop(self):
        """Stop the session and cleanup"""
        # Cancel background tasks
        for task in [self._sync_task, self._metrics_task, self._presence_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Release compute resources
        if self.allocation_id:
            await self.compute_client.release_allocation(self.allocation_id)
        
        # Publish session stopped event
        await self.event_publisher.publish(
            topic="collaboration-session-events",
            event_type="SessionStopped",
            data={
                "session_id": self.session_id,
                "duration_seconds": (datetime.utcnow() - self.created_at).total_seconds(),
                "total_users": len(self.users),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Stopped collaboration session {self.session_id}")
    
    async def add_user(self, user_id: str, name: str, websocket: WebSocket) -> Dict[str, Any]:
        """Add a user to the session"""
        # Create user presence
        user = UserPresence(user_id, name)
        user.websocket = websocket
        self.users[user_id] = user
        
        # Get current state
        state = await self._load_state()
        
        # Get user view
        view = self.domain_adapter.get_view_for_user(state, user_id)
        
        # Send initial state
        await websocket.send_json({
            "type": "initial_state",
            "data": view,
            "version": state.version,
            "users": self._get_active_users()
        })
        
        # Broadcast user joined
        await self._broadcast({
            "type": "user_joined",
            "user_id": user_id,
            "user_name": name,
            "active_users": self._get_active_users()
        }, exclude_user=user_id)
        
        # Check if we need compute resources
        await self._check_resource_requirements(state)
        
        # Publish event
        await self.event_publisher.publish(
            topic="collaboration-session-events",
            event_type="UserJoined",
            data={
                "session_id": self.session_id,
                "user_id": user_id,
                "user_name": name,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return view
    
    async def remove_user(self, user_id: str):
        """Remove a user from the session"""
        if user_id not in self.users:
            return
        
        user = self.users.pop(user_id)
        
        # Broadcast user left
        await self._broadcast({
            "type": "user_left",
            "user_id": user_id,
            "user_name": user.name,
            "active_users": self._get_active_users()
        })
        
        # Publish event
        await self.event_publisher.publish(
            topic="collaboration-session-events",
            event_type="UserLeft",
            data={
                "session_id": self.session_id,
                "user_id": user_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    async def handle_operation(self, user_id: str, operation_data: Dict[str, Any]):
        """Handle an operation from a user"""
        # Create operation
        operation = DomainOperation(
            operation_id=str(uuid.uuid4()),
            operation_type=OperationType(operation_data.get("type", "custom")),
            user_id=user_id,
            session_id=self.session_id,
            timestamp=datetime.utcnow(),
            data=operation_data.get("data", {}),
            vector_clock=self._get_vector_clock(),
            parent_operations=operation_data.get("parent_operations", [])
        )
        
        # Add subtype for domain routing
        operation.data["subtype"] = operation_data.get("subtype", "")
        
        # Validate operation
        is_valid, error = self.domain_adapter.validate_operation(operation)
        if not is_valid:
            # Send error to user
            if user_id in self.users and self.users[user_id].websocket:
                await self.users[user_id].websocket.send_json({
                    "type": "operation_error",
                    "operation_id": operation.operation_id,
                    "error": error
                })
            return
        
        # Buffer operation
        self.operation_buffer.append(operation)
        
        # Update activity
        self.last_activity = datetime.utcnow()
        if user_id in self.users:
            self.users[user_id].update_activity()
        
        # Send acknowledgment
        if user_id in self.users and self.users[user_id].websocket:
            await self.users[user_id].websocket.send_json({
                "type": "operation_ack",
                "operation_id": operation.operation_id
            })
    
    async def handle_user_message(self, user_id: str, message: Dict[str, Any]):
        """Handle a message from a user"""
        msg_type = message.get("type")
        
        if msg_type == "operation":
            await self.handle_operation(user_id, message)
        
        elif msg_type == "cursor":
            # Handle cursor update
            await self._handle_cursor_update(user_id, message.get("data", {}))
        
        elif msg_type == "viewport":
            # Handle viewport update
            await self._handle_viewport_update(user_id, message.get("data", {}))
        
        elif msg_type == "ping":
            # Handle ping
            if user_id in self.users:
                self.users[user_id].update_activity()
                if self.users[user_id].websocket:
                    await self.users[user_id].websocket.send_json({"type": "pong"})
    
    async def _sync_loop(self):
        """Background task to sync operations"""
        while True:
            try:
                await asyncio.sleep(self.sync_interval)
                
                if self.operation_buffer:
                    await self._flush_operations()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
    
    async def _flush_operations(self):
        """Flush buffered operations"""
        if not self.operation_buffer:
            return
        
        # Get operations to process
        operations = self.operation_buffer[:]
        self.operation_buffer.clear()
        
        # Load current state
        state = await self._load_state()
        
        # Apply operations
        for operation in operations:
            try:
                state = self.domain_adapter.handle_operation(operation, state)
                self.state_version = state.version
            except Exception as e:
                logger.error(f"Error applying operation {operation.operation_id}: {e}")
                # Send error to user
                if operation.user_id in self.users and self.users[operation.user_id].websocket:
                    await self.users[operation.user_id].websocket.send_json({
                        "type": "operation_error",
                        "operation_id": operation.operation_id,
                        "error": str(e)
                    })
        
        # Save state
        await self._save_state(state)
        
        # Broadcast state update
        await self._broadcast_state_update(state, operations)
        
        # Publish operations to event stream
        for operation in operations:
            await self.event_publisher.publish(
                topic="collaboration-operation-events",
                event_type="OperationApplied",
                data={
                    "session_id": self.session_id,
                    "operation": operation.to_dict(),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    async def _broadcast_state_update(self, state: DomainState, operations: List[DomainOperation]):
        """Broadcast state update to all users"""
        # Create update message
        update = {
            "type": "state_update",
            "version": state.version,
            "operations": [op.to_dict() for op in operations],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send customized view to each user
        for user_id, user in self.users.items():
            if user.websocket:
                try:
                    # Get user-specific view
                    viewport = user.metadata.get("viewport")
                    view = self.domain_adapter.get_view_for_user(state, user_id, viewport)
                    
                    # Add view to update
                    user_update = {**update, "data": view}
                    
                    await user.websocket.send_json(user_update)
                except Exception as e:
                    logger.error(f"Error sending update to user {user_id}: {e}")
    
    async def _broadcast(self, message: Dict[str, Any], exclude_user: Optional[str] = None):
        """Broadcast message to all users"""
        for user_id, user in self.users.items():
            if user_id != exclude_user and user.websocket:
                try:
                    await user.websocket.send_json(message)
                except Exception as e:
                    logger.error(f"Error broadcasting to user {user_id}: {e}")
    
    async def _metrics_loop(self):
        """Background task to publish metrics"""
        while True:
            try:
                await asyncio.sleep(10)  # Every 10 seconds
                
                # Collect metrics
                metrics = {
                    "session_id": self.session_id,
                    "domain_type": self.domain_type,
                    "active_users": len([u for u in self.users.values() if u.is_active()]),
                    "total_users": len(self.users),
                    "state_version": self.state_version,
                    "operations_buffered": len(self.operation_buffer),
                    "uptime_seconds": (datetime.utcnow() - self.created_at).total_seconds(),
                    "resource_allocated": self.resource_allocated,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                # Get domain-specific metrics
                state = await self._load_state()
                domain_metrics = self.domain_adapter.get_metrics(state)
                metrics.update(domain_metrics)
                
                # Publish metrics
                await self.event_publisher.publish(
                    topic="collaboration-metrics-events",
                    event_type="SessionMetrics",
                    data=metrics
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics loop: {e}")
    
    async def _presence_loop(self):
        """Background task to check user presence"""
        while True:
            try:
                await asyncio.sleep(5)  # Every 5 seconds
                
                # Check for inactive users
                inactive_users = []
                for user_id, user in self.users.items():
                    if not user.is_active():
                        user.status = "idle"
                    else:
                        user.status = "active"
                    
                    # Check for disconnected users
                    if user.websocket and user.websocket.client_state.value == 3:  # DISCONNECTED
                        inactive_users.append(user_id)
                
                # Remove disconnected users
                for user_id in inactive_users:
                    await self.remove_user(user_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in presence loop: {e}")
    
    async def _check_resource_requirements(self, state: DomainState):
        """Check and allocate compute resources if needed"""
        if self.resource_allocated:
            return
        
        # Get resource requirements
        requirements = self.domain_adapter.get_resource_requirements(state)
        
        # Check if we need resources
        if (requirements.get("gpu_required", False) or 
            requirements.get("cpu_cores", 0) > 4):
            
            # Allocate resources
            allocation = await self.compute_client.allocate(
                workload_type=f"collaboration_{self.domain_type}",
                workload_id=self.session_id,
                requirements=requirements,
                duration_hours=4  # Initial allocation
            )
            
            if allocation.get("success"):
                self.allocation_id = allocation["allocation_id"]
                self.resource_allocated = True
                logger.info(f"Allocated resources for session {self.session_id}: {allocation}")
    
    async def _handle_cursor_update(self, user_id: str, cursor_data: Dict[str, Any]):
        """Handle cursor position update"""
        if user_id in self.users:
            self.users[user_id].metadata["cursor"] = cursor_data
            
            # Broadcast cursor update
            await self._broadcast({
                "type": "cursor_update",
                "user_id": user_id,
                "cursor": cursor_data
            }, exclude_user=user_id)
    
    async def _handle_viewport_update(self, user_id: str, viewport_data: Dict[str, Any]):
        """Handle viewport update"""
        if user_id in self.users:
            self.users[user_id].metadata["viewport"] = viewport_data
    
    async def _load_state(self) -> DomainState:
        """Load state from state management service"""
        state_dict = await self.state_client.get(
            cache="collaboration_states",
            key=self.session_id
        )
        
        if state_dict:
            return DomainState(**state_dict)
        else:
            # Return initial state
            return DomainState(
                session_id=self.session_id,
                domain_type=self.domain_type,
                version=0,
                data={},
                metadata={}
            )
    
    async def _save_state(self, state: DomainState):
        """Save state to state management service"""
        await self.state_client.put(
            cache="collaboration_states",
            key=self.session_id,
            value=state.to_dict(),
            ttl=3600  # 1 hour TTL
        )
    
    def _get_vector_clock(self) -> Dict[str, int]:
        """Get current vector clock"""
        clock = {}
        for user_id in self.users:
            clock[user_id] = self.state_version
        return clock
    
    def _get_active_users(self) -> List[Dict[str, Any]]:
        """Get list of active users"""
        return [
            {
                "user_id": user.user_id,
                "name": user.name,
                "status": user.status,
                "joined_at": user.joined_at.isoformat()
            }
            for user in self.users.values()
        ]


class SessionManager:
    """Manages all collaboration sessions"""
    
    def __init__(self,
                 domain_registry: DomainRegistry,
                 state_client: StateManagementClient,
                 compute_client: ComputeAllocationClient,
                 event_publisher: EventPublisher):
        self.domain_registry = domain_registry
        self.state_client = state_client
        self.compute_client = compute_client
        self.event_publisher = event_publisher
        
        self.sessions: Dict[str, CollaborationSession] = {}
        self._cleanup_task: Optional[asyncio.Task] = None
    
    async def start(self):
        """Start the session manager"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("Session manager started")
    
    async def stop(self):
        """Stop the session manager"""
        # Stop all sessions
        for session in list(self.sessions.values()):
            await session.stop()
        
        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Session manager stopped")
    
    async def create_session(self, 
                           domain_type: str,
                           metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create a new collaboration session"""
        # Get domain adapter
        domain_adapter = self.domain_registry.get(domain_type)
        
        # Generate session ID
        session_id = str(uuid.uuid4())
        
        # Create session
        session = CollaborationSession(
            session_id=session_id,
            domain_type=domain_type,
            domain_adapter=domain_adapter,
            state_client=self.state_client,
            compute_client=self.compute_client,
            event_publisher=self.event_publisher
        )
        
        if metadata:
            session.metadata = metadata
        
        # Start session
        await session.start()
        
        # Store session
        self.sessions[session_id] = session
        
        return session_id
    
    async def get_session(self, session_id: str) -> Optional[CollaborationSession]:
        """Get a session by ID"""
        return self.sessions.get(session_id)
    
    async def list_sessions(self, 
                          domain_type: Optional[str] = None,
                          active_only: bool = True) -> List[Dict[str, Any]]:
        """List all sessions"""
        sessions = []
        
        for session_id, session in self.sessions.items():
            if domain_type and session.domain_type != domain_type:
                continue
            
            if active_only and not session.users:
                continue
            
            sessions.append({
                "session_id": session_id,
                "domain_type": session.domain_type,
                "created_at": session.created_at.isoformat(),
                "active_users": len([u for u in session.users.values() if u.is_active()]),
                "total_users": len(session.users),
                "state_version": session.state_version,
                "resource_allocated": session.resource_allocated,
                "metadata": session.metadata
            })
        
        return sessions
    
    async def delete_session(self, session_id: str):
        """Delete a session"""
        session = self.sessions.pop(session_id, None)
        if session:
            await session.stop()
    
    async def _cleanup_loop(self):
        """Background task to cleanup inactive sessions"""
        while True:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # Find inactive sessions
                inactive_sessions = []
                for session_id, session in self.sessions.items():
                    # Check if session has been inactive for 30 minutes
                    if (not session.users and 
                        (datetime.utcnow() - session.last_activity) > timedelta(minutes=30)):
                        inactive_sessions.append(session_id)
                
                # Delete inactive sessions
                for session_id in inactive_sessions:
                    logger.info(f"Cleaning up inactive session {session_id}")
                    await self.delete_session(session_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}") 