import asyncio
from typing import Dict, List, Optional, Set, Any
from datetime import datetime, timedelta
import json
import logging
import time

from platformq_shared.crdts.geometry_crdt import Geometry3DCRDT, GeometryOperation
from platformq_shared.events import GeometryOperationEvent, CADSessionEvent
from platformq_shared.event_publisher import EventPublisher
from .ignite_manager import IgniteManager

logger = logging.getLogger(__name__)


class CRDTSynchronizer:
    """Manages CRDT synchronization between users in CAD sessions"""
    
    def __init__(self, 
                 ignite_manager: IgniteManager,
                 event_publisher: EventPublisher,
                 sync_interval: float = 1.0,
                 checkpoint_interval: int = 100):
        self.ignite = ignite_manager
        self.publisher = event_publisher
        self.sync_interval = sync_interval
        self.checkpoint_interval = checkpoint_interval
        
        # In-memory session tracking
        self.active_sessions: Dict[str, SessionSyncState] = {}
        self.running = False
        self._sync_task = None
        
    async def start_sync_loop(self):
        """Start the background synchronization loop"""
        self.running = True
        self._sync_task = asyncio.create_task(self._sync_loop())
        logger.info("CRDT synchronizer started")
        
    async def stop(self):
        """Stop the synchronization loop"""
        self.running = False
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        logger.info("CRDT synchronizer stopped")
        
    async def _sync_loop(self):
        """Main synchronization loop"""
        while self.running:
            try:
                # Sync all active sessions
                for session_id, sync_state in list(self.active_sessions.items()):
                    await self._sync_session(session_id, sync_state)
                    
                await asyncio.sleep(self.sync_interval)
                
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                await asyncio.sleep(5.0)  # Back off on error
                
    async def _sync_session(self, session_id: str, sync_state: 'SessionSyncState'):
        """Synchronize a single session"""
        try:
            # Check if checkpoint is needed
            if sync_state.operations_since_checkpoint >= self.checkpoint_interval:
                await self._create_checkpoint(session_id, sync_state)
                
            # Persist CRDT state to Ignite periodically
            if sync_state.needs_persistence():
                compressed_state = sync_state.crdt.serialize()
                await self.ignite.update_active_geometry(session_id, compressed_state)
                sync_state.last_persisted = datetime.utcnow()
                
            # Check for stale sessions (no activity for 5 minutes)
            if sync_state.is_stale():
                await self._close_stale_session(session_id)
                
        except Exception as e:
            logger.error(f"Error syncing session {session_id}: {e}")
            
    async def handle_geometry_operation(self, 
                                      session_id: str,
                                      operation: GeometryOperationEvent) -> Dict[str, Any]:
        """Process a geometry operation and return sync data"""
        # Get or create session state
        if session_id not in self.active_sessions:
            await self._initialize_session(session_id)
            
        sync_state = self.active_sessions[session_id]
        
        # Convert event to CRDT operation
        crdt_op = GeometryOperation(
            id=operation.operation_id,
            replica_id=operation.user_id,
            operation_type=operation.operation_type,
            timestamp=operation.timestamp / 1000.0,  # Convert to seconds
            vector_clock=operation.vector_clock,
            target_id=operation.target_object_ids[0] if operation.target_object_ids else None,
            data=json.loads(operation.operation_data) if isinstance(operation.operation_data, str) else operation.operation_data,
            parent_operations=operation.parent_operations
        )
        
        # Apply operation to CRDT
        sync_state.crdt._apply_operation(crdt_op)
        sync_state.operations_since_checkpoint += 1
        sync_state.last_activity = datetime.utcnow()
        
        # Store operation in Ignite log
        await self.ignite.append_operation(session_id, {
            'id': crdt_op.id,
            'replica_id': crdt_op.replica_id,
            'operation_type': crdt_op.operation_type.value,
            'timestamp': crdt_op.timestamp,
            'vector_clock': crdt_op.vector_clock.clock,
            'target_id': crdt_op.target_id,
            'data': crdt_op.data,
            'parent_operations': crdt_op.parent_operations
        })
        
        # Return sync response
        return {
            'accepted': True,
            'vector_clock': sync_state.crdt.vector_clock.clock,
            'operation_count': len(sync_state.crdt.operation_log),
            'geometry_stats': sync_state.crdt.get_geometry_stats()
        }
        
    async def _initialize_session(self, session_id: str):
        """Initialize a new session or restore from cache"""
        # Try to restore from Ignite
        cached_geometry = await self.ignite.get_active_geometry(session_id)
        
        if cached_geometry:
            # Restore from cache
            crdt = Geometry3DCRDT.deserialize(cached_geometry)
            logger.info(f"Restored session {session_id} from cache")
        else:
            # Create new CRDT
            crdt = Geometry3DCRDT(replica_id=session_id)
            logger.info(f"Created new session {session_id}")
            
        self.active_sessions[session_id] = SessionSyncState(
            crdt=crdt,
            session_id=session_id
        )
        
    async def _create_checkpoint(self, session_id: str, sync_state: 'SessionSyncState'):
        """Create a checkpoint for the session"""
        try:
            # Serialize current state
            state_bytes = sync_state.crdt.serialize()
            
            # Generate checkpoint ID
            checkpoint_id = f"{session_id}:ckpt:{int(time.time() * 1000)}"
            
            # Store snapshot in Ignite
            await self.ignite.store_mesh_snapshot(
                snapshot_id=checkpoint_id,
                mesh_data=state_bytes,
                metadata={
                    'session_id': session_id,
                    'operation_count': len(sync_state.crdt.operation_log),
                    'geometry_stats': sync_state.crdt.get_geometry_stats(),
                    'vector_clock': sync_state.crdt.vector_clock.clock
                }
            )
            
            # Publish checkpoint event
            await self.publisher.publish(
                topic_base='cad-session-events',
                tenant_id='default',  # TODO: Get from session
                schema_class=CADSessionEvent,
                data=CADSessionEvent(
                    tenant_id='default',
                    session_id=session_id,
                    asset_id='',  # TODO: Get from session
                    event_type='CHECKPOINT_CREATED',
                    active_users=[],  # TODO: Get from session
                    checkpoint_data={
                        'checkpoint_id': checkpoint_id,
                        'snapshot_uri': f"ignite://{checkpoint_id}",
                        'operation_range': {
                            'start_operation_id': sync_state.last_checkpoint_op_id,
                            'end_operation_id': sync_state.crdt.operation_log[-1].id if sync_state.crdt.operation_log else '',
                            'operation_count': sync_state.operations_since_checkpoint
                        }
                    },
                    timestamp=int(time.time() * 1000)
                )
            )
            
            # Reset checkpoint counter
            sync_state.operations_since_checkpoint = 0
            sync_state.last_checkpoint_op_id = sync_state.crdt.operation_log[-1].id if sync_state.crdt.operation_log else ''
            
            logger.info(f"Created checkpoint {checkpoint_id} for session {session_id}")
            
        except Exception as e:
            logger.error(f"Failed to create checkpoint for session {session_id}: {e}")
            
    async def _close_stale_session(self, session_id: str):
        """Close a stale session"""
        logger.info(f"Closing stale session {session_id}")
        
        # Final persistence
        sync_state = self.active_sessions[session_id]
        compressed_state = sync_state.crdt.serialize()
        await self.ignite.update_active_geometry(session_id, compressed_state)
        
        # Remove from active sessions
        del self.active_sessions[session_id]
        
        # Publish session closed event
        await self.publisher.publish(
            topic_base='cad-session-events',
            tenant_id='default',  # TODO: Get from session
            schema_class=CADSessionEvent,
            data=CADSessionEvent(
                tenant_id='default',
                session_id=session_id,
                asset_id='',  # TODO: Get from session
                event_type='CLOSED',
                active_users=[],
                timestamp=int(time.time() * 1000)
            )
        )
        
    async def get_session_state(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get current state of a session"""
        if session_id not in self.active_sessions:
            # Try to load from cache
            cached_geometry = await self.ignite.get_active_geometry(session_id)
            if not cached_geometry:
                return None
                
            await self._initialize_session(session_id)
            
        sync_state = self.active_sessions[session_id]
        
        return {
            'session_id': session_id,
            'geometry_stats': sync_state.crdt.get_geometry_stats(),
            'vector_clock': sync_state.crdt.vector_clock.clock,
            'operations_since_checkpoint': sync_state.operations_since_checkpoint,
            'last_activity': sync_state.last_activity.isoformat()
        }
        
    async def merge_remote_state(self, session_id: str, remote_state: bytes) -> Dict[str, Any]:
        """Merge a remote CRDT state into the session"""
        if session_id not in self.active_sessions:
            await self._initialize_session(session_id)
            
        sync_state = self.active_sessions[session_id]
        
        # Deserialize and merge remote state
        remote_crdt = Geometry3DCRDT.deserialize(remote_state)
        
        # Get operations to sync
        new_operations = remote_crdt.get_operations_since(sync_state.crdt.vector_clock)
        
        # Merge
        sync_state.crdt.merge(remote_crdt)
        
        # Update activity
        sync_state.last_activity = datetime.utcnow()
        
        return {
            'merged': True,
            'new_operations': len(new_operations),
            'vector_clock': sync_state.crdt.vector_clock.clock,
            'geometry_stats': sync_state.crdt.get_geometry_stats()
        }


class SessionSyncState:
    """Tracks synchronization state for a CAD session"""
    
    def __init__(self, crdt: Geometry3DCRDT, session_id: str):
        self.crdt = crdt
        self.session_id = session_id
        self.last_activity = datetime.utcnow()
        self.last_persisted = datetime.utcnow()
        self.operations_since_checkpoint = 0
        self.last_checkpoint_op_id = ''
        
    def needs_persistence(self, interval_seconds: float = 5.0) -> bool:
        """Check if state needs to be persisted"""
        return (datetime.utcnow() - self.last_persisted).total_seconds() > interval_seconds
        
    def is_stale(self, timeout_minutes: float = 5.0) -> bool:
        """Check if session is stale"""
        return (datetime.utcnow() - self.last_activity).total_seconds() > (timeout_minutes * 60) 