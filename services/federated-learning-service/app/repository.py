"""
Repository for Federated Learning Service

Uses the enhanced repository patterns from platformq-shared.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import json

from platformq_shared import PostgreSQLRepository, CassandraRepository, QueryBuilder
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    FLSessionCreatedEvent,
    FLRoundCompletedEvent,
    FLModelAggregatedEvent,
    FLSessionCompletedEvent,
    DAOProposalCreatedEvent,
    DAOVoteRecordedEvent
)

from .schemas import (
    FLSessionCreate,
    FLSessionUpdate,
    ParticipantRegister,
    ModelUpdate,
    DAOProposalCreate,
    DAOVoteCreate
)

logger = logging.getLogger(__name__)


class FLSessionRepository(PostgreSQLRepository):
    """Repository for federated learning sessions"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory)
        self.event_publisher = event_publisher
        
    def create_session(self, session_data: FLSessionCreate,
                      created_by: str, tenant_id: str) -> Dict[str, Any]:
        """Create a new FL session"""
        with self.get_session() as db:
            session_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO fl_sessions (
                    session_id, session_name, model_type,
                    min_participants, max_participants, current_round,
                    total_rounds, learning_rate, differential_privacy_epsilon,
                    homomorphic_encryption, secure_aggregation,
                    status, created_by, tenant_id, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """
            
            result = db.execute(query, [
                session_id,
                session_data.session_name,
                session_data.model_type,
                session_data.min_participants,
                session_data.max_participants,
                0,  # current_round starts at 0
                session_data.total_rounds,
                session_data.learning_rate,
                session_data.differential_privacy_epsilon,
                session_data.homomorphic_encryption,
                session_data.secure_aggregation,
                'pending',
                created_by,
                tenant_id,
                now,
                now
            ]).fetchone()
            
            session = dict(result)
            
            # Publish event
            if self.event_publisher:
                event = FLSessionCreatedEvent(
                    session_id=session_id,
                    session_name=session_data.session_name,
                    model_type=session_data.model_type,
                    min_participants=session_data.min_participants,
                    max_participants=session_data.max_participants,
                    total_rounds=session_data.total_rounds,
                    privacy_enabled=session_data.differential_privacy_epsilon is not None,
                    created_by=created_by,
                    tenant_id=tenant_id,
                    created_at=now.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/fl-session-created-events",
                    event=event
                )
                
            logger.info(f"Created FL session: {session_id}")
            return session
            
    def update_session(self, session_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update FL session"""
        with self.get_session() as db:
            # Build update query
            set_clauses = []
            values = []
            
            allowed_fields = [
                'current_round', 'status', 'aggregated_model_path',
                'accuracy', 'loss', 'metadata'
            ]
            
            for key, value in updates.items():
                if key in allowed_fields:
                    set_clauses.append(f"{key} = %s")
                    values.append(value)
                    
            if not set_clauses:
                return None
                
            values.extend([datetime.now(timezone.utc), session_id])
            
            query = f"""
                UPDATE fl_sessions 
                SET {', '.join(set_clauses)}, updated_at = %s
                WHERE session_id = %s
                RETURNING *
            """
            
            result = db.execute(query, values).fetchone()
            
            if result:
                session = dict(result)
                
                # Publish event for round completion
                if 'current_round' in updates and self.event_publisher:
                    event = FLRoundCompletedEvent(
                        session_id=session_id,
                        round_number=updates['current_round'],
                        accuracy=updates.get('accuracy'),
                        loss=updates.get('loss'),
                        participants_count=updates.get('participants_count', 0),
                        completed_at=datetime.now(timezone.utc).isoformat()
                    )
                    
                    self.event_publisher.publish_event(
                        topic=f"persistent://platformq/{session['tenant_id']}/fl-round-completed-events",
                        event=event
                    )
                    
                # Publish event for session completion
                if updates.get('status') == 'completed' and self.event_publisher:
                    event = FLSessionCompletedEvent(
                        session_id=session_id,
                        final_accuracy=session.get('accuracy'),
                        final_loss=session.get('loss'),
                        total_rounds_completed=session.get('current_round'),
                        model_path=session.get('aggregated_model_path'),
                        completed_at=datetime.now(timezone.utc).isoformat()
                    )
                    
                    self.event_publisher.publish_event(
                        topic=f"persistent://platformq/{session['tenant_id']}/fl-session-completed-events",
                        event=event
                    )
                    
                return session
                
            return None
            
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get FL session by ID"""
        with self.get_session() as db:
            query = "SELECT * FROM fl_sessions WHERE session_id = %s"
            result = db.execute(query, [session_id]).fetchone()
            return dict(result) if result else None
            
    def list_sessions(self, tenant_id: Optional[str] = None,
                     status: Optional[str] = None,
                     created_by: Optional[str] = None,
                     limit: int = 100) -> List[Dict[str, Any]]:
        """List FL sessions with filters"""
        with self.get_session() as db:
            query = "SELECT * FROM fl_sessions WHERE 1=1"
            params = []
            
            if tenant_id:
                query += " AND tenant_id = %s"
                params.append(tenant_id)
                
            if status:
                query += " AND status = %s"
                params.append(status)
                
            if created_by:
                query += " AND created_by = %s"
                params.append(created_by)
                
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            
            results = db.execute(query, params).fetchall()
            return [dict(row) for row in results]


class ParticipantRepository(PostgreSQLRepository):
    """Repository for FL session participants"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory)
        
    def register_participant(self, session_id: str, participant_id: str,
                           participant_name: str, public_key: str,
                           metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Register participant for FL session"""
        with self.get_session() as db:
            registration_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO fl_participants (
                    registration_id, session_id, participant_id,
                    participant_name, public_key, status,
                    metadata, joined_at, last_update
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (session_id, participant_id) 
                DO UPDATE SET 
                    status = 'active',
                    last_update = EXCLUDED.last_update
                RETURNING *
            """
            
            result = db.execute(query, [
                registration_id,
                session_id,
                participant_id,
                participant_name,
                public_key,
                'active',
                json.dumps(metadata or {}),
                now,
                now
            ]).fetchone()
            
            return dict(result)
            
    def update_participant_status(self, session_id: str, participant_id: str,
                                status: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Update participant status"""
        with self.get_session() as db:
            query = """
                UPDATE fl_participants 
                SET status = %s, metadata = %s, last_update = %s
                WHERE session_id = %s AND participant_id = %s
            """
            
            result = db.execute(query, [
                status,
                json.dumps(metadata or {}),
                datetime.now(timezone.utc),
                session_id,
                participant_id
            ])
            
            return result.rowcount > 0
            
    def get_session_participants(self, session_id: str,
                               status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get participants for a session"""
        with self.get_session() as db:
            query = "SELECT * FROM fl_participants WHERE session_id = %s"
            params = [session_id]
            
            if status:
                query += " AND status = %s"
                params.append(status)
                
            query += " ORDER BY joined_at ASC"
            
            results = db.execute(query, params).fetchall()
            return [dict(row) for row in results]


class ModelUpdateRepository(CassandraRepository):
    """Repository for model updates from participants"""
    
    def __init__(self, db_session_factory):
        super().__init__(
            db_session_factory,
            table_name="fl_model_updates",
            partition_keys=["session_id", "round_number"],
            clustering_keys=["participant_id", "submitted_at"]
        )
        
    def submit_model_update(self, session_id: str, round_number: int,
                          participant_id: str, encrypted_gradients: bytes,
                          metrics: Dict[str, float],
                          computation_time: float) -> Dict[str, Any]:
        """Submit model update from participant"""
        with self.get_session() as session:
            update_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO fl_model_updates (
                    session_id, round_number, participant_id,
                    update_id, encrypted_gradients, metrics,
                    computation_time, submitted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            session.execute(session.prepare(query), [
                uuid.UUID(session_id),
                round_number,
                participant_id,
                update_id,
                encrypted_gradients,
                metrics,
                computation_time,
                now
            ])
            
            return {
                "update_id": update_id,
                "session_id": session_id,
                "round_number": round_number,
                "participant_id": participant_id,
                "submitted_at": now
            }
            
    def get_round_updates(self, session_id: str,
                         round_number: int) -> List[Dict[str, Any]]:
        """Get all model updates for a round"""
        with self.get_session() as session:
            query = """
                SELECT * FROM fl_model_updates 
                WHERE session_id = ? AND round_number = ?
            """
            
            results = session.execute(session.prepare(query), [
                uuid.UUID(session_id),
                round_number
            ])
            
            updates = []
            for row in results:
                updates.append({
                    "update_id": row.update_id,
                    "participant_id": row.participant_id,
                    "encrypted_gradients": row.encrypted_gradients,
                    "metrics": row.metrics,
                    "computation_time": row.computation_time,
                    "submitted_at": row.submitted_at
                })
                
            return updates


class AggregatedModelRepository(CassandraRepository):
    """Repository for aggregated models"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(
            db_session_factory,
            table_name="fl_aggregated_models",
            partition_keys=["session_id"],
            clustering_keys=["round_number"]
        )
        self.event_publisher = event_publisher
        
    def store_aggregated_model(self, session_id: str, round_number: int,
                             model_path: str, model_hash: str,
                             aggregation_method: str,
                             participants_count: int,
                             metrics: Dict[str, float]) -> Dict[str, Any]:
        """Store aggregated model for a round"""
        with self.get_session() as session:
            aggregation_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO fl_aggregated_models (
                    session_id, round_number, aggregation_id,
                    model_path, model_hash, aggregation_method,
                    participants_count, metrics, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            session.execute(session.prepare(query), [
                uuid.UUID(session_id),
                round_number,
                aggregation_id,
                model_path,
                model_hash,
                aggregation_method,
                participants_count,
                metrics,
                now
            ])
            
            result = {
                "aggregation_id": aggregation_id,
                "session_id": session_id,
                "round_number": round_number,
                "model_path": model_path,
                "model_hash": model_hash,
                "created_at": now
            }
            
            # Publish event
            if self.event_publisher:
                event = FLModelAggregatedEvent(
                    session_id=session_id,
                    round_number=round_number,
                    model_path=model_path,
                    model_hash=model_hash,
                    aggregation_method=aggregation_method,
                    participants_count=participants_count,
                    metrics=metrics,
                    aggregated_at=now.isoformat()
                )
                
                # Get tenant_id from session
                # In production, you'd fetch this from session data
                tenant_id = "system"  # Default for now
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/fl-model-aggregated-events",
                    event=event
                )
                
            return result
            
    def get_latest_model(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get latest aggregated model for session"""
        with self.get_session() as session:
            query = """
                SELECT * FROM fl_aggregated_models 
                WHERE session_id = ?
                ORDER BY round_number DESC
                LIMIT 1
            """
            
            result = session.execute(session.prepare(query), [
                uuid.UUID(session_id)
            ]).one()
            
            if result:
                return {
                    "aggregation_id": result.aggregation_id,
                    "round_number": result.round_number,
                    "model_path": result.model_path,
                    "model_hash": result.model_hash,
                    "aggregation_method": result.aggregation_method,
                    "participants_count": result.participants_count,
                    "metrics": result.metrics,
                    "created_at": result.created_at
                }
                
            return None


class DAOProposalRepository(PostgreSQLRepository):
    """Repository for DAO proposals"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory)
        self.event_publisher = event_publisher
        
    def create_proposal(self, proposal_data: DAOProposalCreate,
                       proposer: str, tenant_id: str) -> Dict[str, Any]:
        """Create a new DAO proposal"""
        with self.get_session() as db:
            proposal_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO dao_proposals (
                    proposal_id, proposal_type, title, description,
                    parameters, proposer, status, voting_deadline,
                    execution_deadline, created_by, tenant_id,
                    tx_hash, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """
            
            result = db.execute(query, [
                proposal_id,
                proposal_data.proposal_type,
                proposal_data.title,
                proposal_data.description,
                json.dumps(proposal_data.parameters),
                proposer,
                'active',
                proposal_data.voting_deadline,
                proposal_data.execution_deadline,
                proposer,
                tenant_id,
                proposal_data.tx_hash,
                now,
                now
            ]).fetchone()
            
            proposal = dict(result)
            
            # Publish event
            if self.event_publisher:
                event = DAOProposalCreatedEvent(
                    proposal_id=proposal_id,
                    proposal_type=proposal_data.proposal_type,
                    title=proposal_data.title,
                    proposer=proposer,
                    tenant_id=tenant_id,
                    voting_deadline=proposal_data.voting_deadline.isoformat(),
                    created_at=now.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/dao-proposal-created-events",
                    event=event
                )
                
            return proposal
            
    def update_proposal_status(self, proposal_id: str, status: str,
                             execution_result: Optional[Dict[str, Any]] = None) -> bool:
        """Update proposal status"""
        with self.get_session() as db:
            query = """
                UPDATE dao_proposals 
                SET status = %s, execution_result = %s, updated_at = %s
                WHERE proposal_id = %s
            """
            
            result = db.execute(query, [
                status,
                json.dumps(execution_result) if execution_result else None,
                datetime.now(timezone.utc),
                proposal_id
            ])
            
            return result.rowcount > 0
            
    def get_active_proposals(self, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get active proposals"""
        with self.get_session() as db:
            query = """
                SELECT * FROM dao_proposals 
                WHERE status = 'active' 
                AND voting_deadline > %s
            """
            params = [datetime.now(timezone.utc)]
            
            if tenant_id:
                query += " AND tenant_id = %s"
                params.append(tenant_id)
                
            query += " ORDER BY created_at DESC"
            
            results = db.execute(query, params).fetchall()
            return [dict(row) for row in results]


class DAOVoteRepository(PostgreSQLRepository):
    """Repository for DAO votes"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory)
        self.event_publisher = event_publisher
        
    def record_vote(self, proposal_id: str, voter: str,
                   vote_type: str, voting_power: int,
                   tx_hash: str) -> Dict[str, Any]:
        """Record a vote on a proposal"""
        with self.get_session() as db:
            vote_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO dao_votes (
                    vote_id, proposal_id, voter, vote_type,
                    voting_power, tx_hash, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (proposal_id, voter) DO NOTHING
                RETURNING *
            """
            
            result = db.execute(query, [
                vote_id,
                proposal_id,
                voter,
                vote_type,
                voting_power,
                tx_hash,
                now
            ]).fetchone()
            
            if result:
                vote = dict(result)
                
                # Publish event
                if self.event_publisher:
                    event = DAOVoteRecordedEvent(
                        vote_id=vote_id,
                        proposal_id=proposal_id,
                        voter=voter,
                        vote_type=vote_type,
                        voting_power=voting_power,
                        tx_hash=tx_hash,
                        voted_at=now.isoformat()
                    )
                    
                    self.event_publisher.publish_event(
                        topic=f"persistent://platformq/system/dao-vote-recorded-events",
                        event=event
                    )
                    
                return vote
                
            return None
            
    def get_proposal_votes(self, proposal_id: str) -> Dict[str, Any]:
        """Get vote summary for a proposal"""
        with self.get_session() as db:
            query = """
                SELECT 
                    vote_type,
                    COUNT(*) as vote_count,
                    SUM(voting_power) as total_power
                FROM dao_votes
                WHERE proposal_id = %s
                GROUP BY vote_type
            """
            
            results = db.execute(query, [proposal_id]).fetchall()
            
            summary = {
                "proposal_id": proposal_id,
                "votes": {}
            }
            
            for row in results:
                summary["votes"][row["vote_type"]] = {
                    "count": row["vote_count"],
                    "power": row["total_power"]
                }
                
            return summary 