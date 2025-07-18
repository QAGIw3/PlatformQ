"""
Event Processors for Federated Learning Service

Handles federated learning session lifecycle and DAO governance events.
"""

import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import asyncio

from platformq_shared import (
    EventProcessor,
    event_handler,
    batch_event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients
)
from platformq_events import (
    FLSessionRequestEvent,
    ParticipantJoinedEvent,
    ModelUpdateSubmittedEvent,
    AggregationRequestEvent,
    RoundCompleteEvent,
    DAOProposalSubmittedEvent,
    DAOVoteSubmittedEvent,
    PrivacyViolationEvent,
    TrustScoreUpdateEvent
)

from .repository import (
    FLSessionRepository,
    ParticipantRepository,
    ModelUpdateRepository,
    AggregatedModelRepository,
    DAOProposalRepository,
    DAOVoteRepository
)
from .orchestrator import FederatedLearningOrchestrator
from .aggregation_service import AggregationService
from .privacy_engine import PrivacyEngine
from .dao.federated_dao_governance import FederatedLearningDAO

logger = logging.getLogger(__name__)


class FLSessionProcessor(EventProcessor):
    """Process federated learning session events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 session_repo: FLSessionRepository,
                 participant_repo: ParticipantRepository,
                 orchestrator: FederatedLearningOrchestrator,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.session_repo = session_repo
        self.participant_repo = participant_repo
        self.orchestrator = orchestrator
        self.service_clients = service_clients
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting FL session processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping FL session processor")
        
    @event_handler("persistent://platformq/*/fl-session-request-events", FLSessionRequestEvent)
    async def handle_session_request(self, event: FLSessionRequestEvent, msg):
        """Handle FL session creation request"""
        try:
            # Validate request
            if event.min_participants < 2:
                logger.error("FL session requires at least 2 participants")
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    message="Minimum 2 participants required"
                )
                
            # Create session
            session = self.session_repo.create_session(
                session_data={
                    "session_name": event.session_name,
                    "model_type": event.model_type,
                    "min_participants": event.min_participants,
                    "max_participants": event.max_participants,
                    "total_rounds": event.total_rounds,
                    "learning_rate": event.learning_rate,
                    "differential_privacy_epsilon": event.privacy_config.get("epsilon"),
                    "homomorphic_encryption": event.privacy_config.get("homomorphic_encryption", False),
                    "secure_aggregation": event.privacy_config.get("secure_aggregation", True)
                },
                created_by=event.created_by,
                tenant_id=event.tenant_id
            )
            
            # Initialize orchestrator for session
            await self.orchestrator.initialize_session(
                session["session_id"],
                event.model_type,
                event.privacy_config
            )
            
            # Notify potential participants
            await self._notify_participants(session, event.tenant_id)
            
            logger.info(f"Created FL session: {session['session_id']}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling session request: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/participant-joined-events", ParticipantJoinedEvent)
    async def handle_participant_joined(self, event: ParticipantJoinedEvent, msg):
        """Handle participant joining FL session"""
        try:
            # Verify session exists and is accepting participants
            session = self.session_repo.get_session(event.session_id)
            if not session:
                logger.error(f"Session not found: {event.session_id}")
                return ProcessingResult(status=ProcessingStatus.FAILED)
                
            if session["status"] not in ["pending", "active"]:
                logger.warning(f"Session {event.session_id} not accepting participants")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Register participant
            participant = self.participant_repo.register_participant(
                session_id=event.session_id,
                participant_id=event.participant_id,
                participant_name=event.participant_name,
                public_key=event.public_key,
                metadata={
                    "compute_resources": event.compute_resources,
                    "data_samples": event.data_samples,
                    "trust_score": event.trust_score
                }
            )
            
            # Check if we have enough participants to start
            participants = self.participant_repo.get_session_participants(
                event.session_id,
                status="active"
            )
            
            if len(participants) >= session["min_participants"] and session["status"] == "pending":
                # Start the session
                self.session_repo.update_session(
                    event.session_id,
                    {"status": "active", "current_round": 1}
                )
                
                # Initialize first round
                await self.orchestrator.start_round(
                    event.session_id,
                    round_number=1,
                    participants=[p["participant_id"] for p in participants]
                )
                
                logger.info(f"Started FL session {event.session_id} with {len(participants)} participants")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling participant join: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _notify_participants(self, session: Dict[str, Any], tenant_id: str):
        """Notify potential participants about new session"""
        # Send notifications to eligible participants
        await self.service_clients.post(
            "http://notification-service:8000/api/v1/notifications/broadcast",
            json={
                "tenant_id": tenant_id,
                "notification_type": "fl_session_available",
                "title": f"New FL Session: {session['session_name']}",
                "message": f"A new federated learning session for {session['model_type']} is available",
                "data": {
                    "session_id": session["session_id"],
                    "min_participants": session["min_participants"],
                    "max_participants": session["max_participants"],
                    "total_rounds": session["total_rounds"]
                }
            }
        )


class ModelAggregationProcessor(EventProcessor):
    """Process model aggregation events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 model_update_repo: ModelUpdateRepository,
                 aggregated_model_repo: AggregatedModelRepository,
                 session_repo: FLSessionRepository,
                 aggregation_service: AggregationService,
                 privacy_engine: PrivacyEngine):
        super().__init__(service_name, pulsar_url)
        self.model_update_repo = model_update_repo
        self.aggregated_model_repo = aggregated_model_repo
        self.session_repo = session_repo
        self.aggregation_service = aggregation_service
        self.privacy_engine = privacy_engine
        
    @event_handler("persistent://platformq/*/model-update-submitted-events", ModelUpdateSubmittedEvent)
    async def handle_model_update(self, event: ModelUpdateSubmittedEvent, msg):
        """Handle model update submission from participant"""
        try:
            # Validate privacy constraints
            if not await self.privacy_engine.validate_update(
                event.encrypted_gradients,
                event.noise_scale,
                event.clipping_threshold
            ):
                # Report privacy violation
                await self._report_privacy_violation(event)
                return ProcessingResult(status=ProcessingStatus.FAILED)
                
            # Store model update
            update = self.model_update_repo.submit_model_update(
                session_id=event.session_id,
                round_number=event.round_number,
                participant_id=event.participant_id,
                encrypted_gradients=event.encrypted_gradients,
                metrics={
                    "loss": event.local_loss,
                    "accuracy": event.local_accuracy,
                    "samples": event.num_samples
                },
                computation_time=event.computation_time
            )
            
            # Check if we have all updates for this round
            await self._check_round_completion(event.session_id, event.round_number)
            
            logger.info(f"Stored model update from {event.participant_id} for round {event.round_number}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling model update: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/aggregation-request-events", AggregationRequestEvent)
    async def handle_aggregation_request(self, event: AggregationRequestEvent, msg):
        """Handle request to aggregate models for a round"""
        try:
            # Get all updates for the round
            updates = self.model_update_repo.get_round_updates(
                event.session_id,
                event.round_number
            )
            
            if len(updates) < event.min_updates:
                logger.warning(f"Not enough updates for aggregation: {len(updates)} < {event.min_updates}")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Perform secure aggregation
            aggregated_model = await self.aggregation_service.aggregate_models(
                updates,
                aggregation_method=event.aggregation_method,
                weights=[u["metrics"]["samples"] for u in updates]
            )
            
            # Apply differential privacy if configured
            if event.apply_differential_privacy:
                aggregated_model = await self.privacy_engine.add_noise(
                    aggregated_model,
                    epsilon=event.privacy_epsilon,
                    delta=event.privacy_delta
                )
                
            # Store aggregated model
            model_hash = await self._compute_model_hash(aggregated_model)
            model_path = f"fl-models/{event.session_id}/round_{event.round_number}/aggregated_model.pt"
            
            # Save to storage
            await self._save_model(aggregated_model, model_path)
            
            # Store metadata
            result = self.aggregated_model_repo.store_aggregated_model(
                session_id=event.session_id,
                round_number=event.round_number,
                model_path=model_path,
                model_hash=model_hash,
                aggregation_method=event.aggregation_method,
                participants_count=len(updates),
                metrics={
                    "avg_loss": sum(u["metrics"]["loss"] for u in updates) / len(updates),
                    "avg_accuracy": sum(u["metrics"]["accuracy"] for u in updates) / len(updates),
                    "total_samples": sum(u["metrics"]["samples"] for u in updates)
                }
            )
            
            # Update session
            self.session_repo.update_session(
                event.session_id,
                {
                    "current_round": event.round_number,
                    "accuracy": result["metrics"]["avg_accuracy"],
                    "loss": result["metrics"]["avg_loss"],
                    "aggregated_model_path": model_path
                }
            )
            
            # Check if session is complete
            session = self.session_repo.get_session(event.session_id)
            if event.round_number >= session["total_rounds"]:
                await self._complete_session(event.session_id)
            else:
                # Start next round
                await self._start_next_round(event.session_id, event.round_number + 1)
                
            logger.info(f"Completed aggregation for round {event.round_number}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling aggregation request: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _check_round_completion(self, session_id: str, round_number: int):
        """Check if all participants have submitted updates"""
        session = self.session_repo.get_session(session_id)
        participants = self.participant_repo.get_session_participants(
            session_id,
            status="active"
        )
        
        updates = self.model_update_repo.get_round_updates(session_id, round_number)
        
        if len(updates) >= len(participants) * 0.8:  # 80% threshold
            # Trigger aggregation
            # In production, publish aggregation request event
            logger.info(f"Round {round_number} ready for aggregation")
            
    async def _report_privacy_violation(self, event: ModelUpdateSubmittedEvent):
        """Report privacy violation"""
        logger.error(f"Privacy violation detected from participant {event.participant_id}")
        # In production, publish privacy violation event
        
    async def _compute_model_hash(self, model: bytes) -> str:
        """Compute hash of model for integrity"""
        import hashlib
        return hashlib.sha256(model).hexdigest()
        
    async def _save_model(self, model: bytes, path: str):
        """Save model to storage"""
        # In production, save to MinIO or other storage
        logger.info(f"Saving model to {path}")
        
    async def _complete_session(self, session_id: str):
        """Complete FL session"""
        self.session_repo.update_session(
            session_id,
            {"status": "completed"}
        )
        logger.info(f"FL session {session_id} completed")
        
    async def _start_next_round(self, session_id: str, round_number: int):
        """Start next round of FL"""
        # In production, notify participants about next round
        logger.info(f"Starting round {round_number} for session {session_id}")


class DAOGovernanceProcessor(EventProcessor):
    """Process DAO governance events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 proposal_repo: DAOProposalRepository,
                 vote_repo: DAOVoteRepository,
                 fl_dao: FederatedLearningDAO,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.proposal_repo = proposal_repo
        self.vote_repo = vote_repo
        self.fl_dao = fl_dao
        self.service_clients = service_clients
        
    @event_handler("persistent://platformq/*/dao-proposal-submitted-events", DAOProposalSubmittedEvent)
    async def handle_proposal_submitted(self, event: DAOProposalSubmittedEvent, msg):
        """Handle DAO proposal submission"""
        try:
            # Create proposal on blockchain
            result = await self.fl_dao.create_proposal(
                proposal_type=event.proposal_type,
                title=event.title,
                description=event.description,
                parameters=event.parameters,
                proposer_address=event.proposer_address,
                private_key=event.private_key
            )
            
            if result["success"]:
                # Store in database
                proposal = self.proposal_repo.create_proposal(
                    proposal_data={
                        "proposal_type": event.proposal_type,
                        "title": event.title,
                        "description": event.description,
                        "parameters": event.parameters,
                        "voting_deadline": result["voting_deadline"],
                        "execution_deadline": result["execution_deadline"],
                        "tx_hash": result["tx_hash"]
                    },
                    proposer=event.proposer_address,
                    tenant_id=event.tenant_id
                )
                
                # Notify token holders
                await self._notify_token_holders(proposal, event.tenant_id)
                
                logger.info(f"Created DAO proposal: {proposal['proposal_id']}")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling proposal submission: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/dao-vote-submitted-events", DAOVoteSubmittedEvent)
    async def handle_vote_submitted(self, event: DAOVoteSubmittedEvent, msg):
        """Handle DAO vote submission"""
        try:
            # Submit vote on blockchain
            result = await self.fl_dao.vote_on_proposal(
                proposal_id=int(event.proposal_id),
                vote_type=event.vote_type,
                voter_address=event.voter_address,
                private_key=event.private_key
            )
            
            if result["success"]:
                # Record vote
                self.vote_repo.record_vote(
                    proposal_id=event.proposal_id,
                    voter=event.voter_address,
                    vote_type=event.vote_type.value,
                    voting_power=result["voting_power"],
                    tx_hash=result["tx_hash"]
                )
                
                # Check if proposal reached quorum
                await self._check_proposal_status(event.proposal_id)
                
                logger.info(f"Recorded vote from {event.voter_address} on proposal {event.proposal_id}")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling vote submission: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _notify_token_holders(self, proposal: Dict[str, Any], tenant_id: str):
        """Notify token holders about new proposal"""
        await self.service_clients.post(
            "http://notification-service:8000/api/v1/notifications/broadcast",
            json={
                "tenant_id": tenant_id,
                "notification_type": "dao_proposal",
                "title": f"New DAO Proposal: {proposal['title']}",
                "message": proposal["description"],
                "data": {
                    "proposal_id": proposal["proposal_id"],
                    "voting_deadline": proposal["voting_deadline"].isoformat()
                }
            }
        )
        
    async def _check_proposal_status(self, proposal_id: str):
        """Check if proposal reached quorum or deadline"""
        votes = self.vote_repo.get_proposal_votes(proposal_id)
        
        # Get proposal details from blockchain
        proposal_info = await self.fl_dao.get_proposal(int(proposal_id))
        
        if proposal_info["state"] == "Succeeded":
            # Execute proposal
            await self._execute_proposal(proposal_id, proposal_info)
        elif proposal_info["state"] == "Defeated":
            # Update status
            self.proposal_repo.update_proposal_status(
                proposal_id,
                "defeated"
            )
            
    async def _execute_proposal(self, proposal_id: str, proposal_info: Dict[str, Any]):
        """Execute passed proposal"""
        logger.info(f"Executing proposal {proposal_id}")
        
        # Get proposal from database
        # In production, fetch from database and execute based on type
        
        self.proposal_repo.update_proposal_status(
            proposal_id,
            "executed",
            execution_result={"executed_at": datetime.utcnow().isoformat()}
        )


class PrivacyMonitoringProcessor(EventProcessor):
    """Monitor privacy in FL sessions"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 privacy_engine: PrivacyEngine,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.privacy_engine = privacy_engine
        self.service_clients = service_clients
        
    @batch_event_handler(
        "persistent://platformq/*/privacy-metrics-events",
        PrivacyMetricsEvent,
        max_batch_size=50,
        max_wait_time_ms=5000
    )
    async def handle_privacy_metrics(self, events: List[PrivacyMetricsEvent], msgs):
        """Process privacy metrics"""
        try:
            for event in events:
                # Analyze privacy metrics
                violations = await self.privacy_engine.analyze_metrics(
                    event.session_id,
                    event.round_number,
                    event.metrics
                )
                
                if violations:
                    # Report violations
                    for violation in violations:
                        await self._report_violation(
                            event.session_id,
                            violation["type"],
                            violation["severity"],
                            violation["details"]
                        )
                        
            logger.info(f"Processed {len(events)} privacy metrics")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing privacy metrics: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _report_violation(self, session_id: str, violation_type: str,
                              severity: str, details: Dict[str, Any]):
        """Report privacy violation"""
        logger.warning(f"Privacy violation in session {session_id}: {violation_type}")
        
        # Update trust scores
        if "participant_id" in details:
            await self.service_clients.post(
                "http://verifiable-credential-service:8000/api/v1/trust/update",
                json={
                    "entity_id": details["participant_id"],
                    "entity_type": "user",
                    "change": -0.1 if severity == "low" else -0.3,
                    "reason": f"Privacy violation: {violation_type}"
                }
            ) 