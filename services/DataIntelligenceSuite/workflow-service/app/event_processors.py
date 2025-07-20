"""
Event Processors for Workflow Service

Handles workflow-related events and triggers Airflow DAGs.
"""

import logging
import json
import re
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
    AssetCreatedEvent,
    DocumentUpdatedEvent,
    ProjectCreatedEvent,
    DAOEvent,
    ProposalApprovedEvent,
    SimulationCompletedEvent,
    WorkflowRequestedEvent,
    TaskAssignedEvent,
    ResourceAuthorizationRequestedEvent
)

from .repository import WorkflowRepository, TaskRepository
from .airflow_bridge import AirflowBridge
from .verifiable_credentials.workflow_credentials import WorkflowCredentialManager

logger = logging.getLogger(__name__)


class WorkflowEventProcessor(EventProcessor):
    """Process events that trigger workflows"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 workflow_repo: WorkflowRepository,
                 task_repo: TaskRepository,
                 airflow_bridge: AirflowBridge,
                 credential_manager: WorkflowCredentialManager,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.workflow_repo = workflow_repo
        self.task_repo = task_repo
        self.airflow_bridge = airflow_bridge
        self.credential_manager = credential_manager
        self.service_clients = service_clients
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting workflow event processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping workflow event processor")
        
    @event_handler("persistent://platformq/*/project-created-events", ProjectCreatedEvent)
    async def handle_project_created(self, event: ProjectCreatedEvent, msg):
        """Trigger project setup workflow"""
        try:
            # Create workflow record
            workflow = await self.workflow_repo.create({
                "name": f"Project Setup: {event.project_name}",
                "type": "project_setup",
                "tenant_id": event.tenant_id,
                "created_by": event.creator_id,
                "input_data": {
                    "project_id": event.project_id,
                    "project_name": event.project_name,
                    "project_type": event.project_type,
                    "collaborators": event.collaborators
                }
            })
            
            # Trigger Airflow DAG
            dag_run = await self.airflow_bridge.trigger_dag(
                dag_id="project_setup_workflow",
                conf={
                    "workflow_id": str(workflow.id),
                    "project_id": event.project_id,
                    "project_name": event.project_name,
                    "tenant_id": event.tenant_id,
                    "creator_id": event.creator_id,
                    "setup_nextcloud": True,
                    "setup_openproject": True,
                    "setup_zulip": True
                }
            )
            
            # Update workflow with DAG run ID
            await self.workflow_repo.update_dag_run(
                workflow_id=workflow.id,
                dag_run_id=dag_run["dag_run_id"],
                status="running"
            )
            
            logger.info(f"Triggered project setup workflow for {event.project_name}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing project created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/dao-events", DAOEvent)
    async def handle_dao_event(self, event: DAOEvent, msg):
        """Handle DAO governance events"""
        try:
            # Map DAO event types to workflows
            dag_mapping = {
                "ProposalCreated": "dao_proposal_review_workflow",
                "VoteCast": "dao_vote_processing_workflow",
                "ProposalExecuted": "dao_proposal_execution_workflow",
                "DaoInitialized": "dao_initialization_workflow",
                "TreasuryUpdated": "dao_treasury_audit_workflow"
            }
            
            dag_id = dag_mapping.get(event.event_type)
            if not dag_id:
                logger.info(f"No workflow mapped for DAO event type: {event.event_type}")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Create workflow record
            workflow = await self.workflow_repo.create({
                "name": f"DAO {event.event_type}: {event.dao_id}",
                "type": f"dao_{event.event_type.lower()}",
                "tenant_id": event.tenant_id,
                "created_by": event.initiator_id,
                "input_data": {
                    "dao_id": event.dao_id,
                    "event_type": event.event_type,
                    "blockchain_tx_hash": event.blockchain_tx_hash,
                    "proposal_id": event.proposal_id,
                    "event_data": event.event_data
                }
            })
            
            # Prepare DAG configuration
            conf = {
                "workflow_id": str(workflow.id),
                "tenant_id": event.tenant_id,
                "dao_id": event.dao_id,
                "event_type": event.event_type,
                "blockchain_tx_hash": event.blockchain_tx_hash,
                "event_timestamp": event.event_timestamp
            }
            
            if event.proposal_id:
                conf["proposal_id"] = event.proposal_id
            if event.voter_id:
                conf["voter_id"] = event.voter_id
            if event.event_data:
                conf["event_data"] = json.loads(event.event_data) if isinstance(event.event_data, str) else event.event_data
                
            # Trigger DAG
            dag_run = await self.airflow_bridge.trigger_dag(dag_id, conf)
            
            # Update workflow
            await self.workflow_repo.update_dag_run(
                workflow_id=workflow.id,
                dag_run_id=dag_run["dag_run_id"],
                status="running"
            )
            
            logger.info(f"Triggered {dag_id} for DAO event {event.event_type}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing DAO event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/proposal-approved-events", ProposalApprovedEvent)
    async def handle_proposal_approved(self, event: ProposalApprovedEvent, msg):
        """Execute approved proposal workflow"""
        try:
            # Determine workflow type based on proposal
            workflow_type = self._determine_proposal_workflow(event.proposal_type)
            
            # Create workflow
            workflow = await self.workflow_repo.create({
                "name": f"Execute Proposal: {event.proposal_title}",
                "type": workflow_type,
                "tenant_id": event.tenant_id,
                "created_by": event.proposer_id,
                "input_data": {
                    "proposal_id": event.proposal_id,
                    "proposal_type": event.proposal_type,
                    "approval_votes": event.approval_votes,
                    "execution_params": event.execution_params
                }
            })
            
            # Trigger execution DAG
            dag_run = await self.airflow_bridge.trigger_dag(
                dag_id="proposal_execution_workflow",
                conf={
                    "workflow_id": str(workflow.id),
                    "proposal_id": event.proposal_id,
                    "proposal_type": event.proposal_type,
                    "tenant_id": event.tenant_id,
                    "execution_params": event.execution_params
                }
            )
            
            # Update workflow
            await self.workflow_repo.update_dag_run(
                workflow_id=workflow.id,
                dag_run_id=dag_run["dag_run_id"],
                status="running"
            )
            
            # Issue credential for proposal approval
            if self.credential_manager:
                await self.credential_manager.issue_proposal_approval_credential(
                    proposal_id=event.proposal_id,
                    approver_dids=[f"did:platformq:dao:{event.dao_id}"],
                    approval_data={
                        "votes": event.approval_votes,
                        "approved_at": datetime.utcnow().isoformat()
                    }
                )
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing proposal approved event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @batch_event_handler(
        "persistent://platformq/*/document-updated-events",
        DocumentUpdatedEvent,
        max_batch_size=20,
        max_wait_time_ms=3000
    )
    async def handle_document_updates(self, events: List[DocumentUpdatedEvent], msgs):
        """Handle batch document updates"""
        try:
            # Group by document type
            by_type = {}
            for event in events:
                doc_type = self._get_document_type(event.document_path)
                if doc_type not in by_type:
                    by_type[doc_type] = []
                by_type[doc_type].append(event)
                
            # Process each type
            for doc_type, type_events in by_type.items():
                if doc_type == "markdown":
                    await self._process_markdown_updates(type_events)
                elif doc_type == "cad":
                    await self._process_cad_updates(type_events)
                elif doc_type == "pdf":
                    await self._process_pdf_updates(type_events)
                    
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing document updates: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/simulation-completed-events", SimulationCompletedEvent)
    async def handle_simulation_completed(self, event: SimulationCompletedEvent, msg):
        """Process simulation results and trigger analysis workflows"""
        try:
            # Create analysis workflow
            workflow = await self.workflow_repo.create({
                "name": f"Analyze Simulation: {event.simulation_name}",
                "type": "simulation_analysis",
                "tenant_id": event.tenant_id,
                "created_by": event.owner_id,
                "input_data": {
                    "simulation_id": event.simulation_id,
                    "simulation_type": event.simulation_type,
                    "result_path": event.result_path,
                    "metrics": event.metrics
                }
            })
            
            # Determine analysis type
            analysis_dag = "simulation_basic_analysis"
            if event.simulation_type == "federated":
                analysis_dag = "federated_simulation_analysis"
            elif event.simulation_type == "multi_physics":
                analysis_dag = "multi_physics_analysis"
                
            # Trigger analysis DAG
            dag_run = await self.airflow_bridge.trigger_dag(
                dag_id=analysis_dag,
                conf={
                    "workflow_id": str(workflow.id),
                    "simulation_id": event.simulation_id,
                    "result_path": event.result_path,
                    "generate_report": True,
                    "publish_insights": True
                }
            )
            
            # Update workflow
            await self.workflow_repo.update_dag_run(
                workflow_id=workflow.id,
                dag_run_id=dag_run["dag_run_id"],
                status="running"
            )
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing simulation completed event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    def _determine_proposal_workflow(self, proposal_type: str) -> str:
        """Determine workflow type based on proposal"""
        workflow_map = {
            "budget": "budget_execution_workflow",
            "feature": "feature_deployment_workflow",
            "governance": "governance_update_workflow",
            "partnership": "partnership_activation_workflow"
        }
        return workflow_map.get(proposal_type, "generic_proposal_workflow")
        
    def _get_document_type(self, document_path: str) -> str:
        """Determine document type from path"""
        if document_path.endswith('.md'):
            return "markdown"
        elif document_path.endswith(('.dwg', '.dxf', '.step')):
            return "cad"
        elif document_path.endswith('.pdf'):
            return "pdf"
        else:
            return "other"
            
    async def _process_markdown_updates(self, events: List[DocumentUpdatedEvent]):
        """Process markdown document updates"""
        for event in events:
            # Create documentation update workflow
            workflow = await self.workflow_repo.create({
                "name": f"Update Docs: {event.document_name}",
                "type": "documentation_update",
                "tenant_id": event.tenant_id,
                "created_by": event.updated_by,
                "input_data": {
                    "document_id": event.document_id,
                    "document_path": event.document_path,
                    "version": event.version
                }
            })
            
            # Trigger documentation processing
            await self.airflow_bridge.trigger_dag(
                dag_id="documentation_processing",
                conf={
                    "workflow_id": str(workflow.id),
                    "document_path": event.document_path,
                    "generate_site": True,
                    "update_search_index": True
                }
            )
            
    async def _process_cad_updates(self, events: List[DocumentUpdatedEvent]):
        """Process CAD file updates"""
        for event in events:
            # Trigger CAD validation workflow
            await self.airflow_bridge.trigger_dag(
                dag_id="cad_validation_workflow",
                conf={
                    "document_id": event.document_id,
                    "file_path": event.document_path,
                    "validate_geometry": True,
                    "generate_preview": True
                }
            )
            
    async def _process_pdf_updates(self, events: List[DocumentUpdatedEvent]):
        """Process PDF updates"""
        # Batch process PDFs for OCR/text extraction
        if events:
            await self.airflow_bridge.trigger_dag(
                dag_id="pdf_batch_processing",
                conf={
                    "documents": [
                        {
                            "id": e.document_id,
                            "path": e.document_path
                        }
                        for e in events
                    ],
                    "extract_text": True,
                    "generate_thumbnails": True
                }
            )


class AssetWorkflowProcessor(EventProcessor):
    """Process asset-related workflow events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 workflow_repo: WorkflowRepository,
                 airflow_bridge: AirflowBridge):
        super().__init__(service_name, pulsar_url)
        self.workflow_repo = workflow_repo
        self.airflow_bridge = airflow_bridge
        
    @event_handler("persistent://platformq/*/asset-created-events", AssetCreatedEvent)
    async def handle_asset_created(self, event: AssetCreatedEvent, msg):
        """Trigger asset processing workflows based on type"""
        try:
            # Check for processing rules
            processing_rule = await self._get_processing_rule(event.file_type)
            
            if not processing_rule:
                logger.info(f"No processing rule for asset type: {event.file_type}")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Create workflow
            workflow = await self.workflow_repo.create({
                "name": f"Process Asset: {event.name}",
                "type": "asset_processing",
                "tenant_id": event.tenant_id,
                "created_by": event.creator_id,
                "input_data": {
                    "asset_id": event.asset_id,
                    "file_type": event.file_type,
                    "storage_path": event.storage_path,
                    "processing_rule": processing_rule
                }
            })
            
            # Trigger appropriate DAG
            dag_id = processing_rule.get("dag_id", "generic_asset_processing")
            
            dag_run = await self.airflow_bridge.trigger_dag(
                dag_id=dag_id,
                conf={
                    "workflow_id": str(workflow.id),
                    "asset_id": event.asset_id,
                    "tenant_id": event.tenant_id,
                    "processing_steps": processing_rule.get("steps", [])
                }
            )
            
            # Update workflow
            await self.workflow_repo.update_dag_run(
                workflow_id=workflow.id,
                dag_run_id=dag_run["dag_run_id"],
                status="running"
            )
            
            logger.info(f"Triggered {dag_id} for asset {event.asset_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing asset created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _get_processing_rule(self, file_type: str) -> Optional[Dict[str, Any]]:
        """Get processing rule for file type"""
        # In production, this would query a rules engine or database
        rules = {
            "3d-model": {
                "dag_id": "3d_model_processing",
                "steps": ["validate", "optimize", "generate_preview", "extract_metadata"]
            },
            "cad-file": {
                "dag_id": "cad_processing_workflow",
                "steps": ["validate_geometry", "convert_formats", "generate_bom"]
            },
            "simulation": {
                "dag_id": "simulation_post_processing",
                "steps": ["validate_results", "generate_visualizations", "extract_insights"]
            },
            "document": {
                "dag_id": "document_processing",
                "steps": ["extract_text", "analyze_content", "index_search"]
            }
        }
        
        return rules.get(file_type) 