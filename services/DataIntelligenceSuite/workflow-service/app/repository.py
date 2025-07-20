"""
Repository for Workflow Service

Uses the enhanced repository patterns from platformq-shared.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
import json

from platformq_shared import PostgresRepository, QueryBuilder
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    WorkflowCreatedEvent,
    WorkflowStartedEvent,
    WorkflowCompletedEvent,
    WorkflowFailedEvent,
    TaskCreatedEvent,
    TaskCompletedEvent
)

from .database import Workflow, Task, ResourceAuthorization
from .schemas import (
    WorkflowCreate,
    WorkflowUpdate,
    TaskCreate,
    TaskUpdate,
    WorkflowFilter
)

logger = logging.getLogger(__name__)


class WorkflowRepository(PostgresRepository[Workflow]):
    """Repository for workflows"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory, Workflow)
        self.event_publisher = event_publisher
        
    def create(self, workflow_data: Dict[str, Any]) -> Workflow:
        """Create a new workflow"""
        with self.get_session() as session:
            workflow = Workflow(
                id=uuid.uuid4(),
                name=workflow_data["name"],
                type=workflow_data["type"],
                tenant_id=workflow_data["tenant_id"],
                created_by=workflow_data["created_by"],
                status="pending",
                input_data=workflow_data.get("input_data", {}),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(workflow)
            session.commit()
            session.refresh(workflow)
            
            # Publish event
            if self.event_publisher:
                event = WorkflowCreatedEvent(
                    workflow_id=str(workflow.id),
                    workflow_name=workflow.name,
                    workflow_type=workflow.type,
                    tenant_id=workflow.tenant_id,
                    created_by=workflow.created_by,
                    created_at=workflow.created_at.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{workflow.tenant_id}/workflow-created-events",
                    event=event
                )
                
            logger.info(f"Created workflow {workflow.id}: {workflow.name}")
            return workflow
            
    def update_dag_run(self, workflow_id: uuid.UUID, dag_run_id: str,
                      status: str) -> Optional[Workflow]:
        """Update workflow with Airflow DAG run info"""
        with self.get_session() as session:
            workflow = session.query(Workflow).filter(
                Workflow.id == workflow_id
            ).first()
            
            if not workflow:
                return None
                
            workflow.dag_run_id = dag_run_id
            workflow.status = status
            workflow.updated_at = datetime.utcnow()
            
            if status == "running":
                workflow.started_at = datetime.utcnow()
                
                # Publish started event
                if self.event_publisher:
                    event = WorkflowStartedEvent(
                        workflow_id=str(workflow.id),
                        dag_run_id=dag_run_id,
                        started_at=workflow.started_at.isoformat()
                    )
                    
                    self.event_publisher.publish_event(
                        topic=f"persistent://platformq/{workflow.tenant_id}/workflow-started-events",
                        event=event
                    )
                    
            session.commit()
            session.refresh(workflow)
            
            return workflow
            
    def complete_workflow(self, workflow_id: uuid.UUID, output_data: Dict[str, Any],
                         success: bool = True) -> Optional[Workflow]:
        """Mark workflow as completed"""
        with self.get_session() as session:
            workflow = session.query(Workflow).filter(
                Workflow.id == workflow_id
            ).first()
            
            if not workflow:
                return None
                
            workflow.status = "completed" if success else "failed"
            workflow.output_data = output_data
            workflow.completed_at = datetime.utcnow()
            workflow.updated_at = datetime.utcnow()
            
            session.commit()
            session.refresh(workflow)
            
            # Publish appropriate event
            if self.event_publisher:
                if success:
                    event = WorkflowCompletedEvent(
                        workflow_id=str(workflow.id),
                        workflow_name=workflow.name,
                        duration_seconds=int((workflow.completed_at - workflow.created_at).total_seconds()),
                        output_summary=output_data.get("summary", {}),
                        completed_at=workflow.completed_at.isoformat()
                    )
                    topic = f"persistent://platformq/{workflow.tenant_id}/workflow-completed-events"
                else:
                    event = WorkflowFailedEvent(
                        workflow_id=str(workflow.id),
                        workflow_name=workflow.name,
                        error_message=output_data.get("error", "Unknown error"),
                        failed_at=workflow.completed_at.isoformat()
                    )
                    topic = f"persistent://platformq/{workflow.tenant_id}/workflow-failed-events"
                    
                self.event_publisher.publish_event(topic=topic, event=event)
                
            return workflow
            
    def get_active_workflows(self, tenant_id: str, limit: int = 100) -> List[Workflow]:
        """Get active workflows for tenant"""
        with self.get_session() as session:
            return session.query(Workflow).filter(
                and_(
                    Workflow.tenant_id == tenant_id,
                    Workflow.status.in_(["pending", "running"])
                )
            ).order_by(Workflow.created_at.desc()).limit(limit).all()
            
    def search(self, filters: WorkflowFilter, tenant_id: str,
               skip: int = 0, limit: int = 100) -> List[Workflow]:
        """Search workflows with filters"""
        with self.get_session() as session:
            query = session.query(Workflow).filter(
                Workflow.tenant_id == tenant_id
            )
            
            if filters.name:
                query = query.filter(Workflow.name.ilike(f"%{filters.name}%"))
                
            if filters.type:
                query = query.filter(Workflow.type == filters.type)
                
            if filters.status:
                query = query.filter(Workflow.status == filters.status)
                
            if filters.created_by:
                query = query.filter(Workflow.created_by == filters.created_by)
                
            if filters.created_after:
                query = query.filter(Workflow.created_at >= filters.created_after)
                
            if filters.created_before:
                query = query.filter(Workflow.created_at <= filters.created_before)
                
            if filters.dag_run_id:
                query = query.filter(Workflow.dag_run_id == filters.dag_run_id)
                
            # Apply ordering
            query = query.order_by(Workflow.created_at.desc())
            
            return query.offset(skip).limit(limit).all()
            
    def get_statistics(self, tenant_id: str, time_window: Optional[int] = None) -> Dict[str, Any]:
        """Get workflow statistics"""
        with self.get_session() as session:
            base_query = session.query(Workflow).filter(
                Workflow.tenant_id == tenant_id
            )
            
            if time_window:
                cutoff = datetime.utcnow() - timedelta(days=time_window)
                base_query = base_query.filter(Workflow.created_at >= cutoff)
                
            # Total workflows
            total = base_query.count()
            
            # By status
            status_counts = session.query(
                Workflow.status,
                func.count(Workflow.id)
            ).filter(
                Workflow.tenant_id == tenant_id
            ).group_by(Workflow.status).all()
            
            # By type
            type_counts = session.query(
                Workflow.type,
                func.count(Workflow.id)
            ).filter(
                Workflow.tenant_id == tenant_id
            ).group_by(Workflow.type).all()
            
            # Success rate
            completed = base_query.filter(Workflow.status == "completed").count()
            failed = base_query.filter(Workflow.status == "failed").count()
            success_rate = (completed / (completed + failed) * 100) if (completed + failed) > 0 else 0
            
            # Average duration
            avg_duration = session.query(
                func.avg(
                    func.extract('epoch', Workflow.completed_at - Workflow.created_at)
                )
            ).filter(
                and_(
                    Workflow.tenant_id == tenant_id,
                    Workflow.completed_at.isnot(None)
                )
            ).scalar()
            
            return {
                "total_workflows": total,
                "by_status": dict(status_counts),
                "by_type": dict(type_counts),
                "success_rate": round(success_rate, 2),
                "avg_duration_seconds": round(avg_duration or 0, 2),
                "active_workflows": base_query.filter(
                    Workflow.status.in_(["pending", "running"])
                ).count()
            }


class TaskRepository(PostgresRepository[Task]):
    """Repository for workflow tasks"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory, Task)
        self.event_publisher = event_publisher
        
    def create(self, task_data: TaskCreate, workflow_id: uuid.UUID) -> Task:
        """Create a new task"""
        with self.get_session() as session:
            task = Task(
                id=uuid.uuid4(),
                workflow_id=workflow_id,
                name=task_data.name,
                task_type=task_data.task_type,
                status="pending",
                input_data=task_data.input_data or {},
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            session.add(task)
            session.commit()
            session.refresh(task)
            
            # Publish event
            if self.event_publisher:
                # Get workflow for tenant_id
                workflow = session.query(Workflow).filter(
                    Workflow.id == workflow_id
                ).first()
                
                if workflow:
                    event = TaskCreatedEvent(
                        task_id=str(task.id),
                        workflow_id=str(workflow_id),
                        task_name=task.name,
                        task_type=task.task_type,
                        created_at=task.created_at.isoformat()
                    )
                    
                    self.event_publisher.publish_event(
                        topic=f"persistent://platformq/{workflow.tenant_id}/task-created-events",
                        event=event
                    )
                    
            return task
            
    def update_status(self, task_id: uuid.UUID, status: str,
                     output_data: Optional[Dict[str, Any]] = None) -> Optional[Task]:
        """Update task status"""
        with self.get_session() as session:
            task = session.query(Task).filter(Task.id == task_id).first()
            
            if not task:
                return None
                
            task.status = status
            task.updated_at = datetime.utcnow()
            
            if status == "running":
                task.started_at = datetime.utcnow()
            elif status in ["completed", "failed"]:
                task.completed_at = datetime.utcnow()
                if output_data:
                    task.output_data = output_data
                    
            session.commit()
            session.refresh(task)
            
            # Publish completion event
            if self.event_publisher and status == "completed":
                # Get workflow for tenant_id
                workflow = session.query(Workflow).filter(
                    Workflow.id == task.workflow_id
                ).first()
                
                if workflow:
                    event = TaskCompletedEvent(
                        task_id=str(task.id),
                        workflow_id=str(task.workflow_id),
                        task_name=task.name,
                        duration_seconds=int((task.completed_at - task.started_at).total_seconds()) if task.started_at else 0,
                        completed_at=task.completed_at.isoformat()
                    )
                    
                    self.event_publisher.publish_event(
                        topic=f"persistent://platformq/{workflow.tenant_id}/task-completed-events",
                        event=event
                    )
                    
            return task
            
    def get_workflow_tasks(self, workflow_id: uuid.UUID) -> List[Task]:
        """Get all tasks for a workflow"""
        with self.get_session() as session:
            return session.query(Task).filter(
                Task.workflow_id == workflow_id
            ).order_by(Task.created_at).all()
            
    def get_pending_tasks(self, limit: int = 100) -> List[Task]:
        """Get pending tasks across all workflows"""
        with self.get_session() as session:
            return session.query(Task).filter(
                Task.status == "pending"
            ).order_by(Task.created_at).limit(limit).all()


class ResourceAuthorizationRepository(PostgresRepository[ResourceAuthorization]):
    """Repository for resource authorizations"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory, ResourceAuthorization)
        
    def create(self, auth_data: Dict[str, Any]) -> ResourceAuthorization:
        """Create resource authorization"""
        with self.get_session() as session:
            auth = ResourceAuthorization(
                id=uuid.uuid4(),
                resource_id=auth_data["resource_id"],
                resource_type=auth_data["resource_type"],
                authorized_did=auth_data["authorized_did"],
                permissions=json.dumps(auth_data["permissions"]),
                conditions=json.dumps(auth_data.get("conditions", {})),
                credential_id=auth_data.get("credential_id"),
                expires_at=auth_data.get("expires_at"),
                issued_by=auth_data["issued_by"],
                created_at=datetime.utcnow()
            )
            
            session.add(auth)
            session.commit()
            session.refresh(auth)
            
            return auth
            
    def get_active_authorizations(self, resource_id: str,
                                 authorized_did: str) -> List[ResourceAuthorization]:
        """Get active authorizations for resource and DID"""
        with self.get_session() as session:
            return session.query(ResourceAuthorization).filter(
                and_(
                    ResourceAuthorization.resource_id == resource_id,
                    ResourceAuthorization.authorized_did == authorized_did,
                    ResourceAuthorization.revoked == False,
                    or_(
                        ResourceAuthorization.expires_at.is_(None),
                        ResourceAuthorization.expires_at > datetime.utcnow()
                    )
                )
            ).all()
            
    def revoke_authorization(self, auth_id: uuid.UUID) -> Optional[ResourceAuthorization]:
        """Revoke authorization"""
        with self.get_session() as session:
            auth = session.query(ResourceAuthorization).filter(
                ResourceAuthorization.id == auth_id
            ).first()
            
            if not auth:
                return None
                
            auth.revoked = True
            auth.revoked_at = datetime.utcnow()
            
            session.commit()
            session.refresh(auth)
            
            return auth 