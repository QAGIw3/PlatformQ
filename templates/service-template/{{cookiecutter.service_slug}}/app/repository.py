"""
Repository for {{ cookiecutter.service_name }}

Uses the enhanced repository patterns from platformq-shared.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

{% if cookiecutter.database_type == "postgresql" or cookiecutter.database_type == "both" %}
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from platformq_shared import PostgresRepository, QueryBuilder
{% endif %}

{% if cookiecutter.database_type == "cassandra" or cookiecutter.database_type == "both" %}
from cassandra.cluster import Session as CassandraSession
from cassandra.query import SimpleStatement, ConsistencyLevel
from platformq_shared import CassandraRepository
{% endif %}

from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    # Import your event schemas here
    GenericCreatedEvent,
    GenericUpdatedEvent,
    GenericDeletedEvent
)

from .models import {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model
from .schemas import (
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Create,
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Update,
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Filter
)

logger = logging.getLogger(__name__)


{% if cookiecutter.database_type == "postgresql" or cookiecutter.database_type == "both" %}
class {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository(PostgresRepository[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model]):
    """Repository for {{ cookiecutter.service_name }} using PostgreSQL"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory, {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model)
        self.event_publisher = event_publisher
        
    def create(self, data: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Create, 
               tenant_id: str, user_id: str) -> {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model:
        """Create a new record"""
        with self.get_session() as session:
            # Create model instance
            model = {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                created_by=user_id,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                # Add other fields from data
                **data.dict()
            )
            
            session.add(model)
            session.commit()
            session.refresh(model)
            
            # Publish event if publisher available
            if self.event_publisher:
                event = GenericCreatedEvent(
                    entity_id=str(model.id),
                    entity_type="{{ cookiecutter.service_slug }}",
                    tenant_id=tenant_id,
                    created_by=user_id,
                    created_at=model.created_at.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/{{ cookiecutter.service_slug }}-created-events",
                    event=event
                )
                
            logger.info(f"Created {{ cookiecutter.service_slug }} {model.id} for tenant {tenant_id}")
            return model
            
    def update(self, id: uuid.UUID, data: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Update,
               tenant_id: str) -> Optional[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model]:
        """Update a record"""
        with self.get_session() as session:
            model = session.query({{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model).filter(
                and_(
                    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model.id == id,
                    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model.tenant_id == tenant_id
                )
            ).first()
            
            if not model:
                return None
                
            # Update fields
            update_data = data.dict(exclude_unset=True)
            for field, value in update_data.items():
                setattr(model, field, value)
                
            model.updated_at = datetime.utcnow()
            
            session.commit()
            session.refresh(model)
            
            # Publish update event
            if self.event_publisher:
                event = GenericUpdatedEvent(
                    entity_id=str(model.id),
                    entity_type="{{ cookiecutter.service_slug }}",
                    tenant_id=tenant_id,
                    updated_fields=list(update_data.keys()),
                    updated_at=model.updated_at.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/{{ cookiecutter.service_slug }}-updated-events",
                    event=event
                )
                
            return model
            
    def search(self, filters: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Filter, 
               tenant_id: str, skip: int = 0, limit: int = 100) -> List[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model]:
        """Search records with filters"""
        with self.get_session() as session:
            query = session.query({{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model).filter(
                {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model.tenant_id == tenant_id
            )
            
            # Apply filters based on your filter schema
            # Example:
            # if filters.name:
            #     query = query.filter(Model.name.contains(filters.name))
            
            # Apply ordering
            query = query.order_by({{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model.created_at.desc())
            
            # Apply pagination
            return query.offset(skip).limit(limit).all()
            
    def get_by_tenant(self, tenant_id: str, skip: int = 0, limit: int = 100) -> List[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model]:
        """Get all records for a tenant"""
        return self.query()\
            .filter_by(tenant_id=tenant_id)\
            .order_by({{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model.created_at.desc())\
            .paginate(skip, limit)
            
    def bulk_create(self, items: List[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Create], 
                    tenant_id: str, user_id: str) -> List[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model]:
        """Bulk create records"""
        created_items = []
        
        with self.get_session() as session:
            for item_data in items:
                model = {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model(
                    id=uuid.uuid4(),
                    tenant_id=tenant_id,
                    created_by=user_id,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    **item_data.dict()
                )
                session.add(model)
                created_items.append(model)
                
            session.commit()
            
            # Refresh all models
            for model in created_items:
                session.refresh(model)
                
        # Publish bulk creation events
        if self.event_publisher:
            for model in created_items:
                event = GenericCreatedEvent(
                    entity_id=str(model.id),
                    entity_type="{{ cookiecutter.service_slug }}",
                    tenant_id=tenant_id,
                    created_by=user_id,
                    created_at=model.created_at.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/{{ cookiecutter.service_slug }}-created-events",
                    event=event
                )
                
        return created_items
{% endif %}

{% if cookiecutter.database_type == "cassandra" %}
class {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository(CassandraRepository[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model]):
    """Repository for {{ cookiecutter.service_name }} using Cassandra"""
    
    def __init__(self, db_session: CassandraSession, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session, {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model, "{{ cookiecutter.service_slug.replace('-', '_') }}_keyspace")
        self.table_name = "{{ cookiecutter.service_slug.replace('-', '_') }}"
        self.event_publisher = event_publisher
        
    def create(self, data: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Create, 
               tenant_id: str, user_id: str) -> {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Model:
        """Create a new record in Cassandra"""
        id = uuid.uuid4()
        created_at = datetime.utcnow()
        
        query = f"""
        INSERT INTO {self.table_name} (
            id, tenant_id, created_by, created_at, updated_at,
            -- Add other columns here based on your schema
        )
        VALUES (%s, %s, %s, %s, %s)
        """
        
        prepared = self.session.prepare(query)
        prepared.consistency_level = ConsistencyLevel.QUORUM
        
        self.session.execute(
            prepared,
            [id, tenant_id, user_id, created_at, created_at]
            # Add other values here
        )
        
        # Publish event
        if self.event_publisher:
            event = GenericCreatedEvent(
                entity_id=str(id),
                entity_type="{{ cookiecutter.service_slug }}",
                tenant_id=tenant_id,
                created_by=user_id,
                created_at=created_at.isoformat()
            )
            
            self.event_publisher.publish_event(
                topic=f"persistent://platformq/{tenant_id}/{{ cookiecutter.service_slug }}-created-events",
                event=event
            )
            
        return self.get(id)
{% endif %} 