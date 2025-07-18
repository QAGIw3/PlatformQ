"""API Endpoints for {{ cookiecutter.service_name }}

Uses standardized patterns for error handling and service communication.
"""

from fastapi import APIRouter, Depends, Query, Path, Body
from typing import List, Optional
from uuid import UUID
import logging

from platformq_shared import (
    ServiceClients,
    NotFoundError,
    ValidationError,
    ConflictError,
    ForbiddenError
)
from platformq_shared.event_publisher import EventPublisher

from ..schemas import (
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Create,
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Update,
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response,
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Filter,
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}ListResponse
)
{% if cookiecutter.database_type != "none" %}
from ..repository import {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository
{% endif %}
from ..auth import get_current_user, require_permissions
from .deps import get_repository, get_event_publisher

logger = logging.getLogger(__name__)

router = APIRouter()

# Service clients for inter-service communication
service_clients = ServiceClients(
    base_timeout=30.0,
    max_retries=3
)


@router.post("/{{ cookiecutter.service_slug }}", response_model={{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response, status_code=201)
async def create_{{ cookiecutter.service_slug.replace('-', '_') }}(
    data: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Create = Body(..., description="Data for creating new {{ cookiecutter.service_slug }}"),
    {% if cookiecutter.database_type != "none" %}repository: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository = Depends(get_repository),{% endif %}
    event_publisher: EventPublisher = Depends(get_event_publisher),
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new {{ cookiecutter.service_slug }}.
    
    Requires authentication and appropriate permissions.
    """
    try:
        # Validate permissions
        tenant_id = current_user.get("tenant_id")
        user_id = current_user.get("user_id")
        
        if not tenant_id:
            raise ValidationError("Tenant ID is required")
            
        {% if cookiecutter.database_type != "none" %}
        # Check for duplicates if needed
        # existing = repository.get_by_name(data.name, tenant_id)
        # if existing:
        #     raise ConflictError(f"{{ cookiecutter.service_name }} with name '{data.name}' already exists")
        
        # Create the record
        created = repository.create(
            data=data,
            tenant_id=tenant_id,
            user_id=user_id
        )
        
        logger.info(f"Created {{ cookiecutter.service_slug }} {created.id} for tenant {tenant_id}")
        
        return {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response.from_orm(created)
        {% else %}
        # Implement your creation logic here
        logger.info(f"Creating {{ cookiecutter.service_slug }} for tenant {tenant_id}")
        
        return {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response(
            id="dummy-id",
            **data.dict()
        )
        {% endif %}
        
    except (ValidationError, ConflictError):
        raise
    except Exception as e:
        logger.error(f"Error creating {{ cookiecutter.service_slug }}: {e}")
        raise


@router.get("/{{ cookiecutter.service_slug }}", response_model={{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}ListResponse)
async def list_{{ cookiecutter.service_slug.replace('-', '_') }}(
    filters: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Filter = Depends(),
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of items to return"),
    {% if cookiecutter.database_type != "none" %}repository: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository = Depends(get_repository),{% endif %}
    current_user: dict = Depends(get_current_user)
):
    """
    List {{ cookiecutter.service_slug }} with optional filtering.
    
    Supports pagination with skip/limit parameters.
    """
    try:
        tenant_id = current_user.get("tenant_id")
        
        {% if cookiecutter.database_type != "none" %}
        # Get filtered results
        items = repository.search(
            filters=filters,
            tenant_id=tenant_id,
            skip=skip,
            limit=limit
        )
        
        # Get total count for pagination
        total = repository.count(filters=filters, tenant_id=tenant_id)
        
        return {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}ListResponse(
            items=[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response.from_orm(item) for item in items],
            total=total,
            skip=skip,
            limit=limit
        )
        {% else %}
        # Implement your listing logic here
        return {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}ListResponse(
            items=[],
            total=0,
            skip=skip,
            limit=limit
        )
        {% endif %}
        
    except Exception as e:
        logger.error(f"Error listing {{ cookiecutter.service_slug }}: {e}")
        raise


@router.get("/{{ cookiecutter.service_slug }}/{id}", response_model={{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response)
async def get_{{ cookiecutter.service_slug.replace('-', '_') }}(
    id: UUID = Path(..., description="{{ cookiecutter.service_name }} ID"),
    {% if cookiecutter.database_type != "none" %}repository: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository = Depends(get_repository),{% endif %}
    current_user: dict = Depends(get_current_user)
):
    """Get a specific {{ cookiecutter.service_slug }} by ID."""
    try:
        tenant_id = current_user.get("tenant_id")
        
        {% if cookiecutter.database_type != "none" %}
        item = repository.get(id)
        
        if not item:
            raise NotFoundError(f"{{ cookiecutter.service_name }} {id} not found")
            
        # Verify tenant access
        if item.tenant_id != tenant_id:
            raise ForbiddenError("Access denied to this resource")
            
        return {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response.from_orm(item)
        {% else %}
        # Implement your get logic here
        raise NotFoundError(f"{{ cookiecutter.service_name }} {id} not found")
        {% endif %}
        
    except (NotFoundError, ForbiddenError):
        raise
    except Exception as e:
        logger.error(f"Error getting {{ cookiecutter.service_slug }} {id}: {e}")
        raise


@router.put("/{{ cookiecutter.service_slug }}/{id}", response_model={{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response)
async def update_{{ cookiecutter.service_slug.replace('-', '_') }}(
    id: UUID = Path(..., description="{{ cookiecutter.service_name }} ID"),
    data: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Update = Body(..., description="Update data"),
    {% if cookiecutter.database_type != "none" %}repository: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository = Depends(get_repository),{% endif %}
    event_publisher: EventPublisher = Depends(get_event_publisher),
    current_user: dict = Depends(get_current_user)
):
    """Update an existing {{ cookiecutter.service_slug }}."""
    try:
        tenant_id = current_user.get("tenant_id")
        
        {% if cookiecutter.database_type != "none" %}
        # Get existing item
        existing = repository.get(id)
        
        if not existing:
            raise NotFoundError(f"{{ cookiecutter.service_name }} {id} not found")
            
        # Verify tenant access
        if existing.tenant_id != tenant_id:
            raise ForbiddenError("Access denied to this resource")
            
        # Perform update
        updated = repository.update(
            id=id,
            data=data,
            tenant_id=tenant_id
        )
        
        logger.info(f"Updated {{ cookiecutter.service_slug }} {id}")
        
        return {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response.from_orm(updated)
        {% else %}
        # Implement your update logic here
        raise NotFoundError(f"{{ cookiecutter.service_name }} {id} not found")
        {% endif %}
        
    except (NotFoundError, ForbiddenError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Error updating {{ cookiecutter.service_slug }} {id}: {e}")
        raise


@router.delete("/{{ cookiecutter.service_slug }}/{id}", status_code=204)
async def delete_{{ cookiecutter.service_slug.replace('-', '_') }}(
    id: UUID = Path(..., description="{{ cookiecutter.service_name }} ID"),
    {% if cookiecutter.database_type != "none" %}repository: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository = Depends(get_repository),{% endif %}
    event_publisher: EventPublisher = Depends(get_event_publisher),
    current_user: dict = Depends(get_current_user)
):
    """Delete a {{ cookiecutter.service_slug }}."""
    try:
        tenant_id = current_user.get("tenant_id")
        
        {% if cookiecutter.database_type != "none" %}
        # Get existing item
        existing = repository.get(id)
        
        if not existing:
            raise NotFoundError(f"{{ cookiecutter.service_name }} {id} not found")
            
        # Verify tenant access
        if existing.tenant_id != tenant_id:
            raise ForbiddenError("Access denied to this resource")
            
        # Check if deletion is allowed (business rules)
        # if existing.status == "active":
        #     raise ValidationError("Cannot delete active {{ cookiecutter.service_slug }}")
            
        # Perform deletion (soft or hard delete based on requirements)
        repository.delete(id)
        
        logger.info(f"Deleted {{ cookiecutter.service_slug }} {id}")
        {% else %}
        # Implement your deletion logic here
        logger.info(f"Deleting {{ cookiecutter.service_slug }} {id}")
        {% endif %}
        
    except (NotFoundError, ForbiddenError, ValidationError):
        raise
    except Exception as e:
        logger.error(f"Error deleting {{ cookiecutter.service_slug }} {id}: {e}")
        raise


# Add more endpoints as needed
@router.post("/{{ cookiecutter.service_slug }}/bulk", response_model=List[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response])
async def bulk_create_{{ cookiecutter.service_slug.replace('-', '_') }}(
    items: List[{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Create] = Body(..., min_items=1, max_items=100),
    {% if cookiecutter.database_type != "none" %}repository: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository = Depends(get_repository),{% endif %}
    event_publisher: EventPublisher = Depends(get_event_publisher),
    current_user: dict = Depends(get_current_user)
):
    """Bulk create multiple {{ cookiecutter.service_slug }} items."""
    try:
        tenant_id = current_user.get("tenant_id")
        user_id = current_user.get("user_id")
        
        {% if cookiecutter.database_type != "none" %}
        created_items = repository.bulk_create(
            items=items,
            tenant_id=tenant_id,
            user_id=user_id
        )
        
        logger.info(f"Bulk created {len(created_items)} {{ cookiecutter.service_slug }} items")
        
        return [{{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Response.from_orm(item) for item in created_items]
        {% else %}
        # Implement your bulk creation logic here
        return []
        {% endif %}
        
    except ValidationError:
        raise
    except Exception as e:
        logger.error(f"Error in bulk create: {e}")
        raise


# Example of calling another service
@router.post("/{{ cookiecutter.service_slug }}/{id}/process")
async def process_{{ cookiecutter.service_slug.replace('-', '_') }}(
    id: UUID = Path(..., description="{{ cookiecutter.service_name }} ID"),
    current_user: dict = Depends(get_current_user)
):
    """Trigger processing for a {{ cookiecutter.service_slug }}."""
    try:
        # Call another service
        response = await service_clients.post(
            "processing-service",
            "/api/v1/process",
            json={
                "entity_type": "{{ cookiecutter.service_slug }}",
                "entity_id": str(id),
                "requested_by": current_user.get("user_id")
            }
        )
        
        return {"status": "processing", "job_id": response.get("job_id")}
        
    except Exception as e:
        logger.error(f"Error triggering processing: {e}")
        raise 