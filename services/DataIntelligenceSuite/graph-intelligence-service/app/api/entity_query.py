"""
Entity Query API endpoints

Provides endpoints for querying entity properties in the graph.
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from ..api.deps import get_current_tenant_and_user
from gremlin_python.driver import client, serializer

router = APIRouter()
logger = logging.getLogger(__name__)


class EntityQueryRequest(BaseModel):
    """Request model for entity property queries"""
    entity_ids: List[str] = Field(..., description="List of entity IDs to query")
    properties: List[str] = Field(..., description="Properties to retrieve")


class EntityQueryResponse(BaseModel):
    """Response model for entity queries"""
    entities: List[Dict[str, Any]]


def get_gremlin_client():
    """Get Gremlin client"""
    # This should be injected from app state in production
    return client.Client(
        'ws://janusgraph:8182/gremlin',
        'g',
        message_serializer=serializer.GraphSONSerializersV3d0()
    )


@router.post("/query/entities", response_model=EntityQueryResponse)
async def query_entity_properties(
    request: EntityQueryRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Query specific properties for entities
    
    This endpoint allows querying multiple entities and retrieving
    specific properties from the graph database.
    """
    tenant_id = context["tenant_id"]
    gremlin_client = get_gremlin_client()
    
    try:
        results = []
        
        for entity_id in request.entity_ids:
            # Build property projection
            property_projections = []
            property_names = []
            
            for prop in request.properties:
                property_projections.append(f".by(coalesce(values('{prop}'), constant(null)))")
                property_names.append(prop)
            
            # Build query
            query = f"""
                g.V().has('entity_id', '{entity_id}')
                .has('tenant_id', '{tenant_id}')
                .project('entity_id', {', '.join([f"'{p}'" for p in property_names])})
                .by('entity_id')
                {' '.join(property_projections)}
            """
            
            result = gremlin_client.submit(query).all().result()
            
            if result:
                results.append(result[0])
            else:
                # Entity not found, return with null properties
                entity_result = {"entity_id": entity_id}
                for prop in request.properties:
                    entity_result[prop] = None
                results.append(entity_result)
        
        gremlin_client.close()
        
        return EntityQueryResponse(entities=results)
        
    except Exception as e:
        logger.error(f"Error querying entity properties: {e}")
        if 'gremlin_client' in locals():
            gremlin_client.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/entities/{entity_id}/properties")
async def update_entity_properties(
    entity_id: str,
    properties: Dict[str, Any],
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Update properties for an entity
    
    This endpoint updates properties on an existing entity in the graph.
    """
    tenant_id = context["tenant_id"]
    gremlin_client = get_gremlin_client()
    
    try:
        # Build property update query
        property_updates = []
        for key, value in properties.items():
            if isinstance(value, str):
                value_str = f"'{value}'"
            elif isinstance(value, bool):
                value_str = str(value).lower()
            elif isinstance(value, list):
                value_str = str(value)
            else:
                value_str = str(value)
            property_updates.append(f".property('{key}', {value_str})")
        
        query = f"""
            g.V().has('entity_id', '{entity_id}')
            .has('tenant_id', '{tenant_id}')
            {' '.join(property_updates)}
            .property('last_updated', '{datetime.utcnow().isoformat()}')
        """
        
        result = gremlin_client.submit(query).all().result()
        gremlin_client.close()
        
        if not result:
            raise HTTPException(status_code=404, detail="Entity not found")
        
        return {"status": "updated", "entity_id": entity_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating entity properties: {e}")
        if 'gremlin_client' in locals():
            gremlin_client.close()
        raise HTTPException(status_code=500, detail=str(e)) 