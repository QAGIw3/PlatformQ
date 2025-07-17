from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, UploadFile, File
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
import logging
import httpx
import json
import avro.schema
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

# Schema Registry configuration
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Models
class SchemaDefinition(BaseModel):
    subject: str = Field(..., description="Schema subject name")
    schema: Dict[str, Any] = Field(..., description="Avro schema definition")
    compatibility: Optional[str] = Field("BACKWARD", description="Compatibility mode")
    references: Optional[List[Dict[str, Any]]] = Field(None, description="Schema references")

class SchemaVersion(BaseModel):
    subject: str
    id: int
    version: int
    schema: str

class SchemaCompatibilityCheck(BaseModel):
    subject: str
    schema: Dict[str, Any]
    version: Optional[str] = Field("latest", description="Version to check against")

# Create FastAPI app
app = create_base_app(
    service_name="schema-registry-service",
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None
)

# Core schema definitions for the platform
CORE_SCHEMAS = {
    "digital-asset": {
        "type": "record",
        "name": "DigitalAsset",
        "namespace": "com.platformq.core",
        "fields": [
            {"name": "asset_id", "type": "string"},
            {"name": "asset_name", "type": "string"},
            {"name": "asset_type", "type": "string"},
            {"name": "owner_id", "type": "string"},
            {"name": "tenant_id", "type": "string"},
            {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "updated_at", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "metadata", "type": {"type": "map", "values": "string"}},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "storage_uri", "type": ["null", "string"], "default": None}
        ]
    },
    "cad-session": {
        "type": "record",
        "name": "CADSession",
        "namespace": "com.platformq.cad",
        "fields": [
            {"name": "session_id", "type": "string"},
            {"name": "asset_id", "type": "string"},
            {"name": "tenant_id", "type": "string"},
            {"name": "active_users", "type": {"type": "array", "items": "string"}},
            {"name": "operation_count", "type": "int"},
            {"name": "vector_clock", "type": {"type": "map", "values": "long"}},
            {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
            {"name": "last_activity", "type": "long", "logicalType": "timestamp-millis"}
        ]
    },
    "data-quality-metric": {
        "type": "record",
        "name": "DataQualityMetric",
        "namespace": "com.platformq.quality",
        "fields": [
            {"name": "dataset_id", "type": "string"},
            {"name": "metric_type", "type": {"type": "enum", "name": "MetricType", 
                "symbols": ["COMPLETENESS", "ACCURACY", "CONSISTENCY", "VALIDITY", "UNIQUENESS"]}},
            {"name": "score", "type": "double"},
            {"name": "details", "type": {"type": "map", "values": "string"}},
            {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
        ]
    }
}

@app.on_event("startup")
async def startup_event():
    """Register core schemas on startup"""
    logger.info("Registering core platform schemas...")
    for subject, schema_dict in CORE_SCHEMAS.items():
        try:
            schema_str = json.dumps(schema_dict)
            schema = Schema(schema_str, schema_type="AVRO")
            schema_id = schema_registry_client.register_schema(subject, schema)
            logger.info(f"Registered schema '{subject}' with ID: {schema_id}")
        except Exception as e:
            logger.error(f"Failed to register schema '{subject}': {e}")

# API Endpoints
@app.post("/api/v1/schemas", response_model=SchemaVersion)
async def register_schema(schema_def: SchemaDefinition):
    """Register a new schema or version"""
    try:
        # Validate Avro schema
        avro_schema = avro.schema.parse(json.dumps(schema_def.schema))
        
        # Set compatibility if specified
        if schema_def.compatibility:
            schema_registry_client.set_compatibility(
                schema_def.subject, 
                schema_def.compatibility
            )
        
        # Register schema
        schema_str = json.dumps(schema_def.schema)
        schema = Schema(schema_str, schema_type="AVRO", references=schema_def.references)
        schema_id = schema_registry_client.register_schema(schema_def.subject, schema)
        
        # Get version info
        version = schema_registry_client.get_latest_version(schema_def.subject)
        
        return SchemaVersion(
            subject=schema_def.subject,
            id=schema_id,
            version=version.version,
            schema=schema_str
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/schemas/{subject}/versions")
async def list_versions(subject: str):
    """List all versions of a schema"""
    try:
        versions = schema_registry_client.get_versions(subject)
        return {"subject": subject, "versions": versions}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Subject '{subject}' not found")

@app.get("/api/v1/schemas/{subject}/versions/{version}")
async def get_schema_version(subject: str, version: str):
    """Get a specific version of a schema"""
    try:
        if version == "latest":
            registered_schema = schema_registry_client.get_latest_version(subject)
        else:
            registered_schema = schema_registry_client.get_version(subject, int(version))
        
        return SchemaVersion(
            subject=subject,
            id=registered_schema.schema_id,
            version=registered_schema.version,
            schema=registered_schema.schema.schema_str
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/api/v1/schemas/compatibility/check")
async def check_compatibility(check: SchemaCompatibilityCheck):
    """Check if a schema is compatible with existing versions"""
    try:
        schema_str = json.dumps(check.schema)
        is_compatible = schema_registry_client.test_compatibility(
            check.subject,
            Schema(schema_str, schema_type="AVRO"),
            check.version
        )
        return {"compatible": is_compatible}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/schemas")
async def list_subjects():
    """List all registered subjects"""
    try:
        subjects = schema_registry_client.get_subjects()
        return {"subjects": subjects}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/v1/schemas/{subject}")
async def delete_subject(subject: str, permanent: bool = False):
    """Delete a subject and all its versions"""
    try:
        versions = schema_registry_client.delete_subject(subject, permanent=permanent)
        return {"deleted": True, "versions": versions}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/api/v1/schemas/types/{namespace}")
async def get_schemas_by_namespace(namespace: str):
    """Get all schemas in a specific namespace"""
    try:
        subjects = schema_registry_client.get_subjects()
        matching_schemas = []
        
        for subject in subjects:
            try:
                latest = schema_registry_client.get_latest_version(subject)
                schema_dict = json.loads(latest.schema.schema_str)
                if schema_dict.get("namespace") == namespace:
                    matching_schemas.append({
                        "subject": subject,
                        "name": schema_dict.get("name"),
                        "version": latest.version,
                        "id": latest.schema_id
                    })
            except:
                continue
                
        return {"namespace": namespace, "schemas": matching_schemas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Schema evolution helper
@app.post("/api/v1/schemas/evolve")
async def evolve_schema(
    subject: str,
    add_fields: Optional[List[Dict[str, Any]]] = None,
    remove_fields: Optional[List[str]] = None,
    rename_fields: Optional[Dict[str, str]] = None
):
    """Helper to evolve an existing schema"""
    try:
        # Get current schema
        latest = schema_registry_client.get_latest_version(subject)
        schema_dict = json.loads(latest.schema.schema_str)
        
        # Apply evolution
        if add_fields:
            for field in add_fields:
                # Ensure backward compatibility with defaults
                if "default" not in field:
                    field["default"] = None
                schema_dict["fields"].append(field)
        
        if remove_fields:
            # Only remove if compatibility allows
            schema_dict["fields"] = [
                f for f in schema_dict["fields"] 
                if f["name"] not in remove_fields
            ]
        
        if rename_fields:
            for field in schema_dict["fields"]:
                if field["name"] in rename_fields:
                    field["name"] = rename_fields[field["name"]]
        
        # Register new version
        new_schema = Schema(json.dumps(schema_dict), schema_type="AVRO")
        schema_id = schema_registry_client.register_schema(subject, new_schema)
        
        return {
            "subject": subject,
            "new_version": schema_registry_client.get_latest_version(subject).version,
            "schema_id": schema_id,
            "evolved_schema": schema_dict
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Health check
@app.get("/health")
async def health_check():
    try:
        # Check schema registry connectivity
        subjects = schema_registry_client.get_subjects()
        return {
            "status": "healthy",
            "schema_registry": "connected",
            "registered_subjects": len(subjects)
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        } 