"""
Data Lineage Processor
"""

import asyncio
import json
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import uuid

from platformq_shared.logging import get_logger
from platformq_events import EventStream, Event, EventType
from ..core.config import Settings
from ..core.atlas_client import AtlasClient
from ..core.cache_manager import CacheManager

logger = get_logger(__name__)


class LineageDirection(str, Enum):
    """Lineage traversal direction"""
    UPSTREAM = "upstream"      # Sources/inputs
    DOWNSTREAM = "downstream"  # Targets/outputs  
    BOTH = "both"              # Both directions


class ProcessType(str, Enum):
    """Types of processes in lineage"""
    ETL = "etl"
    STREAMING = "streaming"
    MANUAL = "manual"
    API = "api"
    ML_TRAINING = "ml_training"
    QUALITY_CHECK = "quality_check"


class ImpactLevel(str, Enum):
    """Impact analysis risk levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class LineageProcessor:
    """Processes and manages data lineage using Apache Atlas"""
    
    def __init__(self, atlas_client: AtlasClient, cache_manager: Optional[CacheManager] = None):
        self.atlas_client = atlas_client
        self.cache_manager = cache_manager
        self._processing_queue: asyncio.Queue = asyncio.Queue()
        self._processing_task: Optional[asyncio.Task] = None
        self._batch_size = 100
        self._batch_interval = 5  # seconds
        
    async def start(self):
        """Start the lineage processor"""
        logger.info("Starting lineage processor")
        self._processing_task = asyncio.create_task(self._process_lineage_events())
        
    async def stop(self):
        """Stop the lineage processor"""
        if self._processing_task:
            self._processing_task.cancel()
            
    async def _subscribe_to_events(self):
        """Subscribe to lineage-related events"""
        # Subscribe to various service events
        event_types = [
            "DataProcessed",
            "PipelineCompleted",
            "StreamJobStarted",
            "StreamJobCompleted",
            "MLModelTrained",
            "DataQualityChecked",
            "DataTransformed"
        ]
        
        for event_type in event_types:
            await self.event_stream.subscribe(
                event_type,
                self._handle_lineage_event,
                subscription_name=f"lineage_{event_type}"
            )
            
    async def _handle_lineage_event(self, event: Event):
        """Handle incoming lineage events"""
        try:
            # Extract lineage information from event
            lineage_info = {
                "event_type": event.type,
                "timestamp": event.timestamp,
                "process_name": event.data.get("process_name"),
                "process_type": event.data.get("process_type", ProcessType.ETL),
                "inputs": event.data.get("inputs", []),
                "outputs": event.data.get("outputs", []),
                "metadata": event.data.get("metadata", {})
            }
            
            # Add to processing queue
            await self._processing_queue.put(lineage_info)
            
        except Exception as e:
            logger.error(f"Error handling lineage event: {e}")
            
    async def _process_lineage_queue(self):
        """Process lineage events from queue"""
        batch = []
        
        while True:
            try:
                # Collect batch
                timeout = self.settings.event_batch_timeout
                deadline = datetime.utcnow() + timedelta(seconds=timeout)
                
                while len(batch) < self.settings.lineage_batch_size:
                    remaining = (deadline - datetime.utcnow()).total_seconds()
                    if remaining <= 0:
                        break
                        
                    try:
                        lineage_info = await asyncio.wait_for(
                            self._processing_queue.get(),
                            timeout=remaining
                        )
                        batch.append(lineage_info)
                    except asyncio.TimeoutError:
                        break
                        
                # Process batch
                if batch:
                    await self._process_lineage_batch(batch)
                    batch = []
                    
            except Exception as e:
                logger.error(f"Lineage processing error: {e}")
                await asyncio.sleep(5)
                
    async def _process_lineage_batch(self, batch: List[Dict[str, Any]]):
        """Process a batch of lineage events"""
        for lineage_info in batch:
            try:
                await self.create_lineage(
                    process_name=lineage_info['process_name'],
                    process_type=lineage_info['process_type'],
                    inputs=lineage_info['inputs'],
                    outputs=lineage_info['outputs'],
                    metadata=lineage_info['metadata']
                )
            except Exception as e:
                logger.error(f"Failed to create lineage: {e}")
                
    async def create_lineage(self,
                           process_name: str,
                           process_type: ProcessType,
                           inputs: List[str],
                           outputs: List[str],
                           metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create lineage relationship"""
        logger.info(f"Creating lineage for process: {process_name}")
        
        # Create or get process entity
        process_entity = await self._get_or_create_process(
            process_name, process_type, metadata
        )
        
        # Get input entities
        input_entities = []
        for input_ref in inputs:
            entity = await self._resolve_entity_reference(input_ref)
            if entity:
                input_entities.append(entity)
                
        # Get output entities
        output_entities = []
        for output_ref in outputs:
            entity = await self._resolve_entity_reference(output_ref)
            if entity:
                output_entities.append(entity)
                
        # Create lineage in Atlas
        lineage_request = {
            "guidEntityMap": {
                process_entity['guid']: process_entity
            },
            "relations": []
        }
        
        # Add input relationships
        for input_entity in input_entities:
            lineage_request['guidEntityMap'][input_entity['guid']] = input_entity
            lineage_request['relations'].append({
                "typeName": "dataset_process_inputs",
                "guid": -1,
                "end1": {
                    "guid": input_entity['guid'],
                    "typeName": input_entity['typeName']
                },
                "end2": {
                    "guid": process_entity['guid'],
                    "typeName": process_entity['typeName']
                }
            })
            
        # Add output relationships
        for output_entity in output_entities:
            lineage_request['guidEntityMap'][output_entity['guid']] = output_entity
            lineage_request['relations'].append({
                "typeName": "process_dataset_outputs",
                "guid": -2,
                "end1": {
                    "guid": process_entity['guid'],
                    "typeName": process_entity['typeName']
                },
                "end2": {
                    "guid": output_entity['guid'],
                    "typeName": output_entity['typeName']
                }
            })
            
        # Submit lineage
        response = await self.atlas_client.client.post(
            f"{self.atlas_client.base_url}/api/atlas/v2/entity/bulk",
            json=lineage_request
        )
        response.raise_for_status()
        
        result = response.json()
        
        # Clear lineage cache for affected entities
        for entity in input_entities + output_entities:
            cache_key = f"lineage:{entity['guid']}:*"
            await self.cache_manager.delete_pattern(cache_key)
            
        logger.info(f"Created lineage for process {process_name}")
        return result
        
    async def _get_or_create_process(self,
                                   process_name: str,
                                   process_type: ProcessType,
                                   metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Get or create process entity"""
        # Try to find existing process
        qualified_name = f"process.{process_name}"
        existing = await self.atlas_client.get_entity_by_attribute(
            "Process",
            "qualifiedName",
            qualified_name
        )
        
        if existing:
            return existing
            
        # Create new process entity
        process_entity = {
            "typeName": "Process",
            "attributes": {
                "name": process_name,
                "qualifiedName": qualified_name,
                "processType": process_type.value,
                "description": metadata.get('description', f"{process_type} process"),
                "owner": metadata.get('owner', 'system'),
                "startTime": datetime.utcnow().isoformat(),
                **metadata
            }
        }
        
        return await self.atlas_client.create_entity(process_entity)
        
    async def _resolve_entity_reference(self, reference: str) -> Optional[Dict[str, Any]]:
        """Resolve entity reference to actual entity"""
        # Reference can be:
        # - GUID: direct entity GUID
        # - Qualified name: e.g., "kafka.topic.orders"
        # - URI: e.g., "s3://bucket/path"
        
        # Try as GUID first
        if reference.replace('-', '').isalnum() and len(reference) == 36:
            entity = await self.atlas_client.get_entity_by_guid(reference)
            if entity:
                return entity
                
        # Try as qualified name
        # Guess entity type from qualified name
        if reference.startswith('kafka.'):
            entity_type = 'kafka_topic'
        elif reference.startswith('s3://'):
            entity_type = 's3_object'
        elif reference.startswith('hdfs://'):
            entity_type = 'hdfs_path'
        else:
            entity_type = 'DataSet'  # Generic
            
        entity = await self.atlas_client.get_entity_by_attribute(
            entity_type,
            "qualifiedName",
            reference
        )
        
        if entity:
            return entity
            
        # Create placeholder entity if not found
        logger.warning(f"Entity not found for reference: {reference}, creating placeholder")
        placeholder = {
            "typeName": entity_type,
            "attributes": {
                "name": reference.split('.')[-1],
                "qualifiedName": reference,
                "description": f"Auto-created from lineage reference"
            }
        }
        
        return await self.atlas_client.create_entity(placeholder)
        
    async def get_lineage(self,
                         entity_guid: str,
                         direction: LineageDirection = LineageDirection.BOTH,
                         depth: int = 3) -> Dict[str, Any]:
        """Get lineage for an entity"""
        # Check cache
        cache_key = f"lineage:{entity_guid}:{direction}:{depth}"
        cached = await self.cache_manager.get(cache_key)
        if cached:
            return cached
            
        # Get lineage from Atlas
        lineage = await self.atlas_client.get_lineage(entity_guid, direction.value, depth)
        
        # Enhance lineage with additional info
        enhanced = await self._enhance_lineage(lineage)
        
        # Cache result
        await self.cache_manager.set(cache_key, enhanced, ttl=self.settings.cache_ttl)
        
        return enhanced
        
    async def _enhance_lineage(self, lineage: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance lineage with additional information"""
        enhanced = lineage.copy()
        
        # Add entity details
        guid_map = enhanced.get('guidEntityMap', {})
        for guid, entity in guid_map.items():
            # Add quality scores if available
            if 'dataQualityScore' in entity.get('attributes', {}):
                entity['qualityInfo'] = {
                    "score": entity['attributes']['dataQualityScore'],
                    "lastChecked": entity['attributes'].get('lastQualityCheck')
                }
                
            # Add recent update info
            entity['recentlyUpdated'] = self._is_recently_updated(entity)
            
        # Calculate lineage statistics
        enhanced['statistics'] = self._calculate_lineage_stats(enhanced)
        
        return enhanced
        
    def _is_recently_updated(self, entity: Dict[str, Any]) -> bool:
        """Check if entity was recently updated"""
        update_time = entity.get('updateTime')
        if not update_time:
            return False
            
        try:
            updated = datetime.fromisoformat(update_time.replace('Z', '+00:00'))
            return (datetime.utcnow() - updated).days < 7
        except:
            return False
            
    def _calculate_lineage_stats(self, lineage: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate lineage statistics"""
        stats = {
            "totalEntities": len(lineage.get('guidEntityMap', {})),
            "totalRelations": len(lineage.get('relations', [])),
            "entityTypes": defaultdict(int),
            "maxDepthReached": False
        }
        
        # Count entity types
        for entity in lineage.get('guidEntityMap', {}).values():
            stats['entityTypes'][entity['typeName']] += 1
            
        stats['entityTypes'] = dict(stats['entityTypes'])
        
        return stats
        
    async def track_transformation(
        self,
        source_entities: List[Dict[str, Any]],
        target_entities: List[Dict[str, Any]], 
        transformation: Dict[str, Any],
        execution_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Track a data transformation with detailed lineage
        
        Enhanced to support column-level lineage, transformation logic capture,
        and compliance tracking
        """
        try:
            # Create process entity for transformation
            process_attrs = {
                "name": transformation.get("name", f"transformation_{datetime.utcnow().timestamp()}"),
                "qualifiedName": transformation.get("qualified_name", f"process.{transformation.get('type', 'unknown')}.{uuid.uuid4()}"),
                "processType": transformation.get("type", ProcessType.ETL.value),
                "description": transformation.get("description", ""),
                "transformation_logic": transformation.get("logic", ""),
                "job_id": transformation.get("job_id"),
                "executed_at": transformation.get("executed_at", datetime.utcnow().isoformat()),
                "execution_time_ms": transformation.get("execution_time_ms", 0),
                "records_processed": transformation.get("records_processed", 0),
                "status": transformation.get("status", "completed")
            }
            
            # Add execution context if provided
            if execution_context:
                process_attrs.update({
                    "user": execution_context.get("user"),
                    "environment": execution_context.get("environment"),
                    "version": execution_context.get("version"),
                    "parameters": json.dumps(execution_context.get("parameters", {}))
                })
            
            # Create process entity
            process_entity = await self.atlas_client.create_entity(
                type_name="Process",
                attributes=process_attrs
            )
            
            # Track column-level lineage if provided
            column_lineage = []
            if "column_mappings" in transformation:
                for mapping in transformation["column_mappings"]:
                    column_lineage.append({
                        "source_column": mapping["source"],
                        "target_column": mapping["target"],
                        "transformation": mapping.get("transformation", "direct")
                    })
            
            # Create lineage relationships
            lineage_info = {
                "process_guid": process_entity["guid"],
                "inputs": [],
                "outputs": [],
                "column_lineage": column_lineage
            }
            
            # Process source entities
            for source in source_entities:
                source_guid = await self._get_or_create_entity(source)
                lineage_info["inputs"].append(source_guid)
                
                # Create input relationship
                await self.atlas_client.create_relationship(
                    type_name="process_dataset_inputs",
                    end1_guid=process_entity["guid"],
                    end2_guid=source_guid
                )
            
            # Process target entities
            for target in target_entities:
                target_guid = await self._get_or_create_entity(target)
                lineage_info["outputs"].append(target_guid)
                
                # Create output relationship
                await self.atlas_client.create_relationship(
                    type_name="process_dataset_outputs",
                    end1_guid=process_entity["guid"],
                    end2_guid=target_guid
                )
            
            # Cache lineage for fast retrieval
            if self.cache_manager:
                cache_key = f"lineage:transformation:{process_entity['guid']}"
                await self.cache_manager.set(cache_key, lineage_info, ttl=3600)
            
            # Track for compliance if needed
            if transformation.get("track_compliance", False):
                await self._track_compliance_event(process_entity, transformation)
            
            return {
                "process_guid": process_entity["guid"],
                "lineage_tracked": True,
                "inputs": len(lineage_info["inputs"]),
                "outputs": len(lineage_info["outputs"]),
                "column_mappings": len(column_lineage)
            }
            
        except Exception as e:
            logger.error(f"Failed to track transformation: {e}")
            raise
            
    async def analyze_impact(
        self,
        entity_guid: str,
        change_type: str = "schema_change",
        changes: Optional[List[Dict[str, Any]]] = None,
        max_depth: int = 5,
        include_indirect: bool = True
    ) -> Dict[str, Any]:
        """
        Analyze the impact of changes to an entity
        
        Enhanced impact analysis with risk assessment and recommendations
        """
        try:
            impact_result = {
                "entity_guid": entity_guid,
                "change_type": change_type,
                "changes": changes or [],
                "impacted_entities": [],
                "impact_paths": [],
                "risk_score": 0.0,
                "risk_level": ImpactLevel.LOW.value,
                "breaking_changes": [],
                "recommendations": [],
                "analysis_timestamp": datetime.utcnow().isoformat()
            }
            
            # Get entity details
            entity = await self.atlas_client.get_entity_by_guid(entity_guid)
            entity_type = entity["typeName"]
            
            # Get downstream lineage
            lineage = await self.get_lineage(
                entity_guid=entity_guid,
                direction=LineageDirection.DOWNSTREAM,
                depth=max_depth
            )
            
            # Analyze each impacted entity
            visited = set()
            impact_queue = [(entity_guid, [], 0)]  # (guid, path, depth)
            
            while impact_queue:
                current_guid, path, depth = impact_queue.pop(0)
                
                if current_guid in visited or depth > max_depth:
                    continue
                    
                visited.add(current_guid)
                
                # Get entity relationships
                relationships = await self._get_entity_relationships(current_guid)
                
                for rel in relationships:
                    if rel["relationshipType"] in ["process_dataset_outputs", "dataset_process_inputs"]:
                        target_guid = rel["endGuid"]
                        if target_guid not in visited:
                            new_path = path + [current_guid]
                            impact_queue.append((target_guid, new_path, depth + 1))
                            
                            # Analyze impact on this entity
                            impact = await self._analyze_entity_impact(
                                target_guid, 
                                changes,
                                change_type
                            )
                            
                            if impact["is_impacted"]:
                                impact_result["impacted_entities"].append({
                                    "guid": target_guid,
                                    "name": impact["entity_name"],
                                    "type": impact["entity_type"],
                                    "impact_type": impact["impact_type"],
                                    "severity": impact["severity"],
                                    "path": new_path + [target_guid]
                                })
                                
                                # Track breaking changes
                                if impact["severity"] == "high":
                                    impact_result["breaking_changes"].append(
                                        f"{impact['entity_name']} - {impact['impact_description']}"
                                    )
            
            # Calculate risk score
            impact_result["risk_score"] = self._calculate_risk_score(impact_result)
            impact_result["risk_level"] = self._determine_risk_level(impact_result["risk_score"])
            
            # Generate recommendations
            impact_result["recommendations"] = self._generate_recommendations(
                impact_result,
                change_type,
                changes
            )
            
            # Cache impact analysis
            if self.cache_manager:
                cache_key = f"impact:{entity_guid}:{change_type}:{hash(str(changes))}"
                await self.cache_manager.set(cache_key, impact_result, ttl=600)
            
            return impact_result
            
        except Exception as e:
            logger.error(f"Failed to analyze impact: {e}")
            raise
            
    async def get_compliance_audit_trail(
        self,
        entity_guid: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        include_lineage: bool = True
    ) -> Dict[str, Any]:
        """
        Get compliance audit trail for an entity
        
        Tracks all transformations, accesses, and modifications for compliance
        """
        try:
            audit_trail = {
                "entity_guid": entity_guid,
                "audit_events": [],
                "lineage_events": [],
                "data_classifications": [],
                "retention_info": {},
                "access_history": []
            }
            
            # Get entity details
            entity = await self.atlas_client.get_entity_by_guid(entity_guid)
            
            # Get audit events from Atlas
            audit_events = await self.atlas_client.get_audit_events(
                entity_guid=entity_guid,
                start_time=start_date,
                end_time=end_date
            )
            
            audit_trail["audit_events"] = [
                {
                    "timestamp": event["timestamp"],
                    "user": event["user"],
                    "action": event["action"],
                    "details": event.get("details", {}),
                    "result": event["result"]
                }
                for event in audit_events
            ]
            
            # Get lineage history if requested
            if include_lineage:
                lineage_history = await self._get_lineage_history(
                    entity_guid,
                    start_date,
                    end_date
                )
                audit_trail["lineage_events"] = lineage_history
            
            # Get data classifications
            classifications = entity.get("classifications", [])
            audit_trail["data_classifications"] = [
                {
                    "name": cls["typeName"],
                    "attributes": cls.get("attributes", {}),
                    "propagated": cls.get("propagate", False)
                }
                for cls in classifications
            ]
            
            # Get retention information
            audit_trail["retention_info"] = {
                "retention_period_days": entity.get("attributes", {}).get("retention_days", 2555),
                "deletion_date": entity.get("attributes", {}).get("deletion_date"),
                "legal_hold": entity.get("attributes", {}).get("legal_hold", False)
            }
            
            return audit_trail
            
        except Exception as e:
            logger.error(f"Failed to get compliance audit trail: {e}")
            raise
            
    async def find_sensitive_data_flows(
        self,
        classification: str = "PII",
        start_entity: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Find all data flows containing sensitive data
        
        Used for GDPR compliance and data governance
        """
        try:
            sensitive_flows = {
                "classification": classification,
                "data_sources": [],
                "processing_activities": [],
                "data_destinations": [],
                "flow_paths": [],
                "third_party_sharing": []
            }
            
            # Search for entities with the classification
            search_params = {
                "typeName": "*",
                "classification": classification,
                "limit": 1000
            }
            
            if start_entity:
                search_params["query"] = f"qualifiedName:{start_entity}*"
            
            classified_entities = await self.atlas_client.search_entities(search_params)
            
            # Analyze each classified entity
            for entity in classified_entities:
                entity_guid = entity["guid"]
                
                # Get full lineage
                upstream = await self.get_lineage(
                    entity_guid=entity_guid,
                    direction=LineageDirection.UPSTREAM,
                    depth=10
                )
                
                downstream = await self.get_lineage(
                    entity_guid=entity_guid,
                    direction=LineageDirection.DOWNSTREAM,
                    depth=10
                )
                
                # Categorize entities
                for node in upstream.get("guidEntityMap", {}).values():
                    if node["typeName"] in ["hive_table", "rdbms_table", "kafka_topic"]:
                        if self._is_source(node):
                            sensitive_flows["data_sources"].append({
                                "guid": node["guid"],
                                "name": node["attributes"]["name"],
                                "type": node["typeName"],
                                "location": node["attributes"].get("qualifiedName")
                            })
                
                # Track processing
                for node in upstream.get("guidEntityMap", {}).values():
                    if node["typeName"] == "Process":
                        sensitive_flows["processing_activities"].append({
                            "guid": node["guid"],
                            "name": node["attributes"]["name"],
                            "type": node["attributes"].get("processType"),
                            "description": node["attributes"].get("description")
                        })
                
                # Track destinations
                for node in downstream.get("guidEntityMap", {}).values():
                    if self._is_destination(node):
                        dest_info = {
                            "guid": node["guid"],
                            "name": node["attributes"]["name"],
                            "type": node["typeName"],
                            "location": node["attributes"].get("qualifiedName")
                        }
                        
                        # Check if third-party
                        if self._is_third_party(node):
                            sensitive_flows["third_party_sharing"].append(dest_info)
                        else:
                            sensitive_flows["data_destinations"].append(dest_info)
            
            return sensitive_flows
            
        except Exception as e:
            logger.error(f"Failed to find sensitive data flows: {e}")
            raise
            
    async def visualize_lineage(
        self,
        entity_guid: str,
        depth: int = 3,
        direction: LineageDirection = LineageDirection.BOTH,
        include_columns: bool = False
    ) -> Dict[str, Any]:
        """
        Get lineage data in a format suitable for visualization
        
        Returns D3.js compatible graph format
        """
        try:
            # Get lineage data
            lineage = await self.get_lineage(
                entity_guid=entity_guid,
                direction=direction,
                depth=depth
            )
            
            # Convert to visualization format
            nodes = []
            edges = []
            node_map = {}
            
            # Process entities into nodes
            for guid, entity in lineage.get("guidEntityMap", {}).items():
                node_type = self._determine_node_type(entity["typeName"])
                
                node = {
                    "id": guid,
                    "label": entity["attributes"].get("name", entity["attributes"].get("qualifiedName", guid)),
                    "type": node_type,
                    "entityType": entity["typeName"],
                    "attributes": {
                        "qualifiedName": entity["attributes"].get("qualifiedName"),
                        "owner": entity["attributes"].get("owner"),
                        "createTime": entity.get("createTime")
                    }
                }
                
                # Add column information if requested
                if include_columns and entity["typeName"] in ["hive_table", "rdbms_table"]:
                    columns = await self._get_entity_columns(guid)
                    node["columns"] = columns
                
                nodes.append(node)
                node_map[guid] = node
            
            # Process relationships into edges
            for rel in lineage.get("relations", []):
                edge = {
                    "source": rel["fromEntityId"],
                    "target": rel["toEntityId"],
                    "label": self._get_edge_label(rel["relationshipType"]),
                    "type": rel["relationshipType"]
                }
                
                # Add column-level lineage if available
                if include_columns and "columnMapping" in rel:
                    edge["columnMapping"] = rel["columnMapping"]
                
                edges.append(edge)
            
            return {
                "nodes": nodes,
                "edges": edges,
                "metadata": {
                    "rootEntity": entity_guid,
                    "depth": depth,
                    "direction": direction.value,
                    "totalNodes": len(nodes),
                    "totalEdges": len(edges)
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to visualize lineage: {e}")
            raise
            
    # Helper methods
    async def _analyze_entity_impact(
        self,
        entity_guid: str,
        changes: List[Dict[str, Any]],
        change_type: str
    ) -> Dict[str, Any]:
        """Analyze impact on a specific entity"""
        entity = await self.atlas_client.get_entity_by_guid(entity_guid)
        
        impact = {
            "entity_name": entity["attributes"]["name"],
            "entity_type": entity["typeName"],
            "is_impacted": False,
            "impact_type": "none",
            "severity": "low",
            "impact_description": ""
        }
        
        # Check different types of changes
        if change_type == "schema_change" and changes:
            for change in changes:
                if change.get("action") == "drop":
                    # Check if entity uses this column
                    if await self._entity_uses_column(entity_guid, change["column"]):
                        impact["is_impacted"] = True
                        impact["impact_type"] = "breaking"
                        impact["severity"] = "high"
                        impact["impact_description"] = f"Uses dropped column '{change['column']}'"
                        
                elif change.get("action") == "modify":
                    # Check compatibility
                    if not self._is_type_compatible(change.get("from"), change.get("to")):
                        impact["is_impacted"] = True
                        impact["impact_type"] = "potential_breaking"
                        impact["severity"] = "medium"
                        impact["impact_description"] = f"Column type change may be incompatible"
        
        return impact
        
    def _calculate_risk_score(self, impact_result: Dict[str, Any]) -> float:
        """Calculate risk score based on impact analysis"""
        score = 0.0
        
        # Factor in number of impacted entities
        score += min(len(impact_result["impacted_entities"]) * 0.1, 3.0)
        
        # Factor in breaking changes
        score += len(impact_result["breaking_changes"]) * 1.0
        
        # Factor in severity
        for entity in impact_result["impacted_entities"]:
            if entity["severity"] == "high":
                score += 2.0
            elif entity["severity"] == "medium":
                score += 1.0
            else:
                score += 0.5
        
        return min(score, 10.0)  # Cap at 10
        
    def _determine_risk_level(self, risk_score: float) -> str:
        """Determine risk level from score"""
        if risk_score >= 7.0:
            return ImpactLevel.CRITICAL.value
        elif risk_score >= 5.0:
            return ImpactLevel.HIGH.value
        elif risk_score >= 2.0:
            return ImpactLevel.MEDIUM.value
        else:
            return ImpactLevel.LOW.value
            
    def _generate_recommendations(
        self,
        impact_result: Dict[str, Any],
        change_type: str,
        changes: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate recommendations based on impact analysis"""
        recommendations = []
        
        if impact_result["risk_level"] in [ImpactLevel.HIGH.value, ImpactLevel.CRITICAL.value]:
            recommendations.append("Consider implementing changes in phases")
            recommendations.append("Create backups before applying changes")
            
        if impact_result["breaking_changes"]:
            recommendations.append("Update affected pipelines before applying changes")
            recommendations.append("Notify downstream consumers of breaking changes")
            
        if change_type == "schema_change":
            recommendations.append("Consider adding new columns instead of modifying existing ones")
            recommendations.append("Use feature flags to gradually roll out changes")
            
        return recommendations
        
    async def _track_compliance_event(
        self,
        process_entity: Dict[str, Any],
        transformation: Dict[str, Any]
    ):
        """Track event for compliance purposes"""
        compliance_event = {
            "event_type": "data_transformation",
            "process_guid": process_entity["guid"],
            "timestamp": datetime.utcnow().isoformat(),
            "data_classifications": transformation.get("classifications", []),
            "compliance_tags": transformation.get("compliance_tags", []),
            "retention_applied": transformation.get("retention_applied", False)
        }
        
        # Store in cache for compliance reporting
        if self.cache_manager:
            cache_key = f"compliance:event:{process_entity['guid']}"
            await self.cache_manager.set(cache_key, compliance_event, ttl=86400 * 30)  # 30 days
            
    async def _get_lineage_history(
        self,
        entity_guid: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> List[Dict[str, Any]]:
        """Get historical lineage events for an entity"""
        # This would query historical lineage data
        # For now, return current lineage as history
        lineage = await self.get_lineage(entity_guid, direction=LineageDirection.BOTH)
        
        history = []
        for rel in lineage.get("relations", []):
            history.append({
                "timestamp": datetime.utcnow().isoformat(),
                "relationship_type": rel["relationshipType"],
                "source": rel["fromEntityId"],
                "target": rel["toEntityId"],
                "process": rel.get("processId")
            })
            
        return history
        
    def _is_source(self, entity: Dict[str, Any]) -> bool:
        """Check if entity is a data source"""
        source_types = ["hive_db", "rdbms_db", "kafka_topic", "fs_path"]
        return entity["typeName"] in source_types
        
    def _is_destination(self, entity: Dict[str, Any]) -> bool:
        """Check if entity is a data destination"""
        # Could be more sophisticated based on attributes
        return not self._is_source(entity)
        
    def _is_third_party(self, entity: Dict[str, Any]) -> bool:
        """Check if entity represents third-party data sharing"""
        # Check qualified name or other attributes
        qualified_name = entity["attributes"].get("qualifiedName", "")
        return "external" in qualified_name or "third_party" in qualified_name
        
    def _determine_node_type(self, entity_type: str) -> str:
        """Determine visualization node type"""
        if entity_type in ["hive_table", "rdbms_table"]:
            return "table"
        elif entity_type in ["hive_db", "rdbms_db"]:
            return "database"
        elif entity_type == "Process":
            return "process"
        elif entity_type == "kafka_topic":
            return "stream"
        else:
            return "other"
            
    def _get_edge_label(self, relationship_type: str) -> str:
        """Get human-readable edge label"""
        labels = {
            "process_dataset_inputs": "reads from",
            "process_dataset_outputs": "writes to",
            "dataset_process_inputs": "input to",
            "dataset_process_outputs": "output from"
        }
        return labels.get(relationship_type, relationship_type)
        
    async def _get_entity_columns(self, entity_guid: str) -> List[Dict[str, Any]]:
        """Get column information for an entity"""
        # Query Atlas for column entities related to this table
        columns = []
        try:
            entity = await self.atlas_client.get_entity_by_guid(entity_guid, include_relationships=True)
            if "columns" in entity.get("relationshipAttributes", {}):
                for col in entity["relationshipAttributes"]["columns"]:
                    columns.append({
                        "name": col["displayText"],
                        "type": col.get("typeName", "unknown"),
                        "guid": col.get("guid")
                    })
        except Exception as e:
            logger.warning(f"Failed to get columns for entity {entity_guid}: {e}")
            
        return columns
        
    async def _entity_uses_column(self, entity_guid: str, column_name: str) -> bool:
        """Check if an entity uses a specific column"""
        # This would check the entity's dependencies
        # For now, simplified implementation
        return True  # Conservative assumption
        
    def _is_type_compatible(self, from_type: str, to_type: str) -> bool:
        """Check if a type change is compatible"""
        # Define compatibility rules
        compatible_changes = {
            "varchar(100)": ["varchar(255)", "text"],
            "int": ["bigint", "decimal"],
            "float": ["double", "decimal"]
        }
        
        if from_type in compatible_changes:
            return to_type in compatible_changes[from_type]
            
        return from_type == to_type 