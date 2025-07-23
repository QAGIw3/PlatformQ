"""
Lineage Service

Business logic for lineage operations.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging

from app.infrastructure.repositories import LineageRepository, EntityRepository
from app.core.lineage_processor import LineageDirection, ProcessType
from app.events import EventBus
from app.services.interfaces import ServiceResult

logger = logging.getLogger(__name__)


class LineageService:
    """
    Service layer for lineage operations.
    
    Handles lineage creation, traversal, impact analysis, and compliance tracking.
    """
    
    def __init__(
        self,
        repository: LineageRepository,
        entity_repository: EntityRepository,
        event_bus: EventBus
    ):
        self.repository = repository
        self.entity_repository = entity_repository
        self.event_bus = event_bus
        
    async def create_lineage(
        self,
        process_name: str,
        process_type: ProcessType,
        inputs: List[str],
        outputs: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Create lineage relationship between entities.
        
        Args:
            process_name: Name of the process
            process_type: Type of process (ETL, STREAMING, etc.)
            inputs: List of input entity GUIDs
            outputs: List of output entity GUIDs
            metadata: Additional process metadata
            
        Returns:
            ServiceResult with created lineage
        """
        try:
            # Validate inputs and outputs exist
            validation_errors = await self._validate_entities(inputs + outputs)
            if validation_errors:
                return ServiceResult.failure(
                    error="Invalid entities in lineage",
                    details={"errors": validation_errors}
                )
            
            # Create lineage
            lineage = await self.repository.create_lineage(
                process_name=process_name,
                process_type=process_type,
                inputs=inputs,
                outputs=outputs,
                metadata=metadata
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "LineageCreated",
                "process_guid": lineage['guid'],
                "process_name": process_name,
                "inputs": inputs,
                "outputs": outputs
            })
            
            return ServiceResult.success(lineage)
            
        except Exception as e:
            logger.error(f"Failed to create lineage: {e}")
            return ServiceResult.failure(
                error="Failed to create lineage",
                details={"error": str(e)}
            )
    
    async def get_lineage(
        self,
        entity_guid: str,
        direction: LineageDirection = LineageDirection.BOTH,
        depth: int = 3
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Get lineage graph for an entity.
        
        Args:
            entity_guid: Entity GUID to get lineage for
            direction: Lineage direction (UPSTREAM, DOWNSTREAM, BOTH)
            depth: Maximum traversal depth
            
        Returns:
            ServiceResult with lineage graph
        """
        try:
            # Validate entity exists
            entity = await self.entity_repository.find_by_id(entity_guid)
            if not entity:
                return ServiceResult.failure(
                    error="Entity not found",
                    details={"entity_guid": entity_guid}
                )
            
            # Get lineage
            lineage = await self.repository.get_lineage(
                entity_guid=entity_guid,
                direction=direction,
                depth=depth
            )
            
            # Enhance with metadata
            enhanced_lineage = await self._enhance_lineage(lineage)
            
            return ServiceResult.success(enhanced_lineage)
            
        except Exception as e:
            logger.error(f"Failed to get lineage: {e}")
            return ServiceResult.failure(
                error="Failed to get lineage",
                details={"error": str(e)}
            )
    
    async def analyze_impact(
        self,
        entity_guid: str,
        change_type: str = "schema_change",
        changes: Optional[List[Dict[str, Any]]] = None,
        max_depth: int = 5
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Analyze impact of changes to an entity.
        
        Args:
            entity_guid: Entity to analyze impact for
            change_type: Type of change
            changes: Specific changes to analyze
            max_depth: Maximum analysis depth
            
        Returns:
            ServiceResult with impact analysis
        """
        try:
            # Get downstream lineage
            lineage = await self.repository.get_lineage(
                entity_guid=entity_guid,
                direction=LineageDirection.DOWNSTREAM,
                depth=max_depth
            )
            
            # Analyze impact
            impact_analysis = await self._analyze_entity_impact(
                entity_guid=entity_guid,
                lineage=lineage,
                change_type=change_type,
                changes=changes
            )
            
            # Calculate risk score
            risk_score = self._calculate_risk_score(impact_analysis)
            
            # Generate recommendations
            recommendations = self._generate_recommendations(
                impact_analysis,
                change_type,
                risk_score
            )
            
            result = {
                "entity_guid": entity_guid,
                "change_type": change_type,
                "impacted_entities": impact_analysis['impacted_entities'],
                "impact_paths": impact_analysis['impact_paths'],
                "risk_score": risk_score,
                "risk_level": self._determine_risk_level(risk_score),
                "recommendations": recommendations,
                "analysis_depth": max_depth
            }
            
            return ServiceResult.success(result)
            
        except Exception as e:
            logger.error(f"Failed to analyze impact: {e}")
            return ServiceResult.failure(
                error="Failed to analyze impact",
                details={"error": str(e)}
            )
    
    async def track_transformation(
        self,
        source_entities: List[Dict[str, Any]],
        target_entities: List[Dict[str, Any]],
        transformation: Dict[str, Any],
        execution_context: Optional[Dict[str, Any]] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Track data transformation for compliance and audit.
        
        Args:
            source_entities: Source entities in transformation
            target_entities: Target entities in transformation
            transformation: Transformation details
            execution_context: Execution context
            
        Returns:
            ServiceResult with tracking information
        """
        try:
            # Extract entity GUIDs
            source_guids = [e['guid'] for e in source_entities]
            target_guids = [e['guid'] for e in target_entities]
            
            # Create transformation process
            process_name = transformation.get('name', 'Transformation')
            process_type = ProcessType.ETL
            
            # Create lineage
            lineage_result = await self.create_lineage(
                process_name=process_name,
                process_type=process_type,
                inputs=source_guids,
                outputs=target_guids,
                metadata={
                    'transformation': transformation,
                    'execution_context': execution_context,
                    'tracked_at': datetime.utcnow().isoformat()
                }
            )
            
            if not lineage_result.success:
                return lineage_result
            
            # Track compliance if required
            if transformation.get('track_compliance', False):
                await self._track_compliance_event(
                    lineage_result.data,
                    transformation
                )
            
            return lineage_result
            
        except Exception as e:
            logger.error(f"Failed to track transformation: {e}")
            return ServiceResult.failure(
                error="Failed to track transformation",
                details={"error": str(e)}
            )
    
    async def get_compliance_audit_trail(
        self,
        entity_guid: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        include_lineage: bool = True
    ) -> ServiceResult[Dict[str, Any]]:
        """Get compliance audit trail for an entity"""
        try:
            # Get audit events
            audit_trail = await self.repository.get_audit_trail(
                entity_guid=entity_guid,
                start_date=start_date,
                end_date=end_date
            )
            
            # Include lineage if requested
            if include_lineage:
                lineage = await self.repository.get_lineage(
                    entity_guid=entity_guid,
                    direction=LineageDirection.BOTH,
                    depth=3
                )
                audit_trail['lineage'] = lineage
            
            return ServiceResult.success(audit_trail)
            
        except Exception as e:
            logger.error(f"Failed to get compliance audit trail: {e}")
            return ServiceResult.failure(
                error="Failed to get compliance audit trail",
                details={"error": str(e)}
            )
    
    async def find_sensitive_data_flows(
        self,
        classification: str = "PII",
        start_entity: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Find flows of sensitive data through the system"""
        try:
            # Find entities with classification
            classified_entities = await self.entity_repository.find_by_classification(
                classification
            )
            
            # Analyze flows
            flows = []
            for entity in classified_entities:
                if start_entity and entity.guid != start_entity:
                    continue
                    
                # Get lineage
                lineage = await self.repository.get_lineage(
                    entity_guid=entity.guid,
                    direction=LineageDirection.DOWNSTREAM,
                    depth=10
                )
                
                # Find sensitive paths
                sensitive_paths = self._find_sensitive_paths(
                    lineage,
                    classification
                )
                
                if sensitive_paths:
                    flows.append({
                        "source_entity": entity.guid,
                        "classification": classification,
                        "paths": sensitive_paths,
                        "risk_level": self._assess_data_flow_risk(sensitive_paths)
                    })
            
            result = {
                "classification": classification,
                "total_flows": len(flows),
                "flows": flows,
                "recommendations": self._generate_privacy_recommendations(flows)
            }
            
            return ServiceResult.success(result)
            
        except Exception as e:
            logger.error(f"Failed to find sensitive data flows: {e}")
            return ServiceResult.failure(
                error="Failed to find sensitive data flows",
                details={"error": str(e)}
            )
    
    async def visualize_lineage(
        self,
        entity_guid: str,
        depth: int = 3,
        direction: LineageDirection = LineageDirection.BOTH,
        include_columns: bool = False
    ) -> ServiceResult[Dict[str, Any]]:
        """Generate lineage visualization data"""
        try:
            lineage = await self.repository.get_lineage(
                entity_guid=entity_guid,
                direction=direction,
                depth=depth
            )
            
            # Convert to visualization format
            visualization = {
                "nodes": [],
                "edges": [],
                "layout": "hierarchical"
            }
            
            # Process nodes
            for node in lineage.get('nodes', []):
                visualization['nodes'].append({
                    "id": node['guid'],
                    "label": node['name'],
                    "type": self._determine_node_type(node['type_name']),
                    "metadata": node.get('attributes', {})
                })
            
            # Process edges
            for edge in lineage.get('edges', []):
                visualization['edges'].append({
                    "source": edge['from_guid'],
                    "target": edge['to_guid'],
                    "label": self._get_edge_label(edge['relationship_type']),
                    "metadata": edge.get('attributes', {})
                })
            
            # Include column lineage if requested
            if include_columns:
                column_lineage = await self._get_column_lineage(entity_guid)
                visualization['column_lineage'] = column_lineage
            
            return ServiceResult.success(visualization)
            
        except Exception as e:
            logger.error(f"Failed to visualize lineage: {e}")
            return ServiceResult.failure(
                error="Failed to visualize lineage",
                details={"error": str(e)}
            )
    
    async def _validate_entities(self, entity_guids: List[str]) -> List[str]:
        """Validate that entities exist"""
        errors = []
        for guid in entity_guids:
            entity = await self.entity_repository.find_by_id(guid)
            if not entity:
                errors.append(f"Entity not found: {guid}")
        return errors
    
    async def _enhance_lineage(self, lineage: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance lineage with additional metadata"""
        # Add quality scores, classifications, etc.
        for node in lineage.get('nodes', []):
            entity = await self.entity_repository.find_by_id(node['guid'])
            if entity:
                node['classifications'] = entity.classifications
                node['quality_score'] = entity.attributes.get('quality_score')
                node['last_updated'] = entity.modified_time
        
        return lineage
    
    async def _analyze_entity_impact(
        self,
        entity_guid: str,
        lineage: Dict[str, Any],
        change_type: str,
        changes: Optional[List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """Analyze impact of changes on downstream entities"""
        impacted_entities = []
        impact_paths = []
        
        # Traverse lineage graph
        for node in lineage.get('nodes', []):
            if node['guid'] != entity_guid:
                impact = {
                    "entity_guid": node['guid'],
                    "entity_name": node['name'],
                    "entity_type": node['type_name'],
                    "impact_type": self._determine_impact_type(change_type),
                    "severity": self._assess_impact_severity(node, changes)
                }
                impacted_entities.append(impact)
        
        # Find impact paths
        # TODO: Implement path finding algorithm
        
        return {
            "impacted_entities": impacted_entities,
            "impact_paths": impact_paths
        }
    
    def _calculate_risk_score(self, impact_analysis: Dict[str, Any]) -> float:
        """Calculate risk score based on impact analysis"""
        score = 0.0
        
        # Factor in number of impacted entities
        impacted_count = len(impact_analysis['impacted_entities'])
        score += min(impacted_count * 0.1, 0.5)
        
        # Factor in severity levels
        for impact in impact_analysis['impacted_entities']:
            if impact['severity'] == 'critical':
                score += 0.3
            elif impact['severity'] == 'high':
                score += 0.2
            elif impact['severity'] == 'medium':
                score += 0.1
        
        return min(score, 1.0)
    
    def _determine_risk_level(self, risk_score: float) -> str:
        """Determine risk level from score"""
        if risk_score >= 0.8:
            return "critical"
        elif risk_score >= 0.6:
            return "high"
        elif risk_score >= 0.3:
            return "medium"
        else:
            return "low"
    
    def _generate_recommendations(
        self,
        impact_analysis: Dict[str, Any],
        change_type: str,
        risk_score: float
    ) -> List[str]:
        """Generate recommendations based on impact analysis"""
        recommendations = []
        
        if risk_score > 0.6:
            recommendations.append("Consider phased rollout due to high risk")
            recommendations.append("Implement comprehensive testing strategy")
        
        if change_type == "schema_change":
            recommendations.append("Update dependent ETL processes")
            recommendations.append("Notify downstream consumers")
        
        # Add more specific recommendations based on impact
        
        return recommendations
    
    async def _track_compliance_event(
        self,
        lineage: Dict[str, Any],
        transformation: Dict[str, Any]
    ):
        """Track compliance event for audit"""
        # Publish compliance event
        await self.event_bus.publish({
            "event_type": "ComplianceTracked",
            "process_guid": lineage['guid'],
            "transformation": transformation,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def _find_sensitive_paths(
        self,
        lineage: Dict[str, Any],
        classification: str
    ) -> List[List[str]]:
        """Find paths containing sensitive data"""
        # TODO: Implement path finding for sensitive data
        return []
    
    def _assess_data_flow_risk(self, paths: List[List[str]]) -> str:
        """Assess risk level of data flows"""
        # Simple assessment based on path characteristics
        if any(self._is_external_path(path) for path in paths):
            return "high"
        elif len(paths) > 5:
            return "medium"
        else:
            return "low"
    
    def _is_external_path(self, path: List[str]) -> bool:
        """Check if path leads to external system"""
        # TODO: Implement check for external systems
        return False
    
    def _generate_privacy_recommendations(
        self,
        flows: List[Dict[str, Any]]
    ) -> List[str]:
        """Generate privacy recommendations"""
        recommendations = []
        
        high_risk_flows = [f for f in flows if f['risk_level'] == 'high']
        if high_risk_flows:
            recommendations.append("Review data sharing with external systems")
            recommendations.append("Implement additional access controls")
        
        return recommendations
    
    def _determine_node_type(self, entity_type: str) -> str:
        """Determine visualization node type"""
        type_mapping = {
            "dataset": "data",
            "process": "process",
            "report": "output",
            "api": "interface"
        }
        return type_mapping.get(entity_type.lower(), "default")
    
    def _get_edge_label(self, relationship_type: str) -> str:
        """Get edge label for visualization"""
        label_mapping = {
            "process_input": "reads from",
            "process_output": "writes to",
            "derived_from": "derived from"
        }
        return label_mapping.get(relationship_type, relationship_type)
    
    async def _get_column_lineage(self, entity_guid: str) -> Dict[str, Any]:
        """Get column-level lineage for entity"""
        # TODO: Implement column-level lineage
        return {}
    
    def _determine_impact_type(self, change_type: str) -> str:
        """Determine type of impact"""
        return change_type
    
    def _assess_impact_severity(
        self,
        node: Dict[str, Any],
        changes: Optional[List[Dict[str, Any]]]
    ) -> str:
        """Assess severity of impact on a node"""
        # Simple severity assessment
        if node.get('type_name') == 'critical_report':
            return "critical"
        elif node.get('classifications') and 'PII' in node.get('classifications', []):
            return "high"
        else:
            return "medium" 