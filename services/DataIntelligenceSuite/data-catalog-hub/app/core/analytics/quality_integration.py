"""
Quality Score Integration

Integrates with the Quality Engine Service to provide data trust scores
and quality metrics within the catalog.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio
import httpx
from collections import defaultdict

from ..atlas_client import AtlasClient
from ..config import settings

logger = logging.getLogger(__name__)


class QualityDimension(str, Enum):
    """Data quality dimensions"""
    ACCURACY = "accuracy"
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"


class TrustLevel(str, Enum):
    """Data trust levels"""
    VERIFIED = "verified"      # > 90% quality score
    TRUSTED = "trusted"        # 70-90% quality score
    ACCEPTABLE = "acceptable"  # 50-70% quality score
    REVIEW = "review"          # 30-50% quality score
    UNTRUSTED = "untrusted"    # < 30% quality score


@dataclass
class QualityProfile:
    """Data quality profile"""
    dataset_id: str
    overall_score: float
    trust_level: TrustLevel
    dimensions: Dict[QualityDimension, float]
    issues: List[Dict[str, Any]]
    recommendations: List[str]
    last_assessed: datetime
    trend: str  # improving, stable, declining
    metadata: Dict[str, Any]


@dataclass
class QualityRule:
    """Data quality rule"""
    rule_id: str
    name: str
    description: str
    dimension: QualityDimension
    severity: str  # critical, high, medium, low
    expression: str
    threshold: float
    enabled: bool


class QualityIntegrationEngine:
    """
    Integrates quality scores and trust metrics into the data catalog
    """
    
    def __init__(
        self,
        atlas_client: AtlasClient,
        quality_service_url: Optional[str] = None
    ):
        self.atlas_client = atlas_client
        self.quality_service_url = quality_service_url or settings.quality_service_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Cache for quality scores
        self.quality_cache = {}
        self.cache_ttl = 3600  # 1 hour
        
        # Trust level thresholds
        self.trust_thresholds = {
            TrustLevel.VERIFIED: 0.90,
            TrustLevel.TRUSTED: 0.70,
            TrustLevel.ACCEPTABLE: 0.50,
            TrustLevel.REVIEW: 0.30,
            TrustLevel.UNTRUSTED: 0.0
        }
        
    async def assess_dataset_quality(
        self,
        dataset_id: str,
        force_refresh: bool = False
    ) -> QualityProfile:
        """
        Assess quality of a dataset
        """
        # Check cache
        if not force_refresh and dataset_id in self.quality_cache:
            cached = self.quality_cache[dataset_id]
            if (datetime.utcnow() - cached["timestamp"]).seconds < self.cache_ttl:
                return cached["profile"]
        
        try:
            # Get quality assessment from quality service
            response = await self.http_client.post(
                f"{self.quality_service_url}/api/v1/quality/assess",
                json={"dataset_id": dataset_id}
            )
            
            if response.status_code != 200:
                logger.error(f"Quality service returned {response.status_code}")
                return self._create_default_profile(dataset_id)
            
            data = response.json()
            
            # Create quality profile
            profile = QualityProfile(
                dataset_id=dataset_id,
                overall_score=data.get("overall_score", 0.0),
                trust_level=self._determine_trust_level(data.get("overall_score", 0.0)),
                dimensions=self._parse_dimensions(data.get("dimensions", {})),
                issues=data.get("issues", []),
                recommendations=data.get("recommendations", []),
                last_assessed=datetime.fromisoformat(data.get("assessed_at", datetime.utcnow().isoformat())),
                trend=data.get("trend", "stable"),
                metadata=data.get("metadata", {})
            )
            
            # Update cache
            self.quality_cache[dataset_id] = {
                "profile": profile,
                "timestamp": datetime.utcnow()
            }
            
            # Update catalog
            await self._update_catalog_quality(dataset_id, profile)
            
            return profile
            
        except Exception as e:
            logger.error(f"Error assessing quality for {dataset_id}: {e}")
            return self._create_default_profile(dataset_id)
    
    def _determine_trust_level(self, score: float) -> TrustLevel:
        """
        Determine trust level based on quality score
        """
        for level, threshold in self.trust_thresholds.items():
            if score >= threshold:
                return level
        return TrustLevel.UNTRUSTED
    
    def _parse_dimensions(self, dimensions: Dict[str, Any]) -> Dict[QualityDimension, float]:
        """
        Parse quality dimensions
        """
        parsed = {}
        for dim in QualityDimension:
            if dim.value in dimensions:
                parsed[dim] = float(dimensions[dim.value])
            else:
                parsed[dim] = 0.0
        return parsed
    
    def _create_default_profile(self, dataset_id: str) -> QualityProfile:
        """
        Create default quality profile when assessment fails
        """
        return QualityProfile(
            dataset_id=dataset_id,
            overall_score=0.0,
            trust_level=TrustLevel.UNTRUSTED,
            dimensions={dim: 0.0 for dim in QualityDimension},
            issues=[{"type": "assessment_failed", "message": "Quality assessment unavailable"}],
            recommendations=["Schedule quality assessment"],
            last_assessed=datetime.utcnow(),
            trend="unknown",
            metadata={}
        )
    
    async def _update_catalog_quality(
        self,
        dataset_id: str,
        profile: QualityProfile
    ):
        """
        Update quality information in catalog
        """
        try:
            # Get entity
            entity = await self.atlas_client.get_entity(dataset_id)
            if not entity:
                return
            
            # Update quality attributes
            quality_attrs = {
                "dataQualityScore": profile.overall_score,
                "dataTrustLevel": profile.trust_level.value,
                "qualityDimensions": {
                    dim.value: score
                    for dim, score in profile.dimensions.items()
                },
                "qualityLastAssessed": profile.last_assessed.isoformat(),
                "qualityTrend": profile.trend,
                "qualityIssueCount": len(profile.issues)
            }
            
            await self.atlas_client.partial_update_entity(
                entity["guid"],
                quality_attrs
            )
            
            # Update classifications based on trust level
            await self._update_quality_classifications(entity["guid"], profile)
            
        except Exception as e:
            logger.error(f"Error updating catalog quality: {e}")
    
    async def _update_quality_classifications(
        self,
        entity_guid: str,
        profile: QualityProfile
    ):
        """
        Update quality-related classifications
        """
        classifications = []
        
        # Add trust level classification
        if profile.trust_level == TrustLevel.VERIFIED:
            classifications.append("VerifiedData")
        elif profile.trust_level == TrustLevel.TRUSTED:
            classifications.append("TrustedData")
        elif profile.trust_level in [TrustLevel.REVIEW, TrustLevel.UNTRUSTED]:
            classifications.append("QualityReview")
        
        # Add dimension-specific classifications
        for dim, score in profile.dimensions.items():
            if score < 0.5:
                classifications.append(f"Low{dim.value.title()}")
        
        if classifications:
            await self.atlas_client.add_classifications(entity_guid, classifications)
    
    async def create_quality_rules(
        self,
        dataset_id: str,
        auto_generate: bool = True
    ) -> List[QualityRule]:
        """
        Create quality rules for a dataset
        """
        rules = []
        
        try:
            # Get dataset schema
            entity = await self.atlas_client.get_entity(dataset_id)
            schema = entity.get("attributes", {}).get("schema")
            
            if not schema:
                return rules
            
            if auto_generate:
                # Generate rules based on schema
                rules.extend(self._generate_completeness_rules(schema))
                rules.extend(self._generate_validity_rules(schema))
                rules.extend(self._generate_uniqueness_rules(schema))
            
            # Register rules with quality service
            for rule in rules:
                await self._register_quality_rule(dataset_id, rule)
            
            return rules
            
        except Exception as e:
            logger.error(f"Error creating quality rules: {e}")
            return rules
    
    def _generate_completeness_rules(self, schema: Dict[str, Any]) -> List[QualityRule]:
        """
        Generate completeness rules based on schema
        """
        rules = []
        
        for field in schema.get("fields", []):
            if not field.get("nullable", True):
                rule = QualityRule(
                    rule_id=f"completeness_{field['name']}",
                    name=f"Completeness check for {field['name']}",
                    description=f"Ensure {field['name']} is not null",
                    dimension=QualityDimension.COMPLETENESS,
                    severity="high" if "id" in field["name"].lower() else "medium",
                    expression=f"{field['name']} IS NOT NULL",
                    threshold=1.0,
                    enabled=True
                )
                rules.append(rule)
        
        return rules
    
    def _generate_validity_rules(self, schema: Dict[str, Any]) -> List[QualityRule]:
        """
        Generate validity rules based on data types
        """
        rules = []
        
        for field in schema.get("fields", []):
            field_type = field.get("type", "").lower()
            field_name = field["name"]
            
            # Email validation
            if "email" in field_name.lower():
                rule = QualityRule(
                    rule_id=f"validity_email_{field_name}",
                    name=f"Email format check for {field_name}",
                    description=f"Ensure {field_name} contains valid email",
                    dimension=QualityDimension.VALIDITY,
                    severity="high",
                    expression=f"{field_name} REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{{2,}}$'",
                    threshold=0.95,
                    enabled=True
                )
                rules.append(rule)
            
            # Date validation
            elif "date" in field_type or "timestamp" in field_type:
                rule = QualityRule(
                    rule_id=f"validity_date_{field_name}",
                    name=f"Date range check for {field_name}",
                    description=f"Ensure {field_name} is within reasonable range",
                    dimension=QualityDimension.VALIDITY,
                    severity="medium",
                    expression=f"{field_name} BETWEEN '1900-01-01' AND CURRENT_DATE + INTERVAL '1 year'",
                    threshold=0.99,
                    enabled=True
                )
                rules.append(rule)
            
            # Numeric range validation
            elif any(t in field_type for t in ["int", "float", "decimal", "numeric"]):
                if "age" in field_name.lower():
                    rule = QualityRule(
                        rule_id=f"validity_range_{field_name}",
                        name=f"Age range check for {field_name}",
                        description=f"Ensure {field_name} is between 0 and 150",
                        dimension=QualityDimension.VALIDITY,
                        severity="high",
                        expression=f"{field_name} BETWEEN 0 AND 150",
                        threshold=0.99,
                        enabled=True
                    )
                    rules.append(rule)
        
        return rules
    
    def _generate_uniqueness_rules(self, schema: Dict[str, Any]) -> List[QualityRule]:
        """
        Generate uniqueness rules for potential keys
        """
        rules = []
        
        for field in schema.get("fields", []):
            field_name = field["name"].lower()
            
            # Check for potential unique fields
            if any(key in field_name for key in ["id", "code", "number", "key"]):
                if not any(skip in field_name for skip in ["description", "name"]):
                    rule = QualityRule(
                        rule_id=f"uniqueness_{field['name']}",
                        name=f"Uniqueness check for {field['name']}",
                        description=f"Ensure {field['name']} values are unique",
                        dimension=QualityDimension.UNIQUENESS,
                        severity="critical" if "id" in field_name else "high",
                        expression=f"COUNT(DISTINCT {field['name']}) = COUNT({field['name']})",
                        threshold=1.0,
                        enabled=True
                    )
                    rules.append(rule)
        
        return rules
    
    async def _register_quality_rule(self, dataset_id: str, rule: QualityRule):
        """
        Register quality rule with quality service
        """
        try:
            await self.http_client.post(
                f"{self.quality_service_url}/api/v1/quality/rules",
                json={
                    "dataset_id": dataset_id,
                    "rule": {
                        "id": rule.rule_id,
                        "name": rule.name,
                        "description": rule.description,
                        "dimension": rule.dimension.value,
                        "severity": rule.severity,
                        "expression": rule.expression,
                        "threshold": rule.threshold,
                        "enabled": rule.enabled
                    }
                }
            )
        except Exception as e:
            logger.error(f"Error registering quality rule: {e}")
    
    async def get_quality_trends(
        self,
        dataset_id: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Get quality trends for a dataset
        """
        try:
            response = await self.http_client.get(
                f"{self.quality_service_url}/api/v1/quality/trends",
                params={
                    "dataset_id": dataset_id,
                    "days": days
                }
            )
            
            if response.status_code == 200:
                return response.json()
            
        except Exception as e:
            logger.error(f"Error getting quality trends: {e}")
        
        return {
            "dataset_id": dataset_id,
            "trends": [],
            "summary": "No trend data available"
        }
    
    async def get_quality_recommendations(
        self,
        dataset_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get quality improvement recommendations
        """
        recommendations = []
        
        try:
            # Get current quality profile
            profile = await self.assess_dataset_quality(dataset_id)
            
            # Generate recommendations based on issues
            for dim, score in profile.dimensions.items():
                if score < 0.7:
                    recommendations.append({
                        "dimension": dim.value,
                        "current_score": score,
                        "target_score": 0.8,
                        "priority": "high" if score < 0.5 else "medium",
                        "actions": self._get_dimension_actions(dim, score)
                    })
            
            # Add specific recommendations based on issues
            for issue in profile.issues[:5]:  # Top 5 issues
                recommendations.append({
                    "issue": issue.get("type"),
                    "description": issue.get("message"),
                    "priority": issue.get("severity", "medium"),
                    "actions": issue.get("recommendations", [])
                })
            
            # Sort by priority
            priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
            recommendations.sort(
                key=lambda x: priority_order.get(x.get("priority", "low"), 3)
            )
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []
    
    def _get_dimension_actions(
        self,
        dimension: QualityDimension,
        score: float
    ) -> List[str]:
        """
        Get improvement actions for a quality dimension
        """
        actions = {
            QualityDimension.COMPLETENESS: [
                "Identify and fix null values",
                "Set up data validation at source",
                "Implement default values where appropriate"
            ],
            QualityDimension.ACCURACY: [
                "Cross-validate with authoritative sources",
                "Implement data verification processes",
                "Set up anomaly detection"
            ],
            QualityDimension.CONSISTENCY: [
                "Standardize data formats",
                "Implement referential integrity checks",
                "Create data transformation rules"
            ],
            QualityDimension.TIMELINESS: [
                "Reduce data pipeline latency",
                "Implement real-time data capture",
                "Set up freshness monitoring"
            ],
            QualityDimension.UNIQUENESS: [
                "Identify and merge duplicates",
                "Implement deduplication logic",
                "Add unique constraints"
            ],
            QualityDimension.VALIDITY: [
                "Define and enforce business rules",
                "Implement format validation",
                "Set up range checks"
            ]
        }
        
        return actions.get(dimension, ["Review data quality rules"])
    
    async def create_quality_dashboard_data(
        self,
        catalog_subset: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Create data for quality dashboard
        """
        dashboard_data = {
            "summary": {
                "total_datasets": 0,
                "average_quality": 0.0,
                "trust_distribution": defaultdict(int),
                "trend": "stable"
            },
            "by_layer": defaultdict(lambda: {"count": 0, "avg_quality": 0.0}),
            "by_dimension": defaultdict(list),
            "critical_issues": [],
            "top_performers": [],
            "bottom_performers": []
        }
        
        try:
            # Get datasets to analyze
            if catalog_subset:
                datasets = catalog_subset
            else:
                # Get all datasets from catalog
                search_result = await self.atlas_client.search_entities(
                    query="*",
                    type_name="dataset",
                    limit=1000
                )
                datasets = [e["guid"] for e in search_result.get("entities", [])]
            
            quality_scores = []
            
            # Assess each dataset
            for dataset_id in datasets:
                try:
                    profile = await self.assess_dataset_quality(dataset_id)
                    quality_scores.append(profile.overall_score)
                    
                    # Update summary
                    dashboard_data["summary"]["total_datasets"] += 1
                    dashboard_data["summary"]["trust_distribution"][profile.trust_level.value] += 1
                    
                    # Get dataset details
                    entity = await self.atlas_client.get_entity(dataset_id)
                    layer = entity.get("attributes", {}).get("layer", "unknown")
                    
                    # Update by layer
                    dashboard_data["by_layer"][layer]["count"] += 1
                    dashboard_data["by_layer"][layer]["avg_quality"] += profile.overall_score
                    
                    # Update by dimension
                    for dim, score in profile.dimensions.items():
                        dashboard_data["by_dimension"][dim.value].append(score)
                    
                    # Track top/bottom performers
                    dataset_info = {
                        "id": dataset_id,
                        "name": entity.get("attributes", {}).get("name"),
                        "score": profile.overall_score,
                        "trust_level": profile.trust_level.value
                    }
                    
                    if profile.overall_score >= 0.9:
                        dashboard_data["top_performers"].append(dataset_info)
                    elif profile.overall_score < 0.5:
                        dashboard_data["bottom_performers"].append(dataset_info)
                    
                    # Track critical issues
                    for issue in profile.issues:
                        if issue.get("severity") == "critical":
                            dashboard_data["critical_issues"].append({
                                "dataset": dataset_info["name"],
                                "issue": issue
                            })
                    
                except Exception as e:
                    logger.debug(f"Error assessing {dataset_id}: {e}")
            
            # Calculate averages
            if quality_scores:
                dashboard_data["summary"]["average_quality"] = sum(quality_scores) / len(quality_scores)
            
            for layer, data in dashboard_data["by_layer"].items():
                if data["count"] > 0:
                    data["avg_quality"] = data["avg_quality"] / data["count"]
            
            for dim, scores in dashboard_data["by_dimension"].items():
                if scores:
                    dashboard_data["by_dimension"][dim] = {
                        "average": sum(scores) / len(scores),
                        "min": min(scores),
                        "max": max(scores)
                    }
            
            # Sort performers
            dashboard_data["top_performers"].sort(key=lambda x: x["score"], reverse=True)
            dashboard_data["bottom_performers"].sort(key=lambda x: x["score"])
            
            # Keep only top 10
            dashboard_data["top_performers"] = dashboard_data["top_performers"][:10]
            dashboard_data["bottom_performers"] = dashboard_data["bottom_performers"][:10]
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Error creating dashboard data: {e}")
            return dashboard_data
    
    async def cleanup(self):
        """
        Cleanup resources
        """
        await self.http_client.aclose() 