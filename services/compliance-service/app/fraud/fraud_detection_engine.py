"""
Fraud Detection Engine

Core fraud detection logic using graph analysis and pattern matching.
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal
import uuid

from pyignite import Client as IgniteClient
from ..clients import GraphIntelligenceClient
from ..vault_consul_integration import VaultConsulIntegration

logger = logging.getLogger(__name__)


@dataclass
class FraudCheckResult:
    """Result of a fraud check"""
    job_id: str
    entity_id: str
    fraud_score: float
    is_suspicious: bool
    fraud_indicators: List[str]
    pattern_matches: List[Dict[str, Any]]
    recommendations: List[str]
    timestamp: datetime
    network_analysis: Optional[Dict[str, Any]] = None


class FraudDetectionEngine:
    """
    Fraud detection engine that leverages graph analysis for
    identifying suspicious patterns and relationships.
    """
    
    def __init__(
        self,
        ignite_client: IgniteClient,
        graph_client: GraphIntelligenceClient,
        vault_consul: VaultConsulIntegration,
        fraud_threshold: float = 0.7
    ):
        self.ignite_client = ignite_client
        self.graph_client = graph_client
        self.vault_consul = vault_consul
        self.fraud_threshold = fraud_threshold
        
        # Caches
        self._fraud_cache = None
        self._pattern_cache = None
        self._job_cache = None
        
        # Background tasks
        self._pattern_update_task = None
        
    async def initialize(self):
        """Initialize the fraud detection engine"""
        logger.info("Initializing fraud detection engine")
        
        # Initialize caches
        self._fraud_cache = await self.ignite_client.get_or_create_cache(
            "fraud_detection_results"
        )
        self._pattern_cache = await self.ignite_client.get_or_create_cache(
            "fraud_patterns"
        )
        self._job_cache = await self.ignite_client.get_or_create_cache(
            "fraud_detection_jobs"
        )
        
        # Load fraud patterns from Consul
        await self._load_fraud_patterns()
        
        # Start background tasks
        self._pattern_update_task = asyncio.create_task(
            self._update_patterns_periodically()
        )
        
        logger.info("Fraud detection engine initialized")
        
    async def shutdown(self):
        """Shutdown the fraud detection engine"""
        if self._pattern_update_task:
            self._pattern_update_task.cancel()
            
        await self.graph_client.close()
        
    async def check_entities_for_fraud(
        self,
        entity_ids: List[str],
        check_depth: int,
        include_network_analysis: bool,
        tenant_id: str
    ) -> str:
        """
        Check entities for fraud indicators using graph analysis
        
        Args:
            entity_ids: List of entity IDs to check
            check_depth: Graph traversal depth for analysis
            include_network_analysis: Include network-based fraud indicators
            tenant_id: Tenant ID
            
        Returns:
            Job ID for tracking the fraud check
        """
        job_id = str(uuid.uuid4())
        
        # Store job info
        await self._job_cache.put(job_id, {
            "status": "processing",
            "entity_ids": entity_ids,
            "check_depth": check_depth,
            "include_network_analysis": include_network_analysis,
            "tenant_id": tenant_id,
            "created_at": datetime.utcnow().isoformat()
        })
        
        # Submit graph analytics job
        try:
            graph_job_id = await self.graph_client.submit_graph_analytics_job(
                algorithm="fraud_detection",
                tenant_id=tenant_id,
                parameters={
                    "entity_ids": entity_ids,
                    "check_depth": check_depth,
                    "include_network_analysis": include_network_analysis,
                    "fraud_threshold": self.fraud_threshold
                }
            )
            
            # Update job with graph job ID
            job_info = await self._job_cache.get(job_id)
            job_info["graph_job_id"] = graph_job_id
            await self._job_cache.put(job_id, job_info)
            
            # Start monitoring job
            asyncio.create_task(self._monitor_job(job_id, tenant_id))
            
        except Exception as e:
            logger.error(f"Error submitting fraud detection job: {e}")
            job_info = await self._job_cache.get(job_id)
            job_info["status"] = "failed"
            job_info["error"] = str(e)
            await self._job_cache.put(job_id, job_info)
            
        return job_id
        
    async def get_immediate_fraud_indicators(
        self,
        entity_ids: List[str],
        tenant_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get immediate fraud indicators for entities (quick check)
        
        Args:
            entity_ids: Entity IDs to check (limited to 5)
            tenant_id: Tenant ID
            
        Returns:
            List of immediate fraud indicators
        """
        # Limit to 5 entities for quick check
        check_ids = entity_ids[:5]
        
        # Query current properties from graph
        results = await self.graph_client.query_entity_properties(
            entity_ids=check_ids,
            properties=["fraud_score", "is_suspicious", "trust_score"],
            tenant_id=tenant_id
        )
        
        # Apply quick pattern checks
        immediate_results = []
        for entity_data in results:
            entity_id = entity_data["entity_id"]
            
            # Check cached fraud results
            cached_result = await self._fraud_cache.get(f"{tenant_id}:{entity_id}")
            
            if cached_result and self._is_cache_valid(cached_result):
                immediate_results.append({
                    "entity_id": entity_id,
                    "fraud_score": cached_result["fraud_score"],
                    "is_suspicious": cached_result["is_suspicious"],
                    "trust_score": entity_data.get("trust_score", 0.5),
                    "from_cache": True
                })
            else:
                # Basic scoring based on current properties
                fraud_score = entity_data.get("fraud_score", 0.0)
                trust_score = entity_data.get("trust_score", 0.5)
                
                # Adjust fraud score based on trust
                if trust_score < 0.3:
                    fraud_score = min(1.0, fraud_score + 0.2)
                    
                immediate_results.append({
                    "entity_id": entity_id,
                    "fraud_score": fraud_score,
                    "is_suspicious": fraud_score >= self.fraud_threshold,
                    "trust_score": trust_score,
                    "from_cache": False
                })
                
        return immediate_results
        
    async def get_fraud_check_results(
        self,
        job_id: str,
        tenant_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get results of a fraud detection job
        
        Args:
            job_id: Job ID to retrieve results for
            tenant_id: Tenant ID
            
        Returns:
            Fraud check results or None if still processing
        """
        job_info = await self._job_cache.get(job_id)
        
        if not job_info:
            return None
            
        if job_info["status"] == "processing":
            # Check if graph job completed
            if "graph_job_id" in job_info:
                graph_results = await self.graph_client.get_job_results(
                    job_info["graph_job_id"],
                    tenant_id
                )
                
                if graph_results:
                    # Process and cache results
                    processed_results = await self._process_graph_results(
                        graph_results,
                        job_info,
                        tenant_id
                    )
                    
                    job_info["status"] = "completed"
                    job_info["results"] = processed_results
                    job_info["completed_at"] = datetime.utcnow().isoformat()
                    await self._job_cache.put(job_id, job_info)
                    
        return job_info
        
    async def _monitor_job(self, job_id: str, tenant_id: str):
        """Monitor a fraud detection job"""
        max_wait_time = 300  # 5 minutes
        check_interval = 5  # 5 seconds
        elapsed = 0
        
        while elapsed < max_wait_time:
            await asyncio.sleep(check_interval)
            elapsed += check_interval
            
            job_info = await self._job_cache.get(job_id)
            if job_info["status"] != "processing":
                break
                
            # Check graph job status
            if "graph_job_id" in job_info:
                results = await self.graph_client.get_job_results(
                    job_info["graph_job_id"],
                    tenant_id
                )
                
                if results:
                    # Process results
                    processed_results = await self._process_graph_results(
                        results,
                        job_info,
                        tenant_id
                    )
                    
                    job_info["status"] = "completed"
                    job_info["results"] = processed_results
                    job_info["completed_at"] = datetime.utcnow().isoformat()
                    await self._job_cache.put(job_id, job_info)
                    break
                    
        # Timeout handling
        if elapsed >= max_wait_time:
            job_info = await self._job_cache.get(job_id)
            if job_info["status"] == "processing":
                job_info["status"] = "timeout"
                job_info["error"] = "Job processing timeout"
                await self._job_cache.put(job_id, job_info)
                
    async def _process_graph_results(
        self,
        graph_results: Dict[str, Any],
        job_info: Dict[str, Any],
        tenant_id: str
    ) -> Dict[str, Any]:
        """Process results from graph analytics job"""
        processed = {
            "fraud_analysis": {},
            "suspicious_entities": [],
            "pattern_matches": [],
            "network_insights": {}
        }
        
        # Extract fraud analysis results
        if "pattern_analysis" in graph_results:
            for entity_id, analysis in graph_results["pattern_analysis"].items():
                fraud_score = analysis.get("fraud_score", 0.0)
                is_suspicious = fraud_score >= self.fraud_threshold
                
                processed["fraud_analysis"][entity_id] = {
                    "fraud_score": fraud_score,
                    "is_suspicious": is_suspicious,
                    "fraud_indicators": analysis.get("indicators", []),
                    "pattern_matches": analysis.get("patterns", [])
                }
                
                if is_suspicious:
                    processed["suspicious_entities"].append(entity_id)
                    
                # Cache the result
                await self._fraud_cache.put(
                    f"{tenant_id}:{entity_id}",
                    {
                        "fraud_score": fraud_score,
                        "is_suspicious": is_suspicious,
                        "indicators": analysis.get("indicators", []),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
                # Update entity properties in graph
                await self.graph_client.update_entity_fraud_properties(
                    entity_id=entity_id,
                    fraud_score=fraud_score,
                    is_suspicious=is_suspicious,
                    fraud_indicators=analysis.get("indicators", []),
                    tenant_id=tenant_id
                )
                
        # Extract pattern matches
        if "matched_patterns" in graph_results:
            processed["pattern_matches"] = graph_results["matched_patterns"]
            
        # Extract network insights
        if "network_analysis" in graph_results:
            processed["network_insights"] = graph_results["network_analysis"]
            
        return processed
        
    async def _load_fraud_patterns(self):
        """Load fraud patterns from Consul"""
        try:
            patterns = await self.vault_consul.get_config("fraud/patterns")
            if patterns:
                for pattern_id, pattern_data in patterns.items():
                    await self._pattern_cache.put(pattern_id, pattern_data)
            logger.info(f"Loaded {len(patterns)} fraud patterns")
        except Exception as e:
            logger.error(f"Error loading fraud patterns: {e}")
            
    async def _update_patterns_periodically(self):
        """Update fraud patterns periodically"""
        while True:
            try:
                await asyncio.sleep(3600)  # Update every hour
                await self._load_fraud_patterns()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating fraud patterns: {e}")
                
    def _is_cache_valid(self, cached_data: Dict[str, Any]) -> bool:
        """Check if cached fraud data is still valid"""
        if "timestamp" not in cached_data:
            return False
            
        cached_time = datetime.fromisoformat(cached_data["timestamp"])
        age = datetime.utcnow() - cached_time
        
        # Cache valid for 24 hours
        return age < timedelta(hours=24) 