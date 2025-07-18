"""
Quality Event Processor

Processes data quality assessment events and updates dataset metadata
with quality scores and trust-weighted metrics.
"""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging
import json
import asyncio

from platformq_shared import EventProcessor, event_handler, ProcessingResult, ProcessingStatus
from platformq_events import (
    DataQualityAssessment,
    DataAccessGrant,
    DataLineageEnriched,
    QualityAnomaly
)

from app.engines.trust_weighted_data_engine import TrustWeightedDataEngine
from app.repositories import DatasetRepository, QualityHistoryRepository
from app.integrations import IgniteCache, ElasticsearchClient

logger = logging.getLogger(__name__)


class QualityEventProcessor(EventProcessor):
    """
    Processes quality-related events for the trust-weighted data marketplace
    """
    
    def __init__(
        self,
        service_name: str,
        pulsar_url: str,
        trust_engine: TrustWeightedDataEngine,
        dataset_repo: DatasetRepository,
        quality_repo: QualityHistoryRepository,
        ignite_cache: IgniteCache,
        elasticsearch: ElasticsearchClient
    ):
        super().__init__(service_name, pulsar_url)
        self.trust_engine = trust_engine
        self.dataset_repo = dataset_repo
        self.quality_repo = quality_repo
        self.ignite = ignite_cache
        self.elasticsearch = elasticsearch
        
        # Cache for aggregated quality metrics
        self.quality_aggregator = {}
        
        # Start background aggregation task
        self._aggregation_task = None
        
    async def start(self):
        """Start the event processor"""
        await super().start()
        
        # Start background aggregation
        self._aggregation_task = asyncio.create_task(
            self._quality_aggregation_loop()
        )
        
    async def stop(self):
        """Stop the event processor"""
        if self._aggregation_task:
            self._aggregation_task.cancel()
            
        await super().stop()
        
    @event_handler("data-quality-assessments")
    async def handle_quality_assessment(
        self,
        event: DataQualityAssessment
    ) -> ProcessingResult:
        """
        Handle data quality assessment events
        """
        try:
            logger.info(f"Processing quality assessment for dataset {event.dataset_id}")
            
            # Store quality assessment
            await self.quality_repo.store_assessment({
                'assessment_id': event.assessment_id,
                'dataset_id': event.dataset_id,
                'asset_id': event.asset_id,
                'timestamp': datetime.fromtimestamp(event.timestamp / 1000),
                'assessor_id': event.assessor_id,
                'assessor_trust_score': event.assessor_trust_score,
                'quality_dimensions': event.quality_dimensions,
                'overall_quality_score': event.overall_quality_score,
                'trust_adjusted_score': event.trust_adjusted_score,
                'quality_issues': event.quality_issues,
                'automated_checks': event.automated_checks,
                'verification_proofs': event.verification_proofs
            })
            
            # Update dataset metadata
            await self.dataset_repo.update_quality_metadata(
                event.dataset_id,
                {
                    'latest_quality_score': event.trust_adjusted_score,
                    'latest_assessment_id': event.assessment_id,
                    'quality_dimensions': event.quality_dimensions,
                    'last_assessed': datetime.fromtimestamp(event.timestamp / 1000)
                }
            )
            
            # Update quality index in Elasticsearch
            await self._update_quality_index(event)
            
            # Cache latest quality for fast access
            cache_key = f"quality:latest:{event.dataset_id}"
            await self.ignite.put(
                cache_key,
                json.dumps({
                    'trust_adjusted_score': event.trust_adjusted_score,
                    'overall_quality_score': event.overall_quality_score,
                    'dimensions': event.quality_dimensions,
                    'timestamp': event.timestamp
                }),
                ttl=3600  # 1 hour cache
            )
            
            # Add to aggregation queue
            self._add_to_aggregation(event)
            
            # Check for quality-based actions
            await self._check_quality_triggers(event)
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                message=f"Quality assessment processed for dataset {event.dataset_id}"
            )
            
        except Exception as e:
            logger.error(f"Error processing quality assessment: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILURE,
                message=str(e)
            )
            
    @event_handler("data-access-grants")
    async def handle_access_grant(
        self,
        event: DataAccessGrant
    ) -> ProcessingResult:
        """
        Handle data access grant events
        """
        try:
            logger.info(f"Processing access grant {event.grant_id}")
            
            # Store access log
            await self.dataset_repo.log_access({
                'grant_id': event.grant_id,
                'request_id': event.request_id,
                'dataset_id': event.dataset_id,
                'grantee_id': event.grantee_id,
                'granted_access_level': event.granted_access_level,
                'timestamp': datetime.fromtimestamp(event.timestamp / 1000),
                'expires_at': datetime.fromtimestamp(event.expires_at / 1000),
                'price_paid': event.price_paid,
                'quality_tier': event.quality_tier,
                'trust_requirements': event.trust_requirements
            })
            
            # Update dataset access metrics
            await self._update_access_metrics(event)
            
            # Send access credentials if needed
            if event.encryption_key_id:
                await self._provision_access_credentials(event)
                
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                message=f"Access grant processed for dataset {event.dataset_id}"
            )
            
        except Exception as e:
            logger.error(f"Error processing access grant: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILURE,
                message=str(e)
            )
            
    @event_handler("data-lineage-enriched")
    async def handle_lineage_enriched(
        self,
        event: DataLineageEnriched
    ) -> ProcessingResult:
        """
        Handle enriched data lineage events with trust information
        """
        try:
            logger.info(f"Processing lineage for dataset {event.dataset_id}")
            
            # Update dataset lineage with trust scores
            await self.dataset_repo.update_lineage({
                'dataset_id': event.dataset_id,
                'lineage_id': event.lineage_id,
                'trust_chain': event.trust_chain,
                'aggregated_trust_score': event.aggregated_trust_score,
                'weakest_link_score': event.weakest_link_score,
                'provenance_verified': event.provenance_verified,
                'verification_proofs': event.verification_proofs
            })
            
            # Recalculate dataset trust based on lineage
            await self._recalculate_dataset_trust(event)
            
            # Update graph with lineage relationships
            await self._update_lineage_graph(event)
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                message=f"Lineage processed for dataset {event.dataset_id}"
            )
            
        except Exception as e:
            logger.error(f"Error processing lineage: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILURE,
                message=str(e)
            )
            
    @event_handler("data-quality-anomalies")
    async def handle_quality_anomaly(
        self,
        event: QualityAnomaly
    ) -> ProcessingResult:
        """
        Handle data quality anomaly events
        """
        try:
            logger.info(f"Processing quality anomaly for dataset {event.dataset_id}")
            
            # Store anomaly
            await self.quality_repo.store_anomaly({
                'dataset_id': event.dataset_id,
                'timestamp': datetime.fromtimestamp(event.timestamp / 1000),
                'expected_quality': event.expected_quality,
                'actual_quality': event.actual_quality,
                'severity': event.severity,
                'anomaly_type': event.anomaly_type
            })
            
            # Trigger alerts based on severity
            if event.severity in ['HIGH', 'CRITICAL']:
                await self._trigger_quality_alerts(event)
                
            # Update dataset status if needed
            if event.severity == 'CRITICAL' and event.anomaly_type == 'QUALITY_DEGRADATION':
                await self.dataset_repo.update_status(
                    event.dataset_id,
                    'quality_warning',
                    f"Critical quality degradation detected: {event.actual_quality:.2f}"
                )
                
            # Check if trading should be halted
            if event.actual_quality < 0.3:  # Quality below 30%
                await self._halt_dataset_trading(event.dataset_id)
                
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                message=f"Quality anomaly processed for dataset {event.dataset_id}"
            )
            
        except Exception as e:
            logger.error(f"Error processing quality anomaly: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILURE,
                message=str(e)
            )
            
    async def _update_quality_index(self, event: DataQualityAssessment):
        """Update Elasticsearch quality index"""
        doc = {
            'dataset_id': event.dataset_id,
            'assessment_id': event.assessment_id,
            'timestamp': datetime.fromtimestamp(event.timestamp / 1000),
            'overall_quality': event.overall_quality_score,
            'trust_adjusted_quality': event.trust_adjusted_score,
            'completeness': event.quality_dimensions.get('completeness', 0),
            'accuracy': event.quality_dimensions.get('accuracy', 0),
            'consistency': event.quality_dimensions.get('consistency', 0),
            'timeliness': event.quality_dimensions.get('timeliness', 0),
            'validity': event.quality_dimensions.get('validity', 0),
            'uniqueness': event.quality_dimensions.get('uniqueness', 0),
            'assessor_trust': event.assessor_trust_score,
            'has_issues': len(event.quality_issues) > 0,
            'issue_count': len(event.quality_issues)
        }
        
        await self.elasticsearch.index(
            index='dataset-quality',
            id=event.assessment_id,
            body=doc
        )
        
    def _add_to_aggregation(self, event: DataQualityAssessment):
        """Add assessment to aggregation queue"""
        dataset_id = event.dataset_id
        
        if dataset_id not in self.quality_aggregator:
            self.quality_aggregator[dataset_id] = {
                'assessments': [],
                'last_update': datetime.utcnow()
            }
            
        self.quality_aggregator[dataset_id]['assessments'].append({
            'timestamp': event.timestamp,
            'trust_adjusted_score': event.trust_adjusted_score,
            'dimensions': event.quality_dimensions
        })
        
        # Keep only last 100 assessments
        if len(self.quality_aggregator[dataset_id]['assessments']) > 100:
            self.quality_aggregator[dataset_id]['assessments'].pop(0)
            
    async def _quality_aggregation_loop(self):
        """Background task to aggregate quality metrics"""
        while True:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                
                for dataset_id, data in list(self.quality_aggregator.items()):
                    if not data['assessments']:
                        continue
                        
                    # Calculate aggregated metrics
                    assessments = data['assessments']
                    avg_quality = sum(a['trust_adjusted_score'] for a in assessments) / len(assessments)
                    
                    # Calculate dimension averages
                    dimension_avgs = {}
                    for dim in ['completeness', 'accuracy', 'consistency', 'timeliness', 'validity', 'uniqueness']:
                        values = [a['dimensions'].get(dim, 0) for a in assessments]
                        dimension_avgs[dim] = sum(values) / len(values) if values else 0
                        
                    # Store aggregated metrics
                    await self.ignite.put(
                        f"quality:aggregated:{dataset_id}",
                        json.dumps({
                            'average_quality': avg_quality,
                            'dimension_averages': dimension_avgs,
                            'assessment_count': len(assessments),
                            'last_updated': datetime.utcnow().isoformat()
                        }),
                        ttl=86400  # 24 hour cache
                    )
                    
                    # Clear old data
                    if datetime.utcnow() - data['last_update'] > timedelta(hours=1):
                        del self.quality_aggregator[dataset_id]
                        
            except Exception as e:
                logger.error(f"Error in quality aggregation loop: {e}")
                await asyncio.sleep(60)  # Wait before retrying
                
    async def _check_quality_triggers(self, event: DataQualityAssessment):
        """Check for quality-based automated actions"""
        # Check if quality improved significantly
        prev_quality = await self._get_previous_quality(event.dataset_id)
        
        if prev_quality and event.trust_adjusted_score > prev_quality * 1.2:
            # Quality improved by 20%
            await self._publish_quality_improvement(event)
            
        # Check if quality meets premium tier
        if event.trust_adjusted_score >= 0.9:
            await self.dataset_repo.update_metadata(
                event.dataset_id,
                {'premium_quality': True}
            )
            
    async def _update_access_metrics(self, event: DataAccessGrant):
        """Update dataset access metrics"""
        metrics_key = f"access:metrics:{event.dataset_id}"
        
        # Get current metrics
        current = await self.ignite.get(metrics_key)
        if current:
            metrics = json.loads(current)
        else:
            metrics = {
                'total_accesses': 0,
                'unique_users': set(),
                'revenue': '0',
                'access_levels': {}
            }
            
        # Update metrics
        metrics['total_accesses'] += 1
        metrics['unique_users'].add(event.grantee_id)
        
        if event.price_paid:
            from decimal import Decimal
            metrics['revenue'] = str(
                Decimal(metrics['revenue']) + Decimal(event.price_paid)
            )
            
        level = event.granted_access_level
        metrics['access_levels'][level] = metrics['access_levels'].get(level, 0) + 1
        
        # Convert set to list for JSON serialization
        metrics['unique_users'] = list(metrics['unique_users'])
        
        # Store updated metrics
        await self.ignite.put(metrics_key, json.dumps(metrics), ttl=86400)
        
    async def _recalculate_dataset_trust(self, event: DataLineageEnriched):
        """Recalculate dataset trust score based on lineage"""
        # Use weakest link principle with some averaging
        trust_score = (
            event.aggregated_trust_score * 0.7 +
            event.weakest_link_score * 0.3
        )
        
        await self.dataset_repo.update_metadata(
            event.dataset_id,
            {
                'lineage_trust_score': trust_score,
                'provenance_verified': event.provenance_verified
            }
        )
        
    async def _trigger_quality_alerts(self, event: QualityAnomaly):
        """Trigger alerts for quality anomalies"""
        # Get alert subscriptions
        subscriptions = await self._get_alert_subscriptions(event.dataset_id)
        
        for sub in subscriptions:
            if event.actual_quality < sub.get('threshold', 0.7):
                await self._send_quality_alert(sub, event)
                
    async def _halt_dataset_trading(self, dataset_id: str):
        """Halt trading for critically low quality datasets"""
        await self.dataset_repo.update_status(
            dataset_id,
            'trading_halted',
            'Trading halted due to critical quality issues'
        )
        
        # Publish trading halt event
        await self.event_publisher.publish(
            'dataset-trading-halted',
            {
                'dataset_id': dataset_id,
                'reason': 'critical_quality',
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
    async def _get_previous_quality(self, dataset_id: str) -> Optional[float]:
        """Get previous quality score from cache"""
        cache_key = f"quality:previous:{dataset_id}"
        cached = await self.ignite.get(cache_key)
        
        if cached:
            data = json.loads(cached)
            return data.get('trust_adjusted_score')
            
        return None
        
    async def _publish_quality_improvement(self, event: DataQualityAssessment):
        """Publish quality improvement event"""
        await self.event_publisher.publish(
            'dataset-quality-improved',
            {
                'dataset_id': event.dataset_id,
                'new_quality': event.trust_adjusted_score,
                'improvement_percentage': 20,
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
    async def _provision_access_credentials(self, event: DataAccessGrant):
        """Provision access credentials for granted access"""
        # Implementation depends on your access control system
        pass
        
    async def _update_lineage_graph(self, event: DataLineageEnriched):
        """Update graph database with lineage relationships"""
        # Implementation depends on graph database integration
        pass
        
    async def _get_alert_subscriptions(self, dataset_id: str) -> list:
        """Get alert subscriptions for a dataset"""
        # Implementation depends on subscription storage
        return []
        
    async def _send_quality_alert(self, subscription: Dict, event: QualityAnomaly):
        """Send quality alert to subscriber"""
        # Implementation depends on notification system
        pass 