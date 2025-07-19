"""
CEP-Druid Analytics Integration

Integrates Flink Complex Event Processing alerts with Apache Druid analytics
to provide comprehensive real-time monitoring and historical analysis.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
import json
from collections import defaultdict
import pandas as pd
import numpy as np

from pydruid.client import PyDruid
from pydruid.utils.aggregators import longsum, doublesum, count, thetaSketch
from pydruid.utils.filters import Dimension, Filter, Bound
import pulsar
import httpx

logger = logging.getLogger(__name__)


class CEPDruidIntegration:
    """
    Integrates CEP alerts with Druid analytics for comprehensive monitoring
    """
    
    def __init__(self, 
                 druid_broker_url: str = "http://druid-broker:8082",
                 pulsar_url: str = "pulsar://pulsar:6650",
                 flink_job_manager_url: str = "http://flink-jobmanager:8081"):
        self.druid_client = PyDruid(druid_broker_url, 'druid/v2')
        self.pulsar_client = pulsar.Client(pulsar_url)
        self.flink_url = flink_job_manager_url
        self.http_client = httpx.AsyncClient()
        
        # Druid datasources for CEP data
        self.datasources = {
            'cep_alerts': 'cep_alerts',
            'cep_metrics': 'cep_metrics',
            'entity_profiles': 'entity_profiles',
            'pattern_statistics': 'pattern_statistics'
        }
        
        # Alert consumers
        self.alert_consumer = None
        self.metrics_consumer = None
        
    async def initialize(self):
        """Initialize connections and consumers"""
        try:
            # Create Pulsar consumers for CEP output
            self.alert_consumer = self.pulsar_client.subscribe(
                'cep-alerts',
                'druid-analytics-consumer',
                consumer_type=pulsar.ConsumerType.Shared
            )
            
            self.metrics_consumer = self.pulsar_client.subscribe(
                'cep-metrics',
                'druid-metrics-consumer',
                consumer_type=pulsar.ConsumerType.Shared
            )
            
            # Ensure Druid datasources exist
            await self._ensure_datasources()
            
            logger.info("CEP-Druid integration initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize CEP-Druid integration: {e}")
            raise
    
    async def start_alert_ingestion(self):
        """Start ingesting CEP alerts into Druid"""
        while True:
            try:
                msg = self.alert_consumer.receive(timeout_millis=1000)
                alert = json.loads(msg.data())
                
                # Transform alert for Druid ingestion
                druid_event = self._transform_alert_for_druid(alert)
                
                # Ingest into Druid
                await self._ingest_to_druid('cep_alerts', druid_event)
                
                # Update pattern statistics
                await self._update_pattern_statistics(alert)
                
                # Acknowledge message
                self.alert_consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error processing alert: {e}")
                await asyncio.sleep(0.1)
    
    async def start_metrics_ingestion(self):
        """Start ingesting CEP metrics into Druid"""
        while True:
            try:
                msg = self.metrics_consumer.receive(timeout_millis=1000)
                metrics = json.loads(msg.data())
                
                # Transform metrics for Druid
                druid_events = self._transform_metrics_for_druid(metrics)
                
                # Bulk ingest
                for event in druid_events:
                    await self._ingest_to_druid('cep_metrics', event)
                
                self.metrics_consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error processing metrics: {e}")
                await asyncio.sleep(0.1)
    
    def _transform_alert_for_druid(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        """Transform CEP alert for Druid ingestion"""
        return {
            'timestamp': alert.get('timestamp', datetime.utcnow().isoformat()),
            'alert_id': alert['alert_id'],
            'pattern_name': alert['pattern_name'],
            'severity': alert['severity'],
            'entity_id': alert.get('entity_ids', ['unknown'])[0],
            'entity_type': alert.get('entity_profile', {}).get('entity_type', 'unknown'),
            'risk_score': alert.get('final_risk_score', alert.get('risk_score', 0)),
            'event_count': alert.get('event_count', 0),
            'pattern_duration_seconds': alert.get('pattern_duration_seconds', 0),
            'trust_score': alert.get('entity_profile', {}).get('trust_score', 0.5),
            'account_age_days': alert.get('entity_profile', {}).get('account_age_days', 0),
            'historical_alert_count': alert.get('historical_alert_count', 0),
            'alert_trend': alert.get('historical_context', {}).get('alert_trend', 'STABLE'),
            'is_repeat_pattern': alert.get('historical_context', {}).get('repeat_pattern', False),
            'recommended_actions': ','.join(alert.get('recommended_actions', [])),
            
            # Dimensional data for OLAP
            'hour_of_day': datetime.fromisoformat(alert['timestamp']).hour,
            'day_of_week': datetime.fromisoformat(alert['timestamp']).weekday(),
            'alert_category': self._categorize_alert(alert['pattern_name'])
        }
    
    def _transform_metrics_for_druid(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform CEP metrics for Druid ingestion"""
        events = []
        timestamp = metrics['timestamp']
        
        # Alert count metrics
        for severity, count in metrics.get('alert_counts', {}).items():
            events.append({
                'timestamp': timestamp,
                'metric_type': 'alert_count',
                'severity': severity,
                'value': count,
                'window_minutes': 1
            })
        
        # Pattern distribution metrics
        for pattern, count in metrics.get('pattern_distribution', {}).items():
            events.append({
                'timestamp': timestamp,
                'metric_type': 'pattern_frequency',
                'pattern_name': pattern,
                'value': count,
                'window_minutes': 1
            })
        
        # Top entities metrics
        for i, (entity_id, count) in enumerate(metrics.get('top_entities', [])):
            events.append({
                'timestamp': timestamp,
                'metric_type': 'entity_alert_count',
                'entity_id': entity_id,
                'value': count,
                'rank': i + 1,
                'window_minutes': 1
            })
        
        return events
    
    def _categorize_alert(self, pattern_name: str) -> str:
        """Categorize alert based on pattern name"""
        categories = {
            'fraud': ['velocity_check', 'account_takeover', 'money_laundering', 'pump_and_dump', 'sybil_attack'],
            'security': ['brute_force', 'privilege_escalation', 'data_exfiltration'],
            'anomaly': ['cascading_failure', 'resource_exhaustion', 'latency_spike'],
            'business': ['compliance_violation', 'trading_manipulation', 'duplicate_submission']
        }
        
        for category, patterns in categories.items():
            if pattern_name in patterns:
                return category
        
        return 'other'
    
    async def _update_pattern_statistics(self, alert: Dict[str, Any]):
        """Update pattern statistics in Druid"""
        pattern_stat = {
            'timestamp': datetime.utcnow().isoformat(),
            'pattern_name': alert['pattern_name'],
            'detection_count': 1,
            'avg_risk_score': alert.get('risk_score', 0),
            'avg_event_count': alert.get('event_count', 0),
            'severity_distribution': {
                alert['severity']: 1
            }
        }
        
        await self._ingest_to_druid('pattern_statistics', pattern_stat)
    
    async def _ingest_to_druid(self, datasource: str, event: Dict[str, Any]):
        """Ingest event into Druid"""
        # In production, this would use Kafka/Kinesis for real-time ingestion
        # For now, using the indexing API
        try:
            task_spec = {
                "type": "index_parallel",
                "spec": {
                    "dataSchema": {
                        "dataSource": datasource,
                        "timestampSpec": {
                            "column": "timestamp",
                            "format": "iso"
                        },
                        "dimensionsSpec": {
                            "dimensions": [k for k in event.keys() if k not in ['timestamp', 'value', 'risk_score', 'event_count']]
                        },
                        "metricsSpec": [
                            {"type": "doubleSum", "name": name, "fieldName": name}
                            for name in ['value', 'risk_score', 'event_count']
                            if name in event
                        ]
                    },
                    "ioConfig": {
                        "type": "index_parallel",
                        "inputSource": {
                            "type": "inline",
                            "data": json.dumps(event)
                        },
                        "inputFormat": {
                            "type": "json"
                        }
                    }
                }
            }
            
            # Submit ingestion task
            response = await self.http_client.post(
                f"{self.druid_client.url}/druid/indexer/v1/task",
                json=task_spec
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to ingest to Druid: {response.text}")
                
        except Exception as e:
            logger.error(f"Error ingesting to Druid: {e}")
    
    async def query_alert_analytics(self,
                                  time_range: Tuple[datetime, datetime],
                                  dimensions: List[str] = None,
                                  filters: Dict[str, Any] = None) -> pd.DataFrame:
        """Query alert analytics from Druid"""
        try:
            # Default dimensions
            if dimensions is None:
                dimensions = ['pattern_name', 'severity', 'alert_category']
            
            # Build query
            query = self.druid_client.groupby(
                datasource=self.datasources['cep_alerts'],
                granularity='hour',
                intervals=[f"{time_range[0].isoformat()}/{time_range[1].isoformat()}"],
                dimensions=dimensions,
                aggregations=[
                    count('alert_count'),
                    doublesum('risk_score_sum'),
                    {"type": "doubleAvg", "name": "avg_risk_score", "fieldName": "risk_score"},
                    {"type": "doubleAvg", "name": "avg_event_count", "fieldName": "event_count"},
                    thetaSketch('unique_entities', 'entity_id')
                ]
            )
            
            # Add filters
            if filters:
                filter_list = []
                for key, value in filters.items():
                    if isinstance(value, list):
                        filter_list.append(Dimension(key) in value)
                    else:
                        filter_list.append(Dimension(key) == value)
                
                if len(filter_list) > 1:
                    query.filter = Filter(type="and", fields=filter_list)
                else:
                    query.filter = filter_list[0]
            
            # Execute query
            result = query.export_pandas()
            return result
            
        except Exception as e:
            logger.error(f"Error querying alert analytics: {e}")
            return pd.DataFrame()
    
    async def get_pattern_performance_metrics(self, 
                                            pattern_name: Optional[str] = None,
                                            time_range: str = "24h") -> Dict[str, Any]:
        """Get performance metrics for CEP patterns"""
        try:
            end_time = datetime.utcnow()
            start_time = self._parse_time_range(time_range, end_time)
            
            # Query pattern statistics
            query = self.druid_client.timeseries(
                datasource=self.datasources['pattern_statistics'],
                granularity='hour',
                intervals=[f"{start_time.isoformat()}/{end_time.isoformat()}"],
                aggregations=[
                    count('detection_count'),
                    {"type": "doubleAvg", "name": "avg_risk_score", "fieldName": "avg_risk_score"},
                    {"type": "doubleAvg", "name": "avg_event_count", "fieldName": "avg_event_count"}
                ]
            )
            
            if pattern_name:
                query.filter = Dimension('pattern_name') == pattern_name
            
            result = query.export_pandas()
            
            # Calculate metrics
            metrics = {
                'total_detections': int(result['detection_count'].sum()),
                'avg_risk_score': float(result['avg_risk_score'].mean()),
                'avg_event_count': float(result['avg_event_count'].mean()),
                'detection_trend': self._calculate_trend(result['detection_count'].values),
                'peak_hour': result.loc[result['detection_count'].idxmax()]['timestamp'].hour if not result.empty else None
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting pattern performance metrics: {e}")
            return {}
    
    async def get_entity_risk_profile(self, entity_id: str) -> Dict[str, Any]:
        """Get comprehensive risk profile for an entity from CEP and Druid data"""
        try:
            # Query last 30 days of alerts for this entity
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=30)
            
            query = self.druid_client.timeseries(
                datasource=self.datasources['cep_alerts'],
                granularity='day',
                intervals=[f"{start_time.isoformat()}/{end_time.isoformat()}"],
                filter=Dimension('entity_id') == entity_id,
                aggregations=[
                    count('alert_count'),
                    {"type": "doubleAvg", "name": "avg_risk_score", "fieldName": "risk_score"},
                    {"type": "cardinality", "name": "unique_patterns", "fieldNames": ["pattern_name"]},
                    {"type": "doubleMax", "name": "max_risk_score", "fieldName": "risk_score"}
                ]
            )
            
            result = query.export_pandas()
            
            if result.empty:
                return {
                    'entity_id': entity_id,
                    'risk_level': 'LOW',
                    'alert_history': []
                }
            
            # Calculate risk profile
            total_alerts = int(result['alert_count'].sum())
            avg_risk = float(result['avg_risk_score'].mean())
            max_risk = float(result['max_risk_score'].max())
            unique_patterns = int(result['unique_patterns'].max())
            
            # Determine risk level
            if max_risk > 0.8 or total_alerts > 50:
                risk_level = 'CRITICAL'
            elif avg_risk > 0.6 or total_alerts > 20:
                risk_level = 'HIGH'
            elif avg_risk > 0.4 or total_alerts > 10:
                risk_level = 'MEDIUM'
            else:
                risk_level = 'LOW'
            
            # Get recent alert patterns
            pattern_query = self.druid_client.topn(
                datasource=self.datasources['cep_alerts'],
                dimension='pattern_name',
                threshold=5,
                metric='alert_count',
                intervals=[f"{start_time.isoformat()}/{end_time.isoformat()}"],
                filter=Dimension('entity_id') == entity_id,
                aggregations=[count('alert_count')]
            )
            
            pattern_result = pattern_query.export_pandas()
            top_patterns = [
                {
                    'pattern': row['pattern_name'],
                    'count': int(row['alert_count'])
                }
                for _, row in pattern_result.iterrows()
            ]
            
            return {
                'entity_id': entity_id,
                'risk_level': risk_level,
                'total_alerts_30d': total_alerts,
                'avg_risk_score': round(avg_risk, 2),
                'max_risk_score': round(max_risk, 2),
                'unique_pattern_types': unique_patterns,
                'top_patterns': top_patterns,
                'alert_trend': self._calculate_trend(result['alert_count'].values),
                'last_alert_date': result.index[-1].isoformat() if not result.empty else None
            }
            
        except Exception as e:
            logger.error(f"Error getting entity risk profile: {e}")
            return {}
    
    async def get_real_time_alert_stats(self) -> Dict[str, Any]:
        """Get real-time statistics from CEP system"""
        try:
            # Query Flink job metrics
            response = await self.http_client.get(
                f"{self.flink_url}/jobs"
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get Flink job status: {response.status_code}")
                return {}
            
            jobs = response.json()['jobs']
            cep_job = next((job for job in jobs if 'cep' in job['name'].lower()), None)
            
            if not cep_job:
                return {'status': 'CEP job not found'}
            
            # Get job metrics
            metrics_response = await self.http_client.get(
                f"{self.flink_url}/jobs/{cep_job['id']}/metrics"
            )
            
            metrics = metrics_response.json() if metrics_response.status_code == 200 else []
            
            # Get last 5 minutes of alerts from Druid
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=5)
            
            recent_alerts = await self.query_alert_analytics(
                time_range=(start_time, end_time),
                dimensions=['severity', 'pattern_name']
            )
            
            # Aggregate stats
            stats = {
                'job_status': cep_job['state'],
                'job_uptime_seconds': cep_job.get('duration', 0) / 1000,
                'alerts_last_5min': len(recent_alerts),
                'alerts_by_severity': recent_alerts.groupby('severity')['alert_count'].sum().to_dict() if not recent_alerts.empty else {},
                'top_patterns_5min': recent_alerts.groupby('pattern_name')['alert_count'].sum().nlargest(5).to_dict() if not recent_alerts.empty else {},
                'processing_rate': self._extract_metric(metrics, 'numRecordsInPerSecond'),
                'checkpoint_duration': self._extract_metric(metrics, 'lastCheckpointDuration')
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting real-time alert stats: {e}")
            return {}
    
    def _parse_time_range(self, time_range: str, end_time: datetime) -> datetime:
        """Parse time range string to datetime"""
        units = {
            's': timedelta(seconds=1),
            'm': timedelta(minutes=1),
            'h': timedelta(hours=1),
            'd': timedelta(days=1),
            'w': timedelta(weeks=1)
        }
        
        try:
            value = int(time_range[:-1])
            unit = time_range[-1]
            return end_time - (units.get(unit, timedelta(hours=1)) * value)
        except:
            return end_time - timedelta(hours=1)
    
    def _calculate_trend(self, values: np.ndarray) -> str:
        """Calculate trend from time series values"""
        if len(values) < 2:
            return 'STABLE'
        
        # Simple linear regression
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        if slope > 0.1:
            return 'INCREASING'
        elif slope < -0.1:
            return 'DECREASING'
        else:
            return 'STABLE'
    
    def _extract_metric(self, metrics: List[Dict], metric_name: str) -> Optional[float]:
        """Extract metric value from Flink metrics"""
        metric = next((m for m in metrics if m['id'] == metric_name), None)
        return float(metric['value']) if metric else None
    
    async def _ensure_datasources(self):
        """Ensure Druid datasources exist"""
        # In production, this would create the ingestion specs
        # For now, just log
        logger.info(f"Ensuring Druid datasources: {list(self.datasources.values())}")
    
    async def close(self):
        """Close connections"""
        if self.alert_consumer:
            self.alert_consumer.close()
        if self.metrics_consumer:
            self.metrics_consumer.close()
        if self.pulsar_client:
            self.pulsar_client.close()
        await self.http_client.aclose() 