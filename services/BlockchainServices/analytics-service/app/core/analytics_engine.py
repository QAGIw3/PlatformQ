from typing import Dict, List, Optional, Any, Type
import asyncio
import logging
from datetime import datetime, timedelta
from collections import defaultdict
import json

import pandas as pd
import numpy as np
from pyignite import AsyncClient as IgniteClient
import redis.asyncio as redis
import motor.motor_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import httpx

from ..config import config
from ..models.analytics_models import (
    TimeSeries, ChainMetrics, AnalyticsQuery,
    AnalyticsReport, Alert, TimeInterval,
    MetricType, WalletAnalytics, TokenAnalytics
)
from ..analyzers.base_analyzer import BaseAnalyzer
from ..analyzers.transaction_analyzer import TransactionAnalyzer


class AnalyticsEngine:
    """Core analytics engine for blockchain data processing"""
    
    def __init__(
        self,
        ignite_client: IgniteClient,
        redis_client: redis.Redis,
        mongodb_client: motor.motor_asyncio.AsyncIOMotorClient
    ):
        self.ignite_client = ignite_client
        self.redis_client = redis_client
        self.mongodb_client = mongodb_client
        self.mongodb_db = mongodb_client[config.mongodb_database]
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize analyzers
        self.analyzers: Dict[str, BaseAnalyzer] = {
            'transaction': TransactionAnalyzer()
        }
        
        # HTTP clients for data sources
        self.blockchain_client = httpx.AsyncClient(
            base_url=config.blockchain_connector_url,
            timeout=30.0
        )
        self.event_client = httpx.AsyncClient(
            base_url=config.event_monitoring_url,
            timeout=30.0
        )
        
        # Database engine for time series data
        self.timeseries_engine = create_async_engine(config.timescale_url)
        self.timeseries_session = sessionmaker(
            self.timeseries_engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        # Cache for frequently accessed data
        self.cache: Dict[str, Any] = {}
        self.cache_ttl = config.cache_ttl_seconds
        
        # Active alerts
        self.active_alerts: Dict[str, Alert] = {}
        
        # Background tasks
        self.tasks: List[asyncio.Task] = []
        self._running = False
        
    async def initialize(self) -> None:
        """Initialize analytics engine"""
        self.logger.info("Initializing analytics engine")
        
        # Create caches
        await self._create_caches()
        
        # Load active alerts
        await self._load_alerts()
        
        self._running = True
        
        # Start background tasks
        self.tasks.append(asyncio.create_task(self._process_analytics_queue()))
        self.tasks.append(asyncio.create_task(self._monitor_alerts()))
        self.tasks.append(asyncio.create_task(self._update_cached_metrics()))
        self.tasks.append(asyncio.create_task(self._cleanup_old_data()))
        
    async def shutdown(self) -> None:
        """Shutdown analytics engine"""
        self.logger.info("Shutting down analytics engine")
        self._running = False
        
        # Cancel background tasks
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close HTTP clients
        await self.blockchain_client.aclose()
        await self.event_client.aclose()
        
        # Close analyzers
        for analyzer in self.analyzers.values():
            if hasattr(analyzer, 'close'):
                await analyzer.close()
        
        # Close database connections
        await self.timeseries_engine.dispose()
    
    async def _create_caches(self) -> None:
        """Create Ignite caches"""
        self.metrics_cache = await self.ignite_client.get_or_create_cache({
            'name': 'analytics_metrics',
            'key_type': 'str',
            'value_type': 'str'
        })
        
        self.reports_cache = await self.ignite_client.get_or_create_cache({
            'name': 'analytics_reports',
            'key_type': 'str',
            'value_type': 'str'
        })
        
        self.alerts_cache = await self.ignite_client.get_or_create_cache({
            'name': 'analytics_alerts',
            'key_type': 'str',
            'value_type': 'str'
        })
    
    async def query_analytics(
        self,
        query: AnalyticsQuery
    ) -> Dict[str, Any]:
        """Execute analytics query"""
        self.logger.info(f"Executing analytics query: {query.metric_type}")
        
        # Check cache first
        cache_key = self._generate_cache_key(query)
        cached_result = await self._get_cached_result(cache_key)
        if cached_result:
            self.logger.debug("Returning cached result")
            return cached_result
        
        # Determine which analyzer to use
        analyzer = self._get_analyzer_for_metric(query.metric_type)
        if not analyzer:
            raise ValueError(f"No analyzer found for metric type: {query.metric_type}")
        
        results = {
            'query': query.dict(),
            'data': {},
            'insights': [],
            'metadata': {
                'executed_at': datetime.utcnow().isoformat(),
                'data_sources': []
            }
        }
        
        # Process each chain
        for chain in query.chains:
            # Collect data
            data = await analyzer.collect_data(
                chain=chain,
                start_time=query.start_time,
                end_time=query.end_time,
                addresses=query.addresses,
                tokens=query.tokens,
                protocols=query.protocols
            )
            
            # Calculate metrics
            metrics_to_calculate = self._get_metrics_for_type(query.metric_type)
            metrics = await analyzer.calculate_metrics(
                data=data,
                metrics=metrics_to_calculate,
                interval=query.interval or TimeInterval.ONE_HOUR
            )
            
            # Generate insights
            insights = await analyzer.generate_insights(metrics)
            
            results['data'][chain] = {
                'metrics': {k: v.dict() for k, v in metrics.items()},
                'summary': self._generate_summary(metrics)
            }
            results['insights'].extend(insights)
        
        # Cache result
        await self._cache_result(cache_key, results)
        
        return results
    
    async def get_chain_metrics(
        self,
        chain: str,
        timestamp: Optional[datetime] = None
    ) -> ChainMetrics:
        """Get current metrics for a blockchain"""
        if not timestamp:
            timestamp = datetime.utcnow()
        
        # Try cache first
        cache_key = f"chain_metrics:{chain}:{timestamp.strftime('%Y%m%d%H')}"
        cached_data = await self.redis_client.get(cache_key)
        if cached_data:
            return ChainMetrics(**json.loads(cached_data))
        
        # Collect fresh data
        analyzer = self.analyzers.get('transaction')
        if not analyzer:
            raise ValueError("Transaction analyzer not available")
        
        # Get last hour of data
        start_time = timestamp - timedelta(hours=1)
        data = await analyzer.collect_data(chain, start_time, timestamp)
        
        if data.empty:
            return ChainMetrics(chain=chain, timestamp=timestamp)
        
        # Calculate metrics
        metrics = ChainMetrics(
            chain=chain,
            timestamp=timestamp,
            transaction_count=int(data['transaction_count'].sum()),
            transaction_volume=str(data['transaction_volume'].astype(float).sum()),
            gas_used=str(data['gas_used'].astype(float).sum()),
            average_gas_price=str(data['average_gas_price'].astype(float).mean()),
            active_addresses=int(data['unique_addresses'].sum()),
            tps=float(data['transaction_count'].sum() / 3600)  # Transactions per second
        )
        
        # Cache for 5 minutes
        await self.redis_client.setex(
            cache_key,
            300,
            metrics.json()
        )
        
        return metrics
    
    async def get_wallet_analytics(
        self,
        address: str,
        chain: str
    ) -> WalletAnalytics:
        """Get analytics for a specific wallet"""
        # Check cache
        cache_key = f"wallet:{chain}:{address}"
        cached_data = await self.redis_client.get(cache_key)
        if cached_data:
            return WalletAnalytics(**json.loads(cached_data))
        
        # Fetch wallet data from blockchain
        try:
            # Get balance
            balance_response = await self.blockchain_client.get(
                f"/chains/{chain}/accounts/{address}/balance"
            )
            balance_data = balance_response.json()
            
            # Get transaction history
            tx_response = await self.blockchain_client.get(
                f"/chains/{chain}/accounts/{address}/transactions",
                params={'limit': 1000}
            )
            transactions = tx_response.json()
            
            # Calculate analytics
            analytics = WalletAnalytics(
                address=address,
                chain=chain,
                native_balance=balance_data.get('native_balance', '0'),
                token_balances=balance_data.get('token_balances', []),
                total_value_usd=balance_data.get('total_value_usd'),
                transaction_count=len(transactions),
                first_transaction=transactions[-1]['timestamp'] if transactions else None,
                last_transaction=transactions[0]['timestamp'] if transactions else None,
                is_contract=balance_data.get('is_contract', False)
            )
            
            # Cache for 10 minutes
            await self.redis_client.setex(
                cache_key,
                600,
                analytics.json()
            )
            
            return analytics
            
        except Exception as e:
            self.logger.error(f"Error fetching wallet analytics: {e}")
            return WalletAnalytics(
                address=address,
                chain=chain,
                native_balance='0'
            )
    
    async def generate_report(
        self,
        name: str,
        report_type: str,
        chains: List[str],
        metrics: List[str],
        start_date: datetime,
        end_date: datetime,
        format: str = 'json'
    ) -> AnalyticsReport:
        """Generate analytics report"""
        self.logger.info(f"Generating {report_type} report: {name}")
        
        report = AnalyticsReport(
            name=name,
            report_type=report_type,
            start_date=start_date,
            end_date=end_date,
            chains=chains,
            metrics=metrics,
            format=format
        )
        
        # Collect data for each chain and metric
        for chain in chains:
            chain_data = {
                'chain': chain,
                'metrics': {},
                'charts': [],
                'insights': []
            }
            
            # Query each metric type
            for metric in metrics:
                query = AnalyticsQuery(
                    metric_type=MetricType(metric),
                    chains=[chain],
                    start_time=start_date,
                    end_time=end_date,
                    interval=TimeInterval.ONE_DAY
                )
                
                result = await self.query_analytics(query)
                chain_data['metrics'][metric] = result['data'][chain]['metrics']
                chain_data['insights'].extend(result['insights'])
            
            report.sections.append(chain_data)
        
        # Generate visualizations
        if format in ['html', 'pdf']:
            report.charts = await self._generate_charts(report.sections)
        
        # Save report
        await self._save_report(report)
        
        return report
    
    async def create_alert(self, alert: Alert) -> Alert:
        """Create a new alert"""
        # Validate alert
        if alert.metric not in ['transaction_count', 'gas_price', 'success_rate', 'tps']:
            raise ValueError(f"Unsupported metric for alerts: {alert.metric}")
        
        # Save to cache and database
        await self.alerts_cache.put(alert.alert_id, alert.json())
        self.active_alerts[alert.alert_id] = alert
        
        self.logger.info(f"Created alert: {alert.name}")
        return alert
    
    async def _monitor_alerts(self) -> None:
        """Monitor active alerts"""
        while self._running:
            try:
                for alert in self.active_alerts.values():
                    if not alert.is_active:
                        continue
                    
                    # Get current metric value
                    current_value = await self._get_metric_value(
                        alert.chain,
                        alert.metric
                    )
                    
                    if current_value is None:
                        continue
                    
                    # Check condition
                    triggered = False
                    if alert.condition == 'gt' and current_value > alert.threshold:
                        triggered = True
                    elif alert.condition == 'lt' and current_value < alert.threshold:
                        triggered = True
                    elif alert.condition == 'eq' and current_value == alert.threshold:
                        triggered = True
                    
                    if triggered:
                        await self._trigger_alert(alert, current_value)
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error monitoring alerts: {e}")
                await asyncio.sleep(60)
    
    async def _trigger_alert(self, alert: Alert, current_value: float) -> None:
        """Trigger an alert"""
        # Check if recently triggered
        if alert.last_triggered:
            time_since_last = datetime.utcnow() - alert.last_triggered
            if time_since_last.total_seconds() < alert.window_minutes * 60:
                return
        
        self.logger.warning(f"Alert triggered: {alert.name} (value: {current_value})")
        
        # Update alert
        alert.last_triggered = datetime.utcnow()
        alert.trigger_count += 1
        await self.alerts_cache.put(alert.alert_id, alert.json())
        
        # Send notifications
        if alert.webhook_url:
            await self._send_webhook_alert(alert, current_value)
        
        # TODO: Add email notifications
    
    async def _send_webhook_alert(self, alert: Alert, current_value: float) -> None:
        """Send webhook alert notification"""
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    alert.webhook_url,
                    json={
                        'alert': alert.dict(),
                        'current_value': current_value,
                        'timestamp': datetime.utcnow().isoformat()
                    },
                    timeout=10.0
                )
        except Exception as e:
            self.logger.error(f"Failed to send webhook alert: {e}")
    
    async def _process_analytics_queue(self) -> None:
        """Process queued analytics requests"""
        while self._running:
            try:
                # Process any queued analytics jobs
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error(f"Error processing analytics queue: {e}")
                await asyncio.sleep(5)
    
    async def _update_cached_metrics(self) -> None:
        """Update cached metrics periodically"""
        while self._running:
            try:
                # Update metrics for all configured chains
                for chain in config.chains:
                    try:
                        metrics = await self.get_chain_metrics(chain)
                        self.logger.debug(f"Updated metrics for {chain}")
                    except Exception as e:
                        self.logger.error(f"Error updating metrics for {chain}: {e}")
                
                await asyncio.sleep(300)  # Update every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error updating cached metrics: {e}")
                await asyncio.sleep(300)
    
    async def _cleanup_old_data(self) -> None:
        """Clean up old analytics data"""
        while self._running:
            try:
                # Clean up old reports
                cutoff_date = datetime.utcnow() - timedelta(
                    days=config.report_retention_days
                )
                
                # TODO: Implement cleanup logic
                
                await asyncio.sleep(86400)  # Run daily
                
            except Exception as e:
                self.logger.error(f"Error cleaning up old data: {e}")
                await asyncio.sleep(86400)
    
    # Helper methods
    
    def _get_analyzer_for_metric(self, metric_type: MetricType) -> Optional[BaseAnalyzer]:
        """Get appropriate analyzer for metric type"""
        if metric_type in [
            MetricType.TRANSACTION_COUNT,
            MetricType.TRANSACTION_VOLUME,
            MetricType.GAS_USED,
            MetricType.GAS_PRICE
        ]:
            return self.analyzers.get('transaction')
        
        # TODO: Add more analyzers
        return None
    
    def _get_metrics_for_type(self, metric_type: MetricType) -> List[str]:
        """Get list of metrics to calculate for a metric type"""
        metric_map = {
            MetricType.TRANSACTION_COUNT: ['transaction_count', 'tps', 'success_rate'],
            MetricType.TRANSACTION_VOLUME: ['transaction_volume'],
            MetricType.GAS_USED: ['gas_metrics'],
            MetricType.GAS_PRICE: ['gas_metrics'],
            MetricType.ACTIVE_ADDRESSES: ['active_addresses']
        }
        return metric_map.get(metric_type, [])
    
    def _generate_cache_key(self, query: AnalyticsQuery) -> str:
        """Generate cache key for query"""
        key_parts = [
            query.metric_type.value,
            '-'.join(sorted(query.chains)),
            query.start_time.strftime('%Y%m%d%H'),
            query.end_time.strftime('%Y%m%d%H'),
            query.interval.value if query.interval else 'none'
        ]
        return ':'.join(key_parts)
    
    async def _get_cached_result(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Get cached query result"""
        cached_data = await self.redis_client.get(f"query:{cache_key}")
        if cached_data:
            return json.loads(cached_data)
        return None
    
    async def _cache_result(self, cache_key: str, result: Dict[str, Any]) -> None:
        """Cache query result"""
        await self.redis_client.setex(
            f"query:{cache_key}",
            self.cache_ttl,
            json.dumps(result, default=str)
        )
    
    def _generate_summary(self, metrics: Dict[str, TimeSeries]) -> Dict[str, Any]:
        """Generate summary statistics from metrics"""
        summary = {}
        
        for metric_name, time_series in metrics.items():
            if time_series.data_points:
                values = [dp.value for dp in time_series.data_points]
                summary[metric_name] = {
                    'average': sum(values) / len(values),
                    'min': min(values),
                    'max': max(values),
                    'latest': values[-1] if values else None,
                    'count': len(values)
                }
        
        return summary
    
    async def _get_metric_value(self, chain: str, metric: str) -> Optional[float]:
        """Get current value of a metric"""
        metrics = await self.get_chain_metrics(chain)
        
        if metric == 'transaction_count':
            return float(metrics.transaction_count)
        elif metric == 'gas_price':
            return float(metrics.average_gas_price)
        elif metric == 'tps':
            return metrics.tps
        
        return None
    
    async def _load_alerts(self) -> None:
        """Load active alerts from storage"""
        # TODO: Load from database
        pass
    
    async def _save_report(self, report: AnalyticsReport) -> None:
        """Save report to storage"""
        # Save to cache
        await self.reports_cache.put(report.report_id, report.json())
        
        # Save to MongoDB for persistence
        await self.mongodb_db.reports.insert_one(report.dict())
    
    async def _generate_charts(self, sections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate chart configurations for report"""
        charts = []
        
        # TODO: Implement chart generation logic
        
        return charts 