"""Cost Analyzer for collecting and analyzing cloud costs"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import json
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
import aiohttp
from kubernetes import client as k8s_client, config as k8s_config
from prometheus_api_client import PrometheusConnect
import numpy as np
from scipy import stats

from platformq_cost_common import (
    CostAnalysis,
    CostBreakdown,
    CostTrend,
    ResourceCost,
    CostAnomaly
)
from platformq_resource_common import ResourceType

from .config import settings
from .repository import CostRepository

logger = logging.getLogger(__name__)


class CostAnalyzer:
    """Analyzes costs from multiple cloud providers"""
    
    def __init__(self, repository: CostRepository):
        self.repository = repository
        self.aws_client = None
        self.cloudstack_session = None
        self.k8s_client = None
        self.prometheus = None
        self._init_clients()
        
    def _init_clients(self):
        """Initialize cloud provider clients"""
        # AWS Cost Explorer
        if settings.aws_cost_explorer_enabled and settings.aws_access_key_id:
            self.aws_client = boto3.client(
                'ce',
                region_name=settings.aws_region,
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key
            )
            
        # CloudStack client
        if settings.cloudstack_api_url:
            self.cloudstack_session = aiohttp.ClientSession()
            
        # Kubernetes client
        try:
            if settings.kubernetes_config_type == "incluster":
                k8s_config.load_incluster_config()
            else:
                k8s_config.load_kube_config()
            self.k8s_client = k8s_client.CoreV1Api()
        except Exception as e:
            logger.warning(f"Failed to initialize Kubernetes client: {e}")
            
        # Prometheus for Kubernetes metrics
        if settings.kubernetes_metrics_enabled:
            self.prometheus = PrometheusConnect(
                url="http://prometheus:9090",
                disable_ssl=True
            )
            
    async def analyze_costs(self, tenant_id: str, start_date: datetime, end_date: datetime) -> CostAnalysis:
        """Analyze costs for a tenant across all providers"""
        logger.info(f"Analyzing costs for tenant {tenant_id} from {start_date} to {end_date}")
        
        # Collect costs from all providers
        aws_costs = await self._get_aws_costs(tenant_id, start_date, end_date)
        cloudstack_costs = await self._get_cloudstack_costs(tenant_id, start_date, end_date)
        k8s_costs = await self._get_kubernetes_costs(tenant_id, start_date, end_date)
        
        # Combine costs
        all_costs = aws_costs + cloudstack_costs + k8s_costs
        total_cost = sum(cost.amount for cost in all_costs)
        
        # Calculate breakdown by resource type
        breakdown = self._calculate_breakdown(all_costs)
        
        # Calculate trends
        trends = await self._calculate_trends(tenant_id, total_cost)
        
        # Detect anomalies
        anomalies = await self._detect_anomalies(tenant_id, total_cost, all_costs)
        
        # Create analysis
        analysis = CostAnalysis(
            tenant_id=tenant_id,
            period_start=start_date,
            period_end=end_date,
            total_cost=total_cost,
            currency="USD",
            breakdown=breakdown,
            trends=trends,
            anomalies=anomalies,
            resource_costs=all_costs,
            analyzed_at=datetime.now(timezone.utc)
        )
        
        # Cache the analysis
        await self.repository.save_cost_analysis(analysis)
        
        return analysis
        
    async def _get_aws_costs(self, tenant_id: str, start_date: datetime, end_date: datetime) -> List[ResourceCost]:
        """Get costs from AWS Cost Explorer"""
        if not self.aws_client:
            return []
            
        try:
            response = self.aws_client.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.strftime('%Y-%m-%d'),
                    'End': end_date.strftime('%Y-%m-%d')
                },
                Granularity='DAILY',
                Metrics=['UnblendedCost'],
                GroupBy=[
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    },
                    {
                        'Type': 'TAG',
                        'Key': 'tenant_id'
                    }
                ],
                Filter={
                    'Tags': {
                        'Key': 'tenant_id',
                        'Values': [tenant_id]
                    }
                }
            )
            
            costs = []
            for result in response['ResultsByTime']:
                for group in result['Groups']:
                    service = group['Keys'][0]
                    amount = float(group['Metrics']['UnblendedCost']['Amount'])
                    
                    if amount > 0:
                        resource_type = self._map_aws_service_to_resource_type(service)
                        costs.append(ResourceCost(
                            resource_id=f"aws-{service}",
                            resource_type=resource_type,
                            provider="AWS",
                            amount=amount,
                            currency="USD",
                            usage_hours=24.0,  # Daily granularity
                            tags={"service": service, "tenant_id": tenant_id}
                        ))
                        
            return costs
            
        except ClientError as e:
            logger.error(f"Error getting AWS costs: {e}")
            return []
            
    async def _get_cloudstack_costs(self, tenant_id: str, start_date: datetime, end_date: datetime) -> List[ResourceCost]:
        """Get costs from CloudStack"""
        if not self.cloudstack_session or not settings.cloudstack_api_url:
            return []
            
        # CloudStack cost calculation would be implemented here
        # This is a stub implementation
        return []
        
    async def _get_kubernetes_costs(self, tenant_id: str, start_date: datetime, end_date: datetime) -> List[ResourceCost]:
        """Calculate Kubernetes resource costs based on usage"""
        if not self.k8s_client or not self.prometheus:
            return []
            
        costs = []
        
        try:
            # Get namespace for tenant
            namespace = f"tenant-{tenant_id}"
            
            # Query CPU usage
            cpu_query = f'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}"}}[1h])) by (pod)'
            cpu_results = self.prometheus.custom_query(cpu_query)
            
            # Query memory usage
            memory_query = f'sum(container_memory_usage_bytes{{namespace="{namespace}"}}) by (pod)'
            memory_results = self.prometheus.custom_query(memory_query)
            
            # Calculate costs based on usage
            # Assuming $0.05 per vCPU-hour and $0.01 per GB-hour
            cpu_cost_per_hour = 0.05
            memory_cost_per_gb_hour = 0.01
            
            total_cpu_cost = 0
            total_memory_cost = 0
            
            for result in cpu_results:
                cpu_cores = float(result['value'][1])
                hours = (end_date - start_date).total_seconds() / 3600
                total_cpu_cost += cpu_cores * cpu_cost_per_hour * hours
                
            for result in memory_results:
                memory_gb = float(result['value'][1]) / (1024**3)
                hours = (end_date - start_date).total_seconds() / 3600
                total_memory_cost += memory_gb * memory_cost_per_gb_hour * hours
                
            if total_cpu_cost > 0:
                costs.append(ResourceCost(
                    resource_id=f"k8s-cpu-{namespace}",
                    resource_type=ResourceType.COMPUTE,
                    provider="Kubernetes",
                    amount=total_cpu_cost,
                    currency="USD",
                    usage_hours=hours,
                    tags={"namespace": namespace, "resource": "cpu"}
                ))
                
            if total_memory_cost > 0:
                costs.append(ResourceCost(
                    resource_id=f"k8s-memory-{namespace}",
                    resource_type=ResourceType.COMPUTE,
                    provider="Kubernetes",
                    amount=total_memory_cost,
                    currency="USD",
                    usage_hours=hours,
                    tags={"namespace": namespace, "resource": "memory"}
                ))
                
        except Exception as e:
            logger.error(f"Error calculating Kubernetes costs: {e}")
            
        return costs
        
    def _map_aws_service_to_resource_type(self, service: str) -> ResourceType:
        """Map AWS service names to resource types"""
        mapping = {
            "Amazon Elastic Compute Cloud": ResourceType.COMPUTE,
            "Amazon Simple Storage Service": ResourceType.STORAGE,
            "Amazon Relational Database Service": ResourceType.DATABASE,
            "AWS Lambda": ResourceType.COMPUTE,
            "Amazon DynamoDB": ResourceType.DATABASE,
            "Amazon ElastiCache": ResourceType.DATABASE,
            "Elastic Load Balancing": ResourceType.NETWORK,
            "Amazon Virtual Private Cloud": ResourceType.NETWORK
        }
        
        for key, resource_type in mapping.items():
            if key in service:
                return resource_type
                
        return ResourceType.OTHER
        
    def _calculate_breakdown(self, costs: List[ResourceCost]) -> List[CostBreakdown]:
        """Calculate cost breakdown by resource type and provider"""
        breakdown_by_type = {}
        breakdown_by_provider = {}
        
        for cost in costs:
            # By resource type
            if cost.resource_type not in breakdown_by_type:
                breakdown_by_type[cost.resource_type] = 0
            breakdown_by_type[cost.resource_type] += cost.amount
            
            # By provider
            if cost.provider not in breakdown_by_provider:
                breakdown_by_provider[cost.provider] = 0
            breakdown_by_provider[cost.provider] += cost.amount
            
        breakdowns = []
        
        # Add resource type breakdowns
        total_cost = sum(costs.amount for costs in costs)
        for resource_type, amount in breakdown_by_type.items():
            breakdowns.append(CostBreakdown(
                category="resource_type",
                name=resource_type,
                amount=amount,
                percentage=(amount / total_cost * 100) if total_cost > 0 else 0
            ))
            
        # Add provider breakdowns
        for provider, amount in breakdown_by_provider.items():
            breakdowns.append(CostBreakdown(
                category="provider",
                name=provider,
                amount=amount,
                percentage=(amount / total_cost * 100) if total_cost > 0 else 0
            ))
            
        return breakdowns
        
    async def _calculate_trends(self, tenant_id: str, current_cost: float) -> List[CostTrend]:
        """Calculate cost trends compared to previous periods"""
        trends = []
        
        # Get historical data
        history = await self.repository.get_cost_history(
            tenant_id=tenant_id,
            days=settings.recommendation_lookback_days
        )
        
        if len(history) < 7:
            return trends
            
        # Daily trend
        yesterday_cost = history[-2].total_cost if len(history) >= 2 else current_cost
        daily_change = ((current_cost - yesterday_cost) / yesterday_cost * 100) if yesterday_cost > 0 else 0
        
        trends.append(CostTrend(
            period="daily",
            change_percentage=daily_change,
            previous_amount=yesterday_cost,
            current_amount=current_cost
        ))
        
        # Weekly trend
        if len(history) >= 7:
            week_ago_cost = history[-7].total_cost
            weekly_change = ((current_cost - week_ago_cost) / week_ago_cost * 100) if week_ago_cost > 0 else 0
            
            trends.append(CostTrend(
                period="weekly",
                change_percentage=weekly_change,
                previous_amount=week_ago_cost,
                current_amount=current_cost
            ))
            
        # Monthly trend
        if len(history) >= 30:
            month_ago_cost = history[-30].total_cost
            monthly_change = ((current_cost - month_ago_cost) / month_ago_cost * 100) if month_ago_cost > 0 else 0
            
            trends.append(CostTrend(
                period="monthly",
                change_percentage=monthly_change,
                previous_amount=month_ago_cost,
                current_amount=current_cost
            ))
            
        return trends
        
    async def _detect_anomalies(self, tenant_id: str, current_cost: float, resource_costs: List[ResourceCost]) -> List[CostAnomaly]:
        """Detect cost anomalies using statistical methods"""
        anomalies = []
        
        # Get historical data
        history = await self.repository.get_cost_history(
            tenant_id=tenant_id,
            days=settings.recommendation_lookback_days
        )
        
        if len(history) < 7:
            return anomalies
            
        # Extract daily costs
        daily_costs = [h.total_cost for h in history[:-1]]  # Exclude current
        
        # Calculate statistics
        mean_cost = np.mean(daily_costs)
        std_cost = np.std(daily_costs)
        
        # Z-score method for anomaly detection
        if std_cost > 0:
            z_score = (current_cost - mean_cost) / std_cost
            
            # If cost deviates by more than threshold
            threshold_multiplier = settings.cost_anomaly_threshold_percent / 100
            if abs(z_score) > 2:  # 2 standard deviations
                deviation_percent = ((current_cost - mean_cost) / mean_cost * 100) if mean_cost > 0 else 0
                
                if abs(deviation_percent) > settings.cost_anomaly_threshold_percent:
                    anomalies.append(CostAnomaly(
                        resource_id="total_cost",
                        anomaly_type="spike" if current_cost > mean_cost else "drop",
                        expected_cost=mean_cost,
                        actual_cost=current_cost,
                        deviation_percentage=deviation_percent,
                        confidence_score=min(abs(z_score) / 3, 1.0),  # Normalize to 0-1
                        detected_at=datetime.now(timezone.utc),
                        description=f"Total cost {'increased' if current_cost > mean_cost else 'decreased'} by {abs(deviation_percent):.1f}% compared to {settings.recommendation_lookback_days}-day average"
                    ))
                    
        # Check for individual resource anomalies
        for cost in resource_costs:
            # Get historical data for this resource
            resource_history = await self.repository.get_resource_cost_history(
                tenant_id=tenant_id,
                resource_id=cost.resource_id,
                days=7
            )
            
            if len(resource_history) >= 3:
                historical_amounts = [h.amount for h in resource_history[:-1]]
                mean_amount = np.mean(historical_amounts)
                std_amount = np.std(historical_amounts)
                
                if std_amount > 0 and mean_amount > 0:
                    z_score = (cost.amount - mean_amount) / std_amount
                    deviation_percent = ((cost.amount - mean_amount) / mean_amount * 100)
                    
                    if abs(z_score) > 2 and abs(deviation_percent) > settings.cost_anomaly_threshold_percent:
                        anomalies.append(CostAnomaly(
                            resource_id=cost.resource_id,
                            anomaly_type="spike" if cost.amount > mean_amount else "drop",
                            expected_cost=mean_amount,
                            actual_cost=cost.amount,
                            deviation_percentage=deviation_percent,
                            confidence_score=min(abs(z_score) / 3, 1.0),
                            detected_at=datetime.now(timezone.utc),
                            description=f"{cost.resource_type} cost {'increased' if cost.amount > mean_amount else 'decreased'} by {abs(deviation_percent):.1f}%"
                        ))
                        
        return anomalies
        
    async def close(self):
        """Close connections"""
        if self.cloudstack_session:
            await self.cloudstack_session.close() 