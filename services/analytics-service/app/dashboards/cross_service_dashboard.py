"""
Cross-Service Analytics Dashboard

Provides unified dashboards that aggregate data from multiple services
for comprehensive platform insights.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
import httpx
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np

from pyignite import Client as IgniteClient
from elasticsearch import AsyncElasticsearch
import apache_druid.client as druid_client

logger = logging.getLogger(__name__)


class DashboardConfig(BaseModel):
    """Configuration for cross-service dashboard"""
    refresh_interval: int = Field(default=30, description="Refresh interval in seconds")
    time_range: str = Field(default="1h", description="Default time range")
    aggregation_level: str = Field(default="minute", description="Data aggregation level")
    services: List[str] = Field(default=[], description="Services to include")
    metrics: List[str] = Field(default=[], description="Metrics to track")


class CrossServiceDashboard:
    """Unified dashboard for cross-service analytics"""
    
    def __init__(self,
                 ignite_client: IgniteClient,
                 es_client: AsyncElasticsearch,
                 druid_config: Dict[str, Any]):
        self.ignite = ignite_client
        self.es = es_client
        self.druid = druid_client.DruidClient(druid_config)
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Service endpoints
        self.service_endpoints = {
            "simulation": "http://simulation-service:8000",
            "cad": "http://cad-collaboration-service:8000",
            "digital_asset": "http://digital-asset-service:8000",
            "workflow": "http://workflow-service:8000",
            "federated_learning": "http://federated-learning-service:8000",
            "projects": "http://projects-service:8000"
        }
        
    async def get_platform_overview(self,
                                  time_range: str = "1h") -> Dict[str, Any]:
        """Get platform-wide overview metrics"""
        try:
            # Collect metrics from all services
            metrics = await asyncio.gather(
                self._get_simulation_metrics(time_range),
                self._get_asset_metrics(time_range),
                self._get_workflow_metrics(time_range),
                self._get_ml_metrics(time_range),
                self._get_collaboration_metrics(time_range),
                return_exceptions=True
            )
            
            # Aggregate results
            overview = {
                "timestamp": datetime.utcnow().isoformat(),
                "time_range": time_range,
                "services": {},
                "aggregated_metrics": {},
                "health_status": "healthy"
            }
            
            service_names = ["simulations", "assets", "workflows", "ml", "collaboration"]
            
            for i, (name, metric) in enumerate(zip(service_names, metrics)):
                if isinstance(metric, Exception):
                    logger.error(f"Error getting {name} metrics: {metric}")
                    overview["services"][name] = {"status": "error"}
                    overview["health_status"] = "degraded"
                else:
                    overview["services"][name] = metric
                    
            # Calculate aggregated metrics
            overview["aggregated_metrics"] = self._calculate_aggregated_metrics(overview["services"])
            
            return overview
            
        except Exception as e:
            logger.error(f"Error getting platform overview: {e}")
            raise
            
    async def get_service_comparison(self,
                                   services: List[str],
                                   metric: str,
                                   time_range: str = "1h") -> Dict[str, Any]:
        """Compare specific metric across services"""
        try:
            comparison = {
                "metric": metric,
                "time_range": time_range,
                "services": {},
                "chart_data": []
            }
            
            # Get metric from each service
            for service in services:
                if service in self.service_endpoints:
                    service_data = await self._get_service_metric(service, metric, time_range)
                    comparison["services"][service] = service_data
                    
            # Prepare chart data
            comparison["chart_data"] = self._prepare_comparison_chart(comparison["services"], metric)
            
            return comparison
            
        except Exception as e:
            logger.error(f"Error comparing services: {e}")
            raise
            
    async def get_user_activity_dashboard(self,
                                        user_id: Optional[str] = None,
                                        time_range: str = "24h") -> Dict[str, Any]:
        """Get user activity across all services"""
        try:
            # Query user activities from different sources
            activities = {
                "simulations": await self._get_user_simulations(user_id, time_range),
                "assets": await self._get_user_assets(user_id, time_range),
                "workflows": await self._get_user_workflows(user_id, time_range),
                "collaborations": await self._get_user_collaborations(user_id, time_range),
                "ml_training": await self._get_user_ml_training(user_id, time_range)
            }
            
            # Build activity timeline
            timeline = self._build_activity_timeline(activities)
            
            # Calculate user statistics
            stats = self._calculate_user_stats(activities)
            
            return {
                "user_id": user_id,
                "time_range": time_range,
                "activities": activities,
                "timeline": timeline,
                "statistics": stats,
                "engagement_score": self._calculate_engagement_score(activities)
            }
            
        except Exception as e:
            logger.error(f"Error getting user activity dashboard: {e}")
            raise
            
    async def get_resource_utilization_dashboard(self,
                                               time_range: str = "1h") -> Dict[str, Any]:
        """Get resource utilization across services"""
        try:
            # Query resource metrics from Druid
            query = {
                "queryType": "timeseries",
                "dataSource": "platform_metrics",
                "intervals": [self._get_time_interval(time_range)],
                "granularity": "minute",
                "aggregations": [
                    {"type": "doubleSum", "name": "cpu_usage", "fieldName": "cpu"},
                    {"type": "doubleSum", "name": "memory_usage", "fieldName": "memory"},
                    {"type": "doubleSum", "name": "gpu_usage", "fieldName": "gpu"},
                    {"type": "doubleSum", "name": "storage_usage", "fieldName": "storage"}
                ],
                "filter": {"type": "selector", "dimension": "metric_type", "value": "resource"}
            }
            
            druid_results = self.druid.query(query)
            
            # Get service-specific utilization
            service_utilization = await self._get_service_utilization()
            
            # Calculate cost estimates
            cost_estimates = self._calculate_resource_costs(druid_results, service_utilization)
            
            return {
                "time_range": time_range,
                "overall_utilization": self._process_druid_results(druid_results),
                "service_breakdown": service_utilization,
                "cost_estimates": cost_estimates,
                "optimization_recommendations": self._generate_resource_recommendations(service_utilization)
            }
            
        except Exception as e:
            logger.error(f"Error getting resource utilization dashboard: {e}")
            raise
            
    async def get_ml_performance_dashboard(self,
                                         time_range: str = "7d") -> Dict[str, Any]:
        """Get ML model performance across the platform"""
        try:
            # Get ML metrics from various sources
            ml_data = {
                "federated_sessions": await self._get_federated_learning_metrics(time_range),
                "model_performance": await self._get_model_performance_metrics(time_range),
                "training_jobs": await self._get_training_job_metrics(time_range),
                "anomaly_detection": await self._get_anomaly_detection_metrics(time_range)
            }
            
            # Calculate ML insights
            insights = {
                "model_accuracy_trend": self._calculate_accuracy_trend(ml_data),
                "training_efficiency": self._calculate_training_efficiency(ml_data),
                "data_quality_score": self._calculate_data_quality_score(ml_data),
                "model_drift_detection": self._detect_model_drift(ml_data)
            }
            
            return {
                "time_range": time_range,
                "ml_metrics": ml_data,
                "insights": insights,
                "recommendations": self._generate_ml_recommendations(ml_data, insights)
            }
            
        except Exception as e:
            logger.error(f"Error getting ML performance dashboard: {e}")
            raise
            
    async def get_collaboration_insights_dashboard(self,
                                                 time_range: str = "30d") -> Dict[str, Any]:
        """Get collaboration insights across projects and teams"""
        try:
            # Query collaboration data
            collab_data = await asyncio.gather(
                self._get_project_collaboration_metrics(time_range),
                self._get_cad_collaboration_metrics(time_range),
                self._get_workflow_collaboration_metrics(time_range)
            )
            
            # Analyze collaboration patterns
            patterns = {
                "collaboration_graph": await self._build_collaboration_graph(collab_data),
                "team_dynamics": self._analyze_team_dynamics(collab_data),
                "knowledge_flow": self._analyze_knowledge_flow(collab_data),
                "bottlenecks": self._identify_collaboration_bottlenecks(collab_data)
            }
            
            return {
                "time_range": time_range,
                "collaboration_metrics": {
                    "projects": collab_data[0],
                    "cad": collab_data[1],
                    "workflows": collab_data[2]
                },
                "patterns": patterns,
                "recommendations": self._generate_collaboration_recommendations(patterns)
            }
            
        except Exception as e:
            logger.error(f"Error getting collaboration insights: {e}")
            raise
            
    async def _get_simulation_metrics(self, time_range: str) -> Dict[str, Any]:
        """Get simulation service metrics"""
        try:
            # Query from Ignite cache
            sim_cache = self.ignite.get_or_create_cache("simulation_metrics_realtime")
            
            metrics = {
                "active_simulations": 0,
                "completed_last_hour": 0,
                "average_runtime": 0,
                "success_rate": 0,
                "resource_usage": {}
            }
            
            # Scan cache for recent metrics
            query = sim_cache.scan()
            simulations = []
            
            for _, sim in query:
                if self._is_within_time_range(sim.get("timestamp"), time_range):
                    simulations.append(sim)
                    
            if simulations:
                metrics["active_simulations"] = len([s for s in simulations if s.get("status") == "running"])
                metrics["completed_last_hour"] = len([s for s in simulations if s.get("status") == "completed"])
                
                runtimes = [s.get("runtime", 0) for s in simulations if s.get("runtime")]
                if runtimes:
                    metrics["average_runtime"] = np.mean(runtimes)
                    
                success_count = len([s for s in simulations if s.get("status") == "completed"])
                total_count = len([s for s in simulations if s.get("status") in ["completed", "failed"]])
                
                if total_count > 0:
                    metrics["success_rate"] = success_count / total_count
                    
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting simulation metrics: {e}")
            return {}
            
    async def _get_asset_metrics(self, time_range: str) -> Dict[str, Any]:
        """Get digital asset metrics"""
        try:
            # Query Elasticsearch for asset metrics
            query = {
                "query": {
                    "range": {
                        "created_at": {
                            "gte": f"now-{time_range}"
                        }
                    }
                },
                "aggs": {
                    "asset_types": {
                        "terms": {
                            "field": "asset_type"
                        }
                    },
                    "total_size": {
                        "sum": {
                            "field": "size"
                        }
                    }
                }
            }
            
            response = await self.es.search(index="assets", body=query)
            
            return {
                "total_assets": response["hits"]["total"]["value"],
                "asset_distribution": {
                    bucket["key"]: bucket["doc_count"]
                    for bucket in response["aggregations"]["asset_types"]["buckets"]
                },
                "total_storage_gb": response["aggregations"]["total_size"]["value"] / (1024**3),
                "recent_uploads": response["hits"]["total"]["value"]
            }
            
        except Exception as e:
            logger.error(f"Error getting asset metrics: {e}")
            return {}
            
    async def _get_workflow_metrics(self, time_range: str) -> Dict[str, Any]:
        """Get workflow metrics"""
        try:
            response = await self.http_client.get(
                f"{self.service_endpoints['workflow']}/api/v1/metrics",
                params={"time_range": time_range}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting workflow metrics: {e}")
            return {}
            
    async def _get_ml_metrics(self, time_range: str) -> Dict[str, Any]:
        """Get ML/federated learning metrics"""
        try:
            response = await self.http_client.get(
                f"{self.service_endpoints['federated_learning']}/api/v1/metrics",
                params={"time_range": time_range}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting ML metrics: {e}")
            return {}
            
    async def _get_collaboration_metrics(self, time_range: str) -> Dict[str, Any]:
        """Get collaboration metrics"""
        try:
            # Query from multiple sources
            cad_sessions = await self._get_cad_session_count(time_range)
            project_activities = await self._get_project_activities(time_range)
            
            return {
                "active_cad_sessions": cad_sessions,
                "project_activities": project_activities,
                "collaboration_score": self._calculate_collaboration_score(cad_sessions, project_activities)
            }
            
        except Exception as e:
            logger.error(f"Error getting collaboration metrics: {e}")
            return {}
            
    def _calculate_aggregated_metrics(self, services: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate platform-wide aggregated metrics"""
        total_activities = 0
        total_resources = 0
        
        for service, metrics in services.items():
            if isinstance(metrics, dict):
                total_activities += metrics.get("total_activities", 0)
                total_resources += metrics.get("resource_usage", {}).get("total", 0)
                
        return {
            "total_platform_activities": total_activities,
            "total_resource_consumption": total_resources,
            "service_health_score": self._calculate_health_score(services)
        }
        
    def _calculate_health_score(self, services: Dict[str, Any]) -> float:
        """Calculate overall platform health score"""
        healthy_services = sum(
            1 for s in services.values()
            if isinstance(s, dict) and s.get("status") != "error"
        )
        
        total_services = len(services)
        
        return healthy_services / total_services if total_services > 0 else 0
        
    def _is_within_time_range(self, timestamp: str, time_range: str) -> bool:
        """Check if timestamp is within time range"""
        try:
            ts = datetime.fromisoformat(timestamp)
            now = datetime.utcnow()
            
            # Parse time range
            if time_range.endswith('h'):
                hours = int(time_range[:-1])
                cutoff = now - timedelta(hours=hours)
            elif time_range.endswith('d'):
                days = int(time_range[:-1])
                cutoff = now - timedelta(days=days)
            else:
                cutoff = now - timedelta(hours=1)
                
            return ts >= cutoff
            
        except:
            return False
            
    def _get_time_interval(self, time_range: str) -> str:
        """Convert time range to Druid interval"""
        now = datetime.utcnow()
        
        if time_range.endswith('h'):
            hours = int(time_range[:-1])
            start = now - timedelta(hours=hours)
        elif time_range.endswith('d'):
            days = int(time_range[:-1])
            start = now - timedelta(days=days)
        else:
            start = now - timedelta(hours=1)
            
        return f"{start.isoformat()}/{now.isoformat()}"
        
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose()


class DashboardOrchestrator:
    """Orchestrates dashboard updates and caching"""
    
    def __init__(self,
                 dashboard: CrossServiceDashboard,
                 cache_ttl: int = 60):
        self.dashboard = dashboard
        self.cache_ttl = cache_ttl
        self.cache = {}
        self.update_tasks = {}
        
    async def start_auto_refresh(self, config: DashboardConfig):
        """Start auto-refresh for dashboard"""
        dashboard_id = f"{config.services}:{config.metrics}"
        
        if dashboard_id in self.update_tasks:
            return
            
        task = asyncio.create_task(
            self._refresh_loop(dashboard_id, config)
        )
        
        self.update_tasks[dashboard_id] = task
        
    async def _refresh_loop(self, dashboard_id: str, config: DashboardConfig):
        """Refresh dashboard data periodically"""
        while True:
            try:
                # Update dashboard data
                data = await self.dashboard.get_platform_overview(config.time_range)
                
                # Cache results
                self.cache[dashboard_id] = {
                    "data": data,
                    "timestamp": datetime.utcnow()
                }
                
                # Wait for next refresh
                await asyncio.sleep(config.refresh_interval)
                
            except Exception as e:
                logger.error(f"Error refreshing dashboard {dashboard_id}: {e}")
                await asyncio.sleep(60)  # Wait longer on error 