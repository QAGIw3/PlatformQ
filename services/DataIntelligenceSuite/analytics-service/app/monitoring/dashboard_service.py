"""
Real-time Dashboard Service for Multi-Physics Simulations

Uses Apache Ignite for real-time data caching and Elasticsearch for analytics.
"""

import asyncio
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import numpy as np
from pyignite import Client as IgniteClient
from pyignite.datatypes import String, DoubleArray, TimestampArray
from elasticsearch import AsyncElasticsearch
from fastapi import WebSocket
import pandas as pd

logger = logging.getLogger(__name__)


class SimulationDashboardService:
    """Real-time dashboard service for monitoring multi-physics simulations"""
    
    def __init__(self, ignite_config: Dict[str, Any], es_config: Dict[str, Any]):
        self.ignite_config = ignite_config
        self.es_config = es_config
        self.ignite_client = None
        self.es_client = None
        self.websocket_connections: Dict[str, List[WebSocket]] = {}
        self._initialize_clients()
        
    def _initialize_clients(self):
        """Initialize Ignite and Elasticsearch clients"""
        # Initialize Ignite
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([
            (self.ignite_config.get('host', 'ignite'), 
             self.ignite_config.get('port', 10800))
        ])
        
        # Create caches
        self.metrics_cache = self.ignite_client.get_or_create_cache('simulation_metrics')
        self.alerts_cache = self.ignite_client.get_or_create_cache('simulation_alerts')
        self.dashboard_cache = self.ignite_client.get_or_create_cache('dashboard_state')
        
        # Initialize Elasticsearch
        self.es_client = AsyncElasticsearch(
            hosts=[self.es_config.get('host', 'http://elasticsearch:9200')],
            basic_auth=(self.es_config.get('username'), self.es_config.get('password'))
            if self.es_config.get('username') else None
        )
        
    async def create_simulation_dashboard(self, simulation_id: str) -> Dict[str, Any]:
        """Create a new dashboard for a simulation"""
        dashboard_id = f"dashboard_{simulation_id}"
        
        dashboard_config = {
            'dashboard_id': dashboard_id,
            'simulation_id': simulation_id,
            'created_at': datetime.utcnow().isoformat(),
            'widgets': [
                {
                    'id': 'convergence_chart',
                    'type': 'line_chart',
                    'title': 'Convergence History',
                    'metrics': ['residual', 'convergence_rate'],
                    'refresh_interval': 1000  # ms
                },
                {
                    'id': 'domain_metrics',
                    'type': 'multi_metric',
                    'title': 'Domain-wise Metrics',
                    'metrics': ['temperature', 'pressure', 'velocity', 'stress'],
                    'domains': []  # Will be populated dynamically
                },
                {
                    'id': 'resource_usage',
                    'type': 'gauge_chart',
                    'title': 'Resource Utilization',
                    'metrics': ['cpu_usage', 'memory_usage', 'gpu_usage'],
                    'thresholds': {'warning': 80, 'critical': 95}
                },
                {
                    'id': 'coupling_heatmap',
                    'type': 'heatmap',
                    'title': 'Coupling Strength Matrix',
                    'update_on': 'coupling_change'
                },
                {
                    'id': 'optimization_tracker',
                    'type': 'timeline',
                    'title': 'Optimization Events',
                    'events': ['parameter_change', 'quantum_optimization', 'convergence_milestone']
                }
            ],
            'layout': {
                'grid': [
                    ['convergence_chart', 'convergence_chart', 'resource_usage'],
                    ['domain_metrics', 'domain_metrics', 'optimization_tracker'],
                    ['coupling_heatmap', 'coupling_heatmap', 'coupling_heatmap']
                ]
            }
        }
        
        # Store in Ignite
        self.dashboard_cache.put(dashboard_id, dashboard_config)
        
        # Create Elasticsearch index for time-series data
        await self._create_simulation_index(simulation_id)
        
        return dashboard_config
        
    async def _create_simulation_index(self, simulation_id: str):
        """Create Elasticsearch index for simulation metrics"""
        index_name = f"simulation-metrics-{simulation_id}"
        
        mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "iteration": {"type": "long"},
                    "domain": {"type": "keyword"},
                    "metric_type": {"type": "keyword"},
                    "value": {"type": "double"},
                    "convergence": {
                        "properties": {
                            "residual": {"type": "double"},
                            "rate": {"type": "double"},
                            "is_converged": {"type": "boolean"}
                        }
                    },
                    "resources": {
                        "properties": {
                            "cpu_percent": {"type": "float"},
                            "memory_mb": {"type": "float"},
                            "gpu_percent": {"type": "float"}
                        }
                    },
                    "coupling": {
                        "properties": {
                            "source_domain": {"type": "keyword"},
                            "target_domain": {"type": "keyword"},
                            "strength": {"type": "float"}
                        }
                    }
                }
            }
        }
        
        await self.es_client.indices.create(
            index=index_name,
            body=mapping,
            ignore=400  # Ignore if already exists
        )
        
    async def update_metrics(self, simulation_id: str, metrics: Dict[str, Any]):
        """Update real-time metrics for a simulation"""
        # Store latest metrics in Ignite for fast access
        cache_key = f"metrics_{simulation_id}_latest"
        self.metrics_cache.put(cache_key, metrics)
        
        # Store time-series data in Elasticsearch
        doc = {
            "timestamp": datetime.utcnow(),
            "iteration": metrics.get("iteration", 0),
            **metrics
        }
        
        index_name = f"simulation-metrics-{simulation_id}"
        await self.es_client.index(
            index=index_name,
            body=doc
        )
        
        # Broadcast to connected WebSocket clients
        await self._broadcast_metrics_update(simulation_id, metrics)
        
        # Check for anomalies
        anomalies = await self._detect_anomalies(simulation_id, metrics)
        if anomalies:
            await self._handle_anomalies(simulation_id, anomalies)
            
    async def _broadcast_metrics_update(self, simulation_id: str, metrics: Dict[str, Any]):
        """Broadcast metrics update to WebSocket connections"""
        connections = self.websocket_connections.get(simulation_id, [])
        
        message = {
            "type": "metrics_update",
            "simulation_id": simulation_id,
            "data": metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to all connected clients
        disconnected = []
        for ws in connections:
            try:
                await ws.send_json(message)
            except:
                disconnected.append(ws)
                
        # Remove disconnected clients
        for ws in disconnected:
            connections.remove(ws)
            
    async def _detect_anomalies(self, simulation_id: str, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect anomalies in simulation metrics"""
        anomalies = []
        
        # Get historical data from Elasticsearch
        history = await self._get_metric_history(
            simulation_id, 
            lookback_minutes=5,
            metric_types=['residual', 'convergence_rate']
        )
        
        if len(history) < 10:
            return []  # Not enough data
            
        # Check for convergence stagnation
        residuals = [h['convergence']['residual'] for h in history if 'convergence' in h]
        if len(residuals) >= 10:
            recent_residuals = residuals[-10:]
            if np.std(recent_residuals) < 1e-8 and np.mean(recent_residuals) > 1e-3:
                anomalies.append({
                    'type': 'convergence_stagnation',
                    'severity': 'warning',
                    'message': 'Convergence has stagnated',
                    'data': {
                        'mean_residual': np.mean(recent_residuals),
                        'std_residual': np.std(recent_residuals)
                    }
                })
                
        # Check for divergence
        if len(residuals) >= 5:
            if residuals[-1] > residuals[-5] * 1.5:
                anomalies.append({
                    'type': 'divergence_detected',
                    'severity': 'critical',
                    'message': 'Simulation appears to be diverging',
                    'data': {
                        'current_residual': residuals[-1],
                        'previous_residual': residuals[-5],
                        'increase_factor': residuals[-1] / residuals[-5]
                    }
                })
                
        # Check resource usage anomalies
        if 'resources' in metrics:
            resources = metrics['resources']
            if resources.get('cpu_percent', 0) > 95:
                anomalies.append({
                    'type': 'high_cpu_usage',
                    'severity': 'warning',
                    'message': 'CPU usage is critically high',
                    'data': {'cpu_percent': resources['cpu_percent']}
                })
                
            if resources.get('memory_mb', 0) > 0.9 * resources.get('memory_limit_mb', float('inf')):
                anomalies.append({
                    'type': 'memory_pressure',
                    'severity': 'critical',
                    'message': 'Memory usage approaching limit',
                    'data': resources
                })
                
        return anomalies
        
    async def _handle_anomalies(self, simulation_id: str, anomalies: List[Dict[str, Any]]):
        """Handle detected anomalies"""
        for anomaly in anomalies:
            # Store in alerts cache
            alert_id = f"alert_{simulation_id}_{datetime.utcnow().timestamp()}"
            self.alerts_cache.put(alert_id, {
                'simulation_id': simulation_id,
                'timestamp': datetime.utcnow().isoformat(),
                **anomaly
            })
            
            # Broadcast alert
            await self._broadcast_alert(simulation_id, anomaly)
            
            # Take corrective action for critical anomalies
            if anomaly['severity'] == 'critical':
                await self._trigger_corrective_action(simulation_id, anomaly)
                
    async def _trigger_corrective_action(self, simulation_id: str, anomaly: Dict[str, Any]):
        """Trigger corrective action for critical anomalies"""
        if anomaly['type'] == 'divergence_detected':
            # Trigger parameter adjustment via quantum optimization
            logger.warning(f"Triggering quantum optimization for {simulation_id} due to divergence")
            # This would call the quantum optimization service
            
        elif anomaly['type'] == 'memory_pressure':
            # Trigger mesh decimation or domain decomposition
            logger.warning(f"Triggering resource optimization for {simulation_id}")
            # This would call resource management service
            
    async def _get_metric_history(self, 
                                 simulation_id: str,
                                 lookback_minutes: int = 10,
                                 metric_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get metric history from Elasticsearch"""
        index_name = f"simulation-metrics-{simulation_id}"
        
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": f"now-{lookback_minutes}m"
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [{"timestamp": "asc"}],
            "size": 1000
        }
        
        if metric_types:
            query["query"]["bool"]["must"].append({
                "terms": {"metric_type": metric_types}
            })
            
        result = await self.es_client.search(
            index=index_name,
            body=query
        )
        
        return [hit["_source"] for hit in result["hits"]["hits"]]
        
    async def _broadcast_alert(self, simulation_id: str, alert: Dict[str, Any]):
        """Broadcast alert to connected clients"""
        connections = self.websocket_connections.get(simulation_id, [])
        
        message = {
            "type": "alert",
            "simulation_id": simulation_id,
            "alert": alert,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        for ws in connections:
            try:
                await ws.send_json(message)
            except:
                pass
                
    async def get_convergence_analytics(self, simulation_id: str) -> Dict[str, Any]:
        """Get detailed convergence analytics"""
        # Get convergence history
        history = await self._get_metric_history(
            simulation_id,
            lookback_minutes=60,
            metric_types=['residual', 'convergence_rate']
        )
        
        if not history:
            return {}
            
        residuals = [h['convergence']['residual'] for h in history if 'convergence' in h]
        iterations = [h['iteration'] for h in history if 'iteration' in h]
        
        # Calculate analytics
        analytics = {
            'total_iterations': len(iterations),
            'current_residual': residuals[-1] if residuals else None,
            'best_residual': min(residuals) if residuals else None,
            'convergence_rate': self._calculate_convergence_rate(residuals),
            'estimated_iterations_remaining': self._estimate_remaining_iterations(residuals),
            'convergence_quality': self._assess_convergence_quality(residuals),
            'stagnation_periods': self._identify_stagnation_periods(residuals, iterations)
        }
        
        return analytics
        
    def _calculate_convergence_rate(self, residuals: List[float]) -> float:
        """Calculate average convergence rate"""
        if len(residuals) < 2:
            return 0.0
            
        rates = []
        for i in range(1, len(residuals)):
            if residuals[i-1] > 0:
                rate = (residuals[i-1] - residuals[i]) / residuals[i-1]
                rates.append(rate)
                
        return np.mean(rates) if rates else 0.0
        
    def _estimate_remaining_iterations(self, residuals: List[float], target: float = 1e-6) -> Optional[int]:
        """Estimate iterations remaining to reach target residual"""
        if len(residuals) < 10:
            return None
            
        # Use exponential fit on recent data
        recent_residuals = residuals[-20:]
        if min(recent_residuals) <= target:
            return 0
            
        # Simple linear extrapolation in log space
        log_residuals = np.log10(recent_residuals)
        x = np.arange(len(log_residuals))
        
        # Fit line
        slope, intercept = np.polyfit(x, log_residuals, 1)
        
        if slope >= 0:
            return None  # Not converging
            
        # Extrapolate to target
        log_target = np.log10(target)
        iterations_to_target = (log_target - intercept) / slope - len(log_residuals)
        
        return max(0, int(iterations_to_target))
        
    def _assess_convergence_quality(self, residuals: List[float]) -> str:
        """Assess the quality of convergence"""
        if len(residuals) < 10:
            return "insufficient_data"
            
        # Check convergence rate
        rate = self._calculate_convergence_rate(residuals[-10:])
        
        # Check stability
        recent_std = np.std(residuals[-10:])
        recent_mean = np.mean(residuals[-10:])
        cv = recent_std / recent_mean if recent_mean > 0 else float('inf')
        
        if rate > 0.1 and cv < 0.1:
            return "excellent"
        elif rate > 0.05 and cv < 0.2:
            return "good"
        elif rate > 0.01:
            return "fair"
        elif rate > 0:
            return "poor"
        else:
            return "stagnant"
            
    def _identify_stagnation_periods(self, residuals: List[float], iterations: List[int]) -> List[Dict[str, Any]]:
        """Identify periods where convergence stagnated"""
        if len(residuals) < 10:
            return []
            
        stagnation_periods = []
        window_size = 10
        threshold = 0.01  # 1% improvement threshold
        
        for i in range(window_size, len(residuals)):
            window = residuals[i-window_size:i]
            improvement = (window[0] - window[-1]) / window[0] if window[0] > 0 else 0
            
            if improvement < threshold:
                # Check if this continues a previous stagnation period
                if stagnation_periods and iterations[i-1] == stagnation_periods[-1]['end_iteration'] + 1:
                    stagnation_periods[-1]['end_iteration'] = iterations[i]
                    stagnation_periods[-1]['duration'] += 1
                else:
                    stagnation_periods.append({
                        'start_iteration': iterations[i-window_size],
                        'end_iteration': iterations[i],
                        'duration': window_size,
                        'improvement': improvement
                    })
                    
        return stagnation_periods
        
    async def get_resource_analytics(self, simulation_id: str) -> Dict[str, Any]:
        """Get resource usage analytics"""
        history = await self._get_metric_history(
            simulation_id,
            lookback_minutes=30
        )
        
        if not history:
            return {}
            
        # Extract resource data
        cpu_usage = []
        memory_usage = []
        gpu_usage = []
        
        for h in history:
            if 'resources' in h:
                cpu_usage.append(h['resources'].get('cpu_percent', 0))
                memory_usage.append(h['resources'].get('memory_mb', 0))
                gpu_usage.append(h['resources'].get('gpu_percent', 0))
                
        return {
            'cpu': {
                'current': cpu_usage[-1] if cpu_usage else 0,
                'average': np.mean(cpu_usage) if cpu_usage else 0,
                'peak': max(cpu_usage) if cpu_usage else 0,
                'std': np.std(cpu_usage) if cpu_usage else 0
            },
            'memory': {
                'current': memory_usage[-1] if memory_usage else 0,
                'average': np.mean(memory_usage) if memory_usage else 0,
                'peak': max(memory_usage) if memory_usage else 0,
                'trend': 'increasing' if len(memory_usage) > 2 and memory_usage[-1] > memory_usage[-3] else 'stable'
            },
            'gpu': {
                'current': gpu_usage[-1] if gpu_usage else 0,
                'average': np.mean(gpu_usage) if gpu_usage else 0,
                'peak': max(gpu_usage) if gpu_usage else 0,
                'utilization_score': np.mean(gpu_usage) / 100 if gpu_usage else 0
            }
        }
        
    async def register_websocket(self, simulation_id: str, websocket: WebSocket):
        """Register a WebSocket connection for real-time updates"""
        if simulation_id not in self.websocket_connections:
            self.websocket_connections[simulation_id] = []
            
        self.websocket_connections[simulation_id].append(websocket)
        
        # Send initial dashboard state
        dashboard_key = f"dashboard_{simulation_id}"
        dashboard_config = self.dashboard_cache.get(dashboard_key)
        
        if dashboard_config:
            await websocket.send_json({
                "type": "dashboard_config",
                "data": dashboard_config
            })
            
    async def unregister_websocket(self, simulation_id: str, websocket: WebSocket):
        """Unregister a WebSocket connection"""
        if simulation_id in self.websocket_connections:
            try:
                self.websocket_connections[simulation_id].remove(websocket)
            except ValueError:
                pass
                
    def close(self):
        """Close connections"""
        if self.ignite_client:
            self.ignite_client.close()
        if self.es_client:
            asyncio.create_task(self.es_client.close()) 