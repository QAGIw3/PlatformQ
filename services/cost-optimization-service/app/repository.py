"""Repository for cost optimization data"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import json
from uuid import uuid4
import pickle

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy
from pyignite import Client
from pyignite.datatypes import String, TimestampObject

from platformq_cost_common import (
    CostAnalysis,
    CostRecommendation,
    Budget,
    BudgetAlert,
    ResourceCost
)
from platformq_resource_common import ResourceMetrics

from .config import settings

logger = logging.getLogger(__name__)


class CostRepository:
    """Repository for cost optimization data"""
    
    def __init__(self):
        self.cassandra_cluster = None
        self.cassandra_session = None
        self.ignite_client = None
        self._init_connections()
        
    def _init_connections(self):
        """Initialize database connections"""
        # Initialize Cassandra
        try:
            auth_provider = None
            if settings.cassandra_username:
                auth_provider = PlainTextAuthProvider(
                    username=settings.cassandra_username,
                    password=settings.cassandra_password
                )
                
            self.cassandra_cluster = Cluster(
                settings.cassandra_hosts.split(','),
                auth_provider=auth_provider,
                default_retry_policy=RetryPolicy(),
                reconnection_policy=ExponentialReconnectionPolicy(1.0, 600.0)
            )
            self.cassandra_session = self.cassandra_cluster.connect()
            
            # Create keyspace and tables
            self._create_cassandra_schema()
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            
        # Initialize Ignite
        try:
            self.ignite_client = Client()
            self.ignite_client.connect(settings.ignite_host, settings.ignite_port)
            
            # Create cache if not exists
            self.cost_cache = self.ignite_client.get_or_create_cache(
                settings.ignite_cache_name
            )
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            
    def _create_cassandra_schema(self):
        """Create Cassandra schema"""
        # Create keyspace
        self.cassandra_session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {settings.cassandra_keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 3
            }}
        """)
        
        self.cassandra_session.set_keyspace(settings.cassandra_keyspace)
        
        # Cost analysis table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS cost_analysis (
                tenant_id text,
                analysis_id text,
                period_start timestamp,
                period_end timestamp,
                total_cost decimal,
                currency text,
                breakdown frozen<list<map<text, text>>>,
                trends frozen<list<map<text, text>>>,
                anomalies frozen<list<map<text, text>>>,
                resource_costs frozen<list<map<text, text>>>,
                analyzed_at timestamp,
                PRIMARY KEY (tenant_id, analysis_id)
            ) WITH CLUSTERING ORDER BY (analysis_id DESC)
        """)
        
        # Recommendations table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS cost_recommendations (
                tenant_id text,
                recommendation_id text,
                resource_id text,
                recommendation_type text,
                title text,
                description text,
                estimated_monthly_savings decimal,
                implementation_effort text,
                risk_level text,
                priority text,
                confidence_score float,
                action_items list<text>,
                metadata map<text, text>,
                generated_at timestamp,
                status text,
                PRIMARY KEY (tenant_id, recommendation_id)
            ) WITH CLUSTERING ORDER BY (recommendation_id DESC)
        """)
        
        # Budget table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS budgets (
                tenant_id text,
                budget_id text,
                name text,
                amount decimal,
                period text,
                start_date date,
                end_date date,
                resource_filters map<text, text>,
                alert_thresholds list<int>,
                created_at timestamp,
                updated_at timestamp,
                PRIMARY KEY (tenant_id, budget_id)
            )
        """)
        
        # Budget alerts table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS budget_alerts (
                tenant_id text,
                alert_id text,
                budget_id text,
                threshold_percentage int,
                current_spend decimal,
                budget_amount decimal,
                alert_type text,
                message text,
                triggered_at timestamp,
                PRIMARY KEY (tenant_id, alert_id)
            ) WITH CLUSTERING ORDER BY (alert_id DESC)
        """)
        
        # Resource cost history table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS resource_cost_history (
                tenant_id text,
                resource_id text,
                date date,
                amount decimal,
                usage_hours float,
                resource_type text,
                provider text,
                tags map<text, text>,
                PRIMARY KEY ((tenant_id, resource_id), date)
            ) WITH CLUSTERING ORDER BY (date DESC)
        """)
        
        # Create indexes
        self.cassandra_session.execute("""
            CREATE INDEX IF NOT EXISTS idx_recommendations_type
            ON cost_recommendations (recommendation_type)
        """)
        
        self.cassandra_session.execute("""
            CREATE INDEX IF NOT EXISTS idx_recommendations_status
            ON cost_recommendations (status)
        """)
        
    async def save_cost_analysis(self, analysis: CostAnalysis) -> str:
        """Save cost analysis"""
        analysis_id = str(uuid4())
        
        # Convert complex objects to JSON
        breakdown_json = [
            {
                "category": b.category,
                "name": b.name,
                "amount": str(b.amount),
                "percentage": str(b.percentage)
            }
            for b in analysis.breakdown
        ]
        
        trends_json = [
            {
                "period": t.period,
                "change_percentage": str(t.change_percentage),
                "previous_amount": str(t.previous_amount),
                "current_amount": str(t.current_amount)
            }
            for t in analysis.trends
        ]
        
        anomalies_json = [
            {
                "resource_id": a.resource_id,
                "anomaly_type": a.anomaly_type,
                "expected_cost": str(a.expected_cost),
                "actual_cost": str(a.actual_cost),
                "deviation_percentage": str(a.deviation_percentage),
                "confidence_score": str(a.confidence_score),
                "description": a.description
            }
            for a in analysis.anomalies
        ]
        
        resource_costs_json = [
            {
                "resource_id": rc.resource_id,
                "resource_type": rc.resource_type,
                "provider": rc.provider,
                "amount": str(rc.amount),
                "currency": rc.currency,
                "usage_hours": str(rc.usage_hours)
            }
            for rc in analysis.resource_costs
        ]
        
        # Save to Cassandra
        query = """
            INSERT INTO cost_analysis (
                tenant_id, analysis_id, period_start, period_end,
                total_cost, currency, breakdown, trends, anomalies,
                resource_costs, analyzed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(query, (
            analysis.tenant_id,
            analysis_id,
            analysis.period_start,
            analysis.period_end,
            analysis.total_cost,
            analysis.currency,
            breakdown_json,
            trends_json,
            anomalies_json,
            resource_costs_json,
            analysis.analyzed_at
        ))
        
        # Cache in Ignite
        cache_key = f"cost_analysis:{analysis.tenant_id}:{analysis_id}"
        self.cost_cache.put(cache_key, pickle.dumps(analysis))
        
        # Also cache latest analysis for quick access
        latest_key = f"cost_analysis:latest:{analysis.tenant_id}"
        self.cost_cache.put(latest_key, pickle.dumps(analysis))
        
        return analysis_id
        
    async def get_cost_analysis(self, tenant_id: str, analysis_id: str) -> Optional[CostAnalysis]:
        """Get cost analysis by ID"""
        # Try cache first
        cache_key = f"cost_analysis:{tenant_id}:{analysis_id}"
        cached = self.cost_cache.get(cache_key)
        if cached:
            return pickle.loads(cached)
            
        # Query Cassandra
        query = """
            SELECT * FROM cost_analysis
            WHERE tenant_id = ? AND analysis_id = ?
        """
        
        result = self.cassandra_session.execute(query, (tenant_id, analysis_id))
        row = result.one()
        
        if row:
            # Convert back to CostAnalysis object
            # This would need proper deserialization
            return self._deserialize_cost_analysis(row)
            
        return None
        
    async def get_cost_history(self, tenant_id: str, days: int) -> List[CostAnalysis]:
        """Get cost analysis history"""
        # For simplicity, return mock data
        # In production, this would query Cassandra with date range
        return []
        
    async def save_recommendation(self, recommendation: CostRecommendation) -> None:
        """Save cost recommendation"""
        query = """
            INSERT INTO cost_recommendations (
                tenant_id, recommendation_id, resource_id, recommendation_type,
                title, description, estimated_monthly_savings, implementation_effort,
                risk_level, priority, confidence_score, action_items, metadata,
                generated_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        metadata_str = {k: str(v) for k, v in recommendation.metadata.items()}
        
        self.cassandra_session.execute(query, (
            recommendation.tenant_id,
            recommendation.recommendation_id,
            recommendation.resource_id,
            recommendation.recommendation_type.value,
            recommendation.title,
            recommendation.description,
            recommendation.estimated_monthly_savings,
            recommendation.implementation_effort,
            recommendation.risk_level,
            recommendation.priority.value,
            recommendation.confidence_score,
            recommendation.action_items,
            metadata_str,
            recommendation.generated_at,
            "pending"
        ))
        
        # Cache in Ignite
        cache_key = f"recommendation:{recommendation.tenant_id}:{recommendation.recommendation_id}"
        self.cost_cache.put(cache_key, pickle.dumps(recommendation))
        
    async def get_recommendations(
        self,
        tenant_id: str,
        status: Optional[str] = None,
        recommendation_type: Optional[str] = None
    ) -> List[CostRecommendation]:
        """Get recommendations for tenant"""
        query = "SELECT * FROM cost_recommendations WHERE tenant_id = ?"
        params = [tenant_id]
        
        if status:
            query += " AND status = ?"
            params.append(status)
            
        if recommendation_type:
            query += " AND recommendation_type = ?"
            params.append(recommendation_type)
            
        query += " ALLOW FILTERING"
        
        results = self.cassandra_session.execute(query, params)
        
        recommendations = []
        for row in results:
            # Convert row to CostRecommendation
            # This would need proper deserialization
            pass
            
        return recommendations
        
    async def save_budget(self, budget: Budget) -> str:
        """Save budget"""
        budget_id = str(uuid4())
        
        query = """
            INSERT INTO budgets (
                tenant_id, budget_id, name, amount, period,
                start_date, end_date, resource_filters, alert_thresholds,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(query, (
            budget.tenant_id,
            budget_id,
            budget.name,
            budget.amount,
            budget.period,
            budget.start_date,
            budget.end_date,
            budget.resource_filters,
            budget.alert_thresholds,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc)
        ))
        
        # Cache in Ignite
        cache_key = f"budget:{budget.tenant_id}:{budget_id}"
        self.cost_cache.put(cache_key, pickle.dumps(budget))
        
        return budget_id
        
    async def get_budgets(self, tenant_id: str) -> List[Budget]:
        """Get all budgets for tenant"""
        query = "SELECT * FROM budgets WHERE tenant_id = ?"
        results = self.cassandra_session.execute(query, (tenant_id,))
        
        budgets = []
        for row in results:
            # Convert row to Budget
            # This would need proper deserialization
            pass
            
        return budgets
        
    async def save_budget_alert(self, alert: BudgetAlert) -> None:
        """Save budget alert"""
        alert_id = str(uuid4())
        
        query = """
            INSERT INTO budget_alerts (
                tenant_id, alert_id, budget_id, threshold_percentage,
                current_spend, budget_amount, alert_type, message, triggered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(query, (
            alert.tenant_id,
            alert_id,
            alert.budget_id,
            alert.threshold_percentage,
            alert.current_spend,
            alert.budget_amount,
            alert.alert_type,
            alert.message,
            alert.triggered_at
        ))
        
    async def get_resource_usage_history(self, tenant_id: str, days: int) -> List[Any]:
        """Get resource usage history"""
        # Mock implementation
        return []
        
    async def get_last_resource_usage(self, tenant_id: str, resource_id: str) -> Optional[datetime]:
        """Get last usage timestamp for resource"""
        # Mock implementation
        return None
        
    async def get_hourly_usage_pattern(self, tenant_id: str, resource_id: str, days: int) -> Dict[int, float]:
        """Get hourly usage pattern"""
        # Mock implementation
        return {}
        
    async def get_resource_cost(self, tenant_id: str, resource_id: str) -> Optional[ResourceCost]:
        """Get current cost for resource"""
        # Mock implementation
        return None
        
    async def get_storage_metrics(self, tenant_id: str, resource_id: str) -> Optional[Dict[str, Any]]:
        """Get storage metrics"""
        # Mock implementation
        return None
        
    async def get_resource_cost_history(
        self,
        tenant_id: str,
        resource_id: str,
        days: int
    ) -> List[ResourceCost]:
        """Get resource cost history"""
        end_date = datetime.now(timezone.utc).date()
        start_date = end_date - timedelta(days=days)
        
        query = """
            SELECT * FROM resource_cost_history
            WHERE tenant_id = ? AND resource_id = ?
            AND date >= ? AND date <= ?
        """
        
        results = self.cassandra_session.execute(
            query,
            (tenant_id, resource_id, start_date, end_date)
        )
        
        costs = []
        for row in results:
            # Convert row to ResourceCost
            # This would need proper deserialization
            pass
            
        return costs
        
    def _deserialize_cost_analysis(self, row) -> CostAnalysis:
        """Deserialize cost analysis from Cassandra row"""
        # This would properly deserialize the row data
        # For now, return None as placeholder
        return None
        
    async def close(self):
        """Close connections"""
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()
        if self.ignite_client:
            self.ignite_client.close() 