"""Cost Analyzer

Analyzes cost data and generates insights, trends, and budget alerts.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import json

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from platformq_cost_common.models import (
    CostAnalysis,
    CostBreakdown,
    CostTrend,
    BudgetAlert,
    ResourceCost
)

from .config import Settings
from .aggregator import MetricsAggregator

logger = logging.getLogger(__name__)


class CostAnalyzer:
    """Analyzes costs and budgets"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.aggregator = None  # Will be set by main
        
        # Initialize Cassandra for budget storage
        auth_provider = None
        if settings.cassandra_username:
            auth_provider = PlainTextAuthProvider(
                username=settings.cassandra_username,
                password=settings.cassandra_password
            )
        
        self.cluster = Cluster(
            settings.cassandra_hosts.split(','),
            auth_provider=auth_provider
        )
        self.session = None
        
    async def initialize(self):
        """Initialize analyzer"""
        self.session = self.cluster.connect()
        
        # Create keyspace if not exists
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.settings.cassandra_keyspace}
            WITH REPLICATION = {{
                'class': 'SimpleStrategy',
                'replication_factor': 3
            }}
        """)
        
        self.session.set_keyspace(self.settings.cassandra_keyspace)
        
        # Create tables
        self._create_tables()
        
    def _create_tables(self):
        """Create required Cassandra tables"""
        
        # Budget table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS budgets (
                tenant_id text PRIMARY KEY,
                monthly_limit decimal,
                currency text,
                alert_thresholds list<int>,
                created_at timestamp,
                updated_at timestamp
            )
        """)
        
        # Budget alerts table
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS budget_alerts (
                alert_id uuid PRIMARY KEY,
                tenant_id text,
                threshold_percent int,
                current_spend decimal,
                budget_limit decimal,
                alert_time timestamp,
                acknowledged boolean,
                metadata text
            )
        """)
        
        # Create index on tenant_id for alerts
        self.session.execute("""
            CREATE INDEX IF NOT EXISTS budget_alerts_by_tenant
            ON budget_alerts (tenant_id)
        """)
    
    async def analyze_costs(
        self,
        tenant_id: str,
        cost_data: Dict[str, Any],
        include_forecast: bool = False
    ) -> CostAnalysis:
        """Analyze cost data and generate insights"""
        
        # Calculate totals
        total_cost = Decimal('0')
        service_costs = {}
        
        for item in cost_data.get("results", []):
            cost = Decimal(str(item.get("rating", 0)))
            total_cost += cost
            
            service = item.get("service", "unknown")
            if service not in service_costs:
                service_costs[service] = Decimal('0')
            service_costs[service] += cost
        
        # Get previous period for comparison
        current_period_days = (
            datetime.fromisoformat(cost_data.get("period", {}).get("end", datetime.utcnow().isoformat())) -
            datetime.fromisoformat(cost_data.get("period", {}).get("begin", datetime.utcnow().isoformat()))
        ).days
        
        previous_cost = await self._get_previous_period_cost(
            tenant_id,
            current_period_days
        )
        
        # Calculate change
        if previous_cost > 0:
            change_percent = ((total_cost - previous_cost) / previous_cost) * 100
        else:
            change_percent = 0
        
        # Create analysis
        analysis = CostAnalysis(
            tenant_id=tenant_id,
            period_start=datetime.fromisoformat(cost_data.get("period", {}).get("begin", datetime.utcnow().isoformat())),
            period_end=datetime.fromisoformat(cost_data.get("period", {}).get("end", datetime.utcnow().isoformat())),
            total_cost=total_cost,
            currency="USD",
            cost_by_service=service_costs,
            cost_change_percent=float(change_percent),
            top_services=self._get_top_services(service_costs),
            anomalies=await self._detect_anomalies(tenant_id, total_cost)
        )
        
        if include_forecast:
            analysis.forecast = await self._generate_forecast(tenant_id, total_cost)
        
        return analysis
    
    async def get_cost_breakdown(
        self,
        tenant_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> CostBreakdown:
        """Get detailed cost breakdown"""
        
        # Get cost data from aggregator
        cost_data = await self.aggregator.get_cloudkitty_summary(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            group_by=["service", "plan", "resource_type"]
        )
        
        # Process into breakdown
        by_service = {}
        by_resource_type = {}
        by_region = {}
        details = []
        
        for item in cost_data.get("results", []):
            cost = Decimal(str(item.get("rating", 0)))
            
            # By service
            service = item.get("service", "unknown")
            if service not in by_service:
                by_service[service] = Decimal('0')
            by_service[service] += cost
            
            # By resource type
            resource_type = item.get("resource_type", "unknown")
            if resource_type not in by_resource_type:
                by_resource_type[resource_type] = Decimal('0')
            by_resource_type[resource_type] += cost
            
            # By region (from metadata)
            region = item.get("metadata", {}).get("region", "default")
            if region not in by_region:
                by_region[region] = Decimal('0')
            by_region[region] += cost
            
            # Details
            details.append({
                "service": service,
                "resource_type": resource_type,
                "plan": item.get("plan", "default"),
                "quantity": item.get("quantity", 0),
                "unit_cost": item.get("unit_cost", 0),
                "total_cost": float(cost),
                "metadata": item.get("metadata", {})
            })
        
        return CostBreakdown(
            tenant_id=tenant_id,
            period_start=start_date,
            period_end=end_date,
            by_service=by_service,
            by_resource_type=by_resource_type,
            by_region=by_region,
            details=details
        )
    
    async def get_cost_trends(
        self,
        tenant_id: str,
        start_date: datetime,
        end_date: datetime,
        period: str = "daily"
    ) -> List[CostTrend]:
        """Get cost trends over time"""
        
        # Determine time buckets
        if period == "hourly":
            delta = timedelta(hours=1)
        elif period == "daily":
            delta = timedelta(days=1)
        elif period == "weekly":
            delta = timedelta(weeks=1)
        else:  # monthly
            delta = timedelta(days=30)
        
        trends = []
        current_date = start_date
        
        while current_date < end_date:
            period_end = min(current_date + delta, end_date)
            
            # Get cost for this period
            cost_data = await self.aggregator.get_cloudkitty_summary(
                tenant_id=tenant_id,
                start_date=current_date,
                end_date=period_end,
                group_by=["service"]
            )
            
            total_cost = sum(
                Decimal(str(item.get("rating", 0)))
                for item in cost_data.get("results", [])
            )
            
            trend = CostTrend(
                timestamp=current_date,
                cost=total_cost,
                period=period,
                service_breakdown={
                    item.get("service", "unknown"): Decimal(str(item.get("rating", 0)))
                    for item in cost_data.get("results", [])
                }
            )
            trends.append(trend)
            
            current_date = period_end
        
        return trends
    
    async def get_budget(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get budget configuration for tenant"""
        
        result = self.session.execute(
            "SELECT * FROM budgets WHERE tenant_id = %s",
            [tenant_id]
        )
        
        row = result.one()
        if row:
            return {
                "tenant_id": row.tenant_id,
                "monthly_limit": float(row.monthly_limit),
                "currency": row.currency,
                "alert_thresholds": row.alert_thresholds,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None
            }
        
        return None
    
    async def set_budget(
        self,
        tenant_id: str,
        monthly_limit: Decimal,
        alert_thresholds: List[int],
        currency: str = "USD"
    ):
        """Set budget for tenant"""
        
        self.session.execute(
            """
            INSERT INTO budgets (tenant_id, monthly_limit, currency, alert_thresholds, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                tenant_id,
                monthly_limit,
                currency,
                alert_thresholds,
                datetime.utcnow(),
                datetime.utcnow()
            ]
        )
    
    async def get_budget_alerts(
        self,
        tenant_id: str,
        active_only: bool = True
    ) -> List[BudgetAlert]:
        """Get budget alerts for tenant"""
        
        query = "SELECT * FROM budget_alerts WHERE tenant_id = %s"
        if active_only:
            query += " AND acknowledged = false"
        query += " ALLOW FILTERING"
        
        result = self.session.execute(query, [tenant_id])
        
        alerts = []
        for row in result:
            metadata = json.loads(row.metadata) if row.metadata else {}
            
            alert = BudgetAlert(
                alert_id=str(row.alert_id),
                tenant_id=row.tenant_id,
                threshold_percent=row.threshold_percent,
                current_spend=row.current_spend,
                budget_limit=row.budget_limit,
                alert_time=row.alert_time,
                severity="high" if row.threshold_percent >= 90 else "medium",
                message=f"Budget threshold {row.threshold_percent}% exceeded",
                metadata=metadata
            )
            alerts.append(alert)
        
        return alerts
    
    async def _get_previous_period_cost(
        self,
        tenant_id: str,
        period_days: int
    ) -> Decimal:
        """Get cost from previous period for comparison"""
        
        end_date = datetime.utcnow() - timedelta(days=period_days)
        start_date = end_date - timedelta(days=period_days)
        
        cost_data = await self.aggregator.get_cloudkitty_summary(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            group_by=["service"]
        )
        
        total_cost = sum(
            Decimal(str(item.get("rating", 0)))
            for item in cost_data.get("results", [])
        )
        
        return total_cost
    
    def _get_top_services(
        self,
        service_costs: Dict[str, Decimal],
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Get top services by cost"""
        
        sorted_services = sorted(
            service_costs.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        total = sum(service_costs.values())
        
        top_services = []
        for service, cost in sorted_services[:limit]:
            percentage = (cost / total * 100) if total > 0 else 0
            top_services.append({
                "service": service,
                "cost": float(cost),
                "percentage": float(percentage)
            })
        
        return top_services
    
    async def _detect_anomalies(
        self,
        tenant_id: str,
        current_cost: Decimal
    ) -> List[Dict[str, Any]]:
        """Detect cost anomalies"""
        
        # Get historical costs for anomaly detection
        # This is simplified - in production, use proper anomaly detection
        anomalies = []
        
        # Get average cost over last 30 days
        avg_cost = await self._get_average_cost(tenant_id, days=30)
        
        if avg_cost > 0:
            deviation = abs(float(current_cost - avg_cost) / float(avg_cost))
            
            if deviation > self.settings.anomaly_detection_threshold:
                anomalies.append({
                    "type": "cost_spike" if current_cost > avg_cost else "cost_drop",
                    "severity": "high" if deviation > 3 else "medium",
                    "current_value": float(current_cost),
                    "expected_value": float(avg_cost),
                    "deviation_percent": deviation * 100
                })
        
        return anomalies
    
    async def _get_average_cost(self, tenant_id: str, days: int) -> Decimal:
        """Get average daily cost over period"""
        
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        
        cost_data = await self.aggregator.get_cloudkitty_summary(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            group_by=["service"]
        )
        
        total_cost = sum(
            Decimal(str(item.get("rating", 0)))
            for item in cost_data.get("results", [])
        )
        
        return total_cost / days if days > 0 else Decimal('0')
    
    async def _generate_forecast(
        self,
        tenant_id: str,
        current_cost: Decimal
    ) -> Dict[str, Any]:
        """Generate cost forecast"""
        
        # Simple linear forecast - in production, use ML models
        avg_daily_cost = await self._get_average_cost(tenant_id, days=30)
        
        forecast = {
            "next_day": float(avg_daily_cost),
            "next_week": float(avg_daily_cost * 7),
            "next_month": float(avg_daily_cost * 30),
            "confidence": 0.75,  # Placeholder
            "method": "linear_regression"
        }
        
        return forecast
    
    async def get_reseller_summary(
        self,
        reseller_id: str,
        start_date: datetime,
        end_date: datetime,
        group_by_customer: bool = True
    ) -> Dict[str, Any]:
        """Get cost summary for reseller"""
        
        cost_data = await self.aggregator.get_hierarchical_costs(
            hierarchy_level="reseller",
            entity_id=reseller_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Process and aggregate by customer if requested
        summary = {
            "reseller_id": reseller_id,
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "total_cost": Decimal('0'),
            "customer_costs": {},
            "service_breakdown": {}
        }
        
        for item in cost_data.get("results", []):
            cost = Decimal(str(item.get("rating", 0)))
            summary["total_cost"] += cost
            
            if group_by_customer:
                customer_id = item.get("customer_id", "unknown")
                if customer_id not in summary["customer_costs"]:
                    summary["customer_costs"][customer_id] = Decimal('0')
                summary["customer_costs"][customer_id] += cost
            
            service = item.get("service", "unknown")
            if service not in summary["service_breakdown"]:
                summary["service_breakdown"][service] = Decimal('0')
            summary["service_breakdown"][service] += cost
        
        return summary
    
    async def get_customer_summary(
        self,
        customer_id: str,
        start_date: datetime,
        end_date: datetime,
        group_by_tenant: bool = True
    ) -> Dict[str, Any]:
        """Get cost summary for customer"""
        
        cost_data = await self.aggregator.get_hierarchical_costs(
            hierarchy_level="customer",
            entity_id=customer_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Process and aggregate by tenant if requested
        summary = {
            "customer_id": customer_id,
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "total_cost": Decimal('0'),
            "tenant_costs": {},
            "service_breakdown": {}
        }
        
        for item in cost_data.get("results", []):
            cost = Decimal(str(item.get("rating", 0)))
            summary["total_cost"] += cost
            
            if group_by_tenant:
                tenant_id = item.get("tenant_id", "unknown")
                if tenant_id not in summary["tenant_costs"]:
                    summary["tenant_costs"][tenant_id] = Decimal('0')
                summary["tenant_costs"][tenant_id] += cost
            
            service = item.get("service", "unknown")
            if service not in summary["service_breakdown"]:
                summary["service_breakdown"][service] = Decimal('0')
            summary["service_breakdown"][service] += cost
        
        return summary
    
    async def export_cost_data(
        self,
        tenant_id: str,
        start_date: datetime,
        end_date: datetime,
        format: str = "csv"
    ) -> Any:
        """Export cost data in various formats"""
        
        # Get detailed cost data
        cost_data = await self.get_cost_breakdown(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date
        )
        
        if format == "csv":
            # Convert to CSV
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow([
                "Date", "Service", "Resource Type", "Plan",
                "Quantity", "Unit Cost", "Total Cost"
            ])
            
            # Data rows
            for detail in cost_data.details:
                writer.writerow([
                    start_date.date(),
                    detail["service"],
                    detail["resource_type"],
                    detail["plan"],
                    detail["quantity"],
                    detail["unit_cost"],
                    detail["total_cost"]
                ])
            
            return output.getvalue()
        
        elif format == "json":
            return cost_data.dict()
        
        elif format == "xlsx":
            # Would use openpyxl or similar
            raise NotImplementedError("Excel export not yet implemented")
        
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    async def close(self):
        """Close database connections"""
        if self.cluster:
            self.cluster.shutdown() 