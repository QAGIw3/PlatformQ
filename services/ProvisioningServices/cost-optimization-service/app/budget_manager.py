"""Budget Manager for tracking spending and generating alerts"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import asyncio

import pulsar
from pulsar.schema import JsonSchema

from platformq_cost_common import (
    Budget,
    BudgetAlert,
    BudgetStatus,
    CostAnalysis
)

from .config import settings
from .repository import CostRepository

logger = logging.getLogger(__name__)


class BudgetEventSchema(JsonSchema):
    """Schema for budget events"""
    event_type: str
    tenant_id: str
    budget_id: str
    budget_name: str
    current_spend: float
    budget_amount: float
    percentage_used: float
    alert_type: Optional[str] = None
    threshold_percentage: Optional[int] = None
    timestamp: str


class BudgetManager:
    """Manages budgets and alerts"""
    
    def __init__(self, repository: CostRepository):
        self.repository = repository
        self.pulsar_client = None
        self.alert_producer = None
        self._init_pulsar()
        
    def _init_pulsar(self):
        """Initialize Pulsar client and producer"""
        try:
            self.pulsar_client = pulsar.Client(settings.pulsar_url)
            
            self.alert_producer = self.pulsar_client.create_producer(
                topic=f"{settings.pulsar_topic_prefix}{settings.pulsar_budget_alerts_topic}",
                schema=BudgetEventSchema(),
                producer_name=f"{settings.service_name}-budget-alerts"
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize Pulsar: {e}")
            
    async def check_budgets(self, tenant_id: str, cost_analysis: CostAnalysis) -> List[BudgetAlert]:
        """Check budgets and generate alerts"""
        logger.info(f"Checking budgets for tenant {tenant_id}")
        
        # Get all active budgets for tenant
        budgets = await self.repository.get_budgets(tenant_id)
        alerts = []
        
        for budget in budgets:
            # Check if budget applies to current period
            if not self._is_budget_active(budget, cost_analysis.period_start):
                continue
                
            # Calculate spending for budget period
            current_spend = await self._calculate_budget_spend(
                budget,
                cost_analysis
            )
            
            # Calculate percentage used
            percentage_used = (current_spend / budget.amount * 100) if budget.amount > 0 else 0
            
            # Check alert thresholds
            for threshold in budget.alert_thresholds:
                if percentage_used >= threshold:
                    # Check if we already sent this alert
                    alert_sent = await self._check_alert_already_sent(
                        tenant_id,
                        budget.budget_id,
                        threshold
                    )
                    
                    if not alert_sent:
                        alert = BudgetAlert(
                            tenant_id=tenant_id,
                            budget_id=budget.budget_id,
                            budget_name=budget.name,
                            threshold_percentage=threshold,
                            current_spend=current_spend,
                            budget_amount=budget.amount,
                            alert_type=self._get_alert_type(threshold),
                            message=self._generate_alert_message(
                                budget,
                                current_spend,
                                percentage_used,
                                threshold
                            ),
                            triggered_at=datetime.now(timezone.utc)
                        )
                        
                        alerts.append(alert)
                        
                        # Save alert
                        await self.repository.save_budget_alert(alert)
                        
                        # Publish alert event
                        await self._publish_budget_alert(alert, percentage_used)
                        
        return alerts
        
    def _is_budget_active(self, budget: Budget, current_date: datetime) -> bool:
        """Check if budget is active for current period"""
        if budget.start_date and current_date.date() < budget.start_date:
            return False
            
        if budget.end_date and current_date.date() > budget.end_date:
            return False
            
        return True
        
    async def _calculate_budget_spend(
        self,
        budget: Budget,
        cost_analysis: CostAnalysis
    ) -> float:
        """Calculate spending for budget period"""
        # Get appropriate period based on budget configuration
        if budget.period == "daily":
            # Use today's costs
            return cost_analysis.total_cost
            
        elif budget.period == "weekly":
            # Get last 7 days of costs
            history = await self.repository.get_cost_history(
                tenant_id=budget.tenant_id,
                days=7
            )
            return sum(h.total_cost for h in history)
            
        elif budget.period == "monthly":
            # Get current month's costs
            history = await self.repository.get_cost_history(
                tenant_id=budget.tenant_id,
                days=30
            )
            return sum(h.total_cost for h in history)
            
        elif budget.period == "quarterly":
            # Get last 90 days of costs
            history = await self.repository.get_cost_history(
                tenant_id=budget.tenant_id,
                days=90
            )
            return sum(h.total_cost for h in history)
            
        elif budget.period == "yearly":
            # Get last 365 days of costs
            history = await self.repository.get_cost_history(
                tenant_id=budget.tenant_id,
                days=365
            )
            return sum(h.total_cost for h in history)
            
        else:
            # Custom period - use resource filters
            if budget.resource_filters:
                # Filter costs based on resource filters
                filtered_costs = []
                for cost in cost_analysis.resource_costs:
                    if self._matches_filters(cost, budget.resource_filters):
                        filtered_costs.append(cost)
                        
                return sum(c.amount for c in filtered_costs)
            else:
                return cost_analysis.total_cost
                
    def _matches_filters(self, cost, filters: Dict[str, str]) -> bool:
        """Check if cost matches budget filters"""
        for key, value in filters.items():
            if key == "resource_type" and cost.resource_type != value:
                return False
            elif key == "provider" and cost.provider != value:
                return False
            elif key in cost.tags and cost.tags[key] != value:
                return False
                
        return True
        
    async def _check_alert_already_sent(
        self,
        tenant_id: str,
        budget_id: str,
        threshold: int
    ) -> bool:
        """Check if alert was already sent for this threshold"""
        # In production, this would query the database
        # For now, return False to always send alerts
        return False
        
    def _get_alert_type(self, threshold: int) -> str:
        """Get alert type based on threshold"""
        if threshold >= 100:
            return "exceeded"
        elif threshold >= 90:
            return "critical"
        elif threshold >= 75:
            return "warning"
        else:
            return "info"
            
    def _generate_alert_message(
        self,
        budget: Budget,
        current_spend: float,
        percentage_used: float,
        threshold: int
    ) -> str:
        """Generate alert message"""
        if threshold >= 100:
            return (
                f"Budget '{budget.name}' has been exceeded! "
                f"Current spend: ${current_spend:.2f} ({percentage_used:.1f}% of ${budget.amount:.2f})"
            )
        else:
            return (
                f"Budget '{budget.name}' has reached {threshold}% threshold. "
                f"Current spend: ${current_spend:.2f} ({percentage_used:.1f}% of ${budget.amount:.2f})"
            )
            
    async def _publish_budget_alert(self, alert: BudgetAlert, percentage_used: float):
        """Publish budget alert to Pulsar"""
        if not self.alert_producer:
            return
            
        try:
            event = BudgetEventSchema(
                event_type="budget_alert",
                tenant_id=alert.tenant_id,
                budget_id=alert.budget_id,
                budget_name=alert.budget_name,
                current_spend=alert.current_spend,
                budget_amount=alert.budget_amount,
                percentage_used=percentage_used,
                alert_type=alert.alert_type,
                threshold_percentage=alert.threshold_percentage,
                timestamp=alert.triggered_at.isoformat()
            )
            
            self.alert_producer.send(event)
            logger.info(f"Published budget alert for {alert.budget_name}")
            
        except Exception as e:
            logger.error(f"Failed to publish budget alert: {e}")
            
    async def create_budget(
        self,
        tenant_id: str,
        name: str,
        amount: float,
        period: str,
        alert_thresholds: List[int],
        resource_filters: Optional[Dict[str, str]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> str:
        """Create a new budget"""
        budget = Budget(
            tenant_id=tenant_id,
            budget_id="",  # Will be generated by repository
            name=name,
            amount=amount,
            period=period,
            start_date=start_date.date() if start_date else None,
            end_date=end_date.date() if end_date else None,
            resource_filters=resource_filters or {},
            alert_thresholds=sorted(alert_thresholds),
            status=BudgetStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        
        budget_id = await self.repository.save_budget(budget)
        
        # Publish budget created event
        if self.alert_producer:
            event = BudgetEventSchema(
                event_type="budget_created",
                tenant_id=tenant_id,
                budget_id=budget_id,
                budget_name=name,
                current_spend=0,
                budget_amount=amount,
                percentage_used=0,
                timestamp=datetime.now(timezone.utc).isoformat()
            )
            
            self.alert_producer.send(event)
            
        return budget_id
        
    async def update_budget(
        self,
        tenant_id: str,
        budget_id: str,
        updates: Dict[str, Any]
    ) -> None:
        """Update an existing budget"""
        # In production, this would update the budget in the database
        pass
        
    async def delete_budget(self, tenant_id: str, budget_id: str) -> None:
        """Delete a budget"""
        # In production, this would delete the budget from the database
        pass
        
    async def get_budget_status(
        self,
        tenant_id: str,
        budget_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get current budget status"""
        # In production, this would return detailed budget status
        return None
        
    async def close(self):
        """Close connections"""
        if self.alert_producer:
            self.alert_producer.close()
        if self.pulsar_client:
            self.pulsar_client.close() 