"""Alert manager for real-time risk notifications."""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional, Set
from enum import Enum
from collections import defaultdict

from platformq_risk_common import RiskAlert, RiskMetric

logger = logging.getLogger(__name__)


class AlertChannel(Enum):
    """Alert delivery channels."""
    EMAIL = "email"
    SMS = "sms"
    WEBHOOK = "webhook"
    PULSAR = "pulsar"
    SLACK = "slack"
    TELEGRAM = "telegram"


class AlertPriority(Enum):
    """Alert priority levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertManager:
    """Manages risk alerts and notifications."""
    
    def __init__(self, config: Dict[str, Any], event_publisher):
        self.config = config
        self.event_publisher = event_publisher
        
        # Alert configuration
        self.alert_rules = self._load_alert_rules()
        self.alert_history = defaultdict(list)
        self.active_alerts = {}
        self.alert_cooldowns = {}
        
        # Rate limiting
        self.rate_limits = {
            AlertPriority.LOW: 10,  # per hour
            AlertPriority.MEDIUM: 20,
            AlertPriority.HIGH: 50,
            AlertPriority.CRITICAL: 100  # No real limit for critical
        }
        
        # Alert subscriptions
        self.subscriptions = defaultdict(set)
        
    def _load_alert_rules(self) -> List[Dict[str, Any]]:
        """Load predefined alert rules."""
        return [
            {
                "rule_id": "margin_critical",
                "metric": RiskMetric.MARGIN_RATIO,
                "condition": "less_than",
                "threshold": 1.2,
                "priority": AlertPriority.CRITICAL,
                "channels": [AlertChannel.EMAIL, AlertChannel.PULSAR, AlertChannel.SLACK],
                "message_template": "Critical margin ratio {value:.2f} for {entity}"
            },
            {
                "rule_id": "margin_warning",
                "metric": RiskMetric.MARGIN_RATIO,
                "condition": "less_than",
                "threshold": 1.5,
                "priority": AlertPriority.HIGH,
                "channels": [AlertChannel.EMAIL, AlertChannel.PULSAR],
                "message_template": "Low margin ratio {value:.2f} for {entity}"
            },
            {
                "rule_id": "leverage_high",
                "metric": RiskMetric.LEVERAGE,
                "condition": "greater_than",
                "threshold": 18,
                "priority": AlertPriority.HIGH,
                "channels": [AlertChannel.EMAIL, AlertChannel.PULSAR],
                "message_template": "High leverage {value:.1f}x for {entity}"
            },
            {
                "rule_id": "var_breach",
                "metric": RiskMetric.VALUE_AT_RISK,
                "condition": "greater_than",
                "threshold": 0.08,  # 8% VaR
                "priority": AlertPriority.HIGH,
                "channels": [AlertChannel.EMAIL, AlertChannel.PULSAR],
                "message_template": "VaR breach {value:.1%} for {entity}"
            },
            {
                "rule_id": "concentration_high",
                "metric": RiskMetric.CONCENTRATION_RISK,
                "condition": "greater_than",
                "threshold": 0.5,
                "priority": AlertPriority.MEDIUM,
                "channels": [AlertChannel.EMAIL],
                "message_template": "High concentration {value:.1%} in {entity}"
            }
        ]
    
    async def process_risk_update(
        self,
        entity_id: str,
        entity_type: str,  # user, position, portfolio
        metrics: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Process risk metrics update and generate alerts."""
        generated_alerts = []
        
        for rule in self.alert_rules:
            # Check if rule applies
            metric_name = rule["metric"]
            if metric_name.value not in metrics:
                continue
            
            metric_value = metrics[metric_name.value]
            threshold = rule["threshold"]
            
            # Evaluate condition
            triggered = self._evaluate_condition(
                metric_value,
                rule["condition"],
                threshold
            )
            
            if triggered:
                # Check cooldown
                alert_key = f"{entity_id}:{rule['rule_id']}"
                if self._is_in_cooldown(alert_key):
                    continue
                
                # Create alert
                alert = await self._create_alert(
                    entity_id,
                    entity_type,
                    rule,
                    metric_value
                )
                
                # Send alert
                await self._dispatch_alert(alert)
                
                # Record alert
                self._record_alert(alert)
                generated_alerts.append(alert)
        
        return generated_alerts
    
    def _evaluate_condition(
        self,
        value: Any,
        condition: str,
        threshold: Any
    ) -> bool:
        """Evaluate if condition is met."""
        try:
            value = Decimal(str(value))
            threshold = Decimal(str(threshold))
            
            if condition == "greater_than":
                return value > threshold
            elif condition == "less_than":
                return value < threshold
            elif condition == "equal_to":
                return value == threshold
            elif condition == "not_equal_to":
                return value != threshold
            else:
                return False
        except:
            return False
    
    async def _create_alert(
        self,
        entity_id: str,
        entity_type: str,
        rule: Dict[str, Any],
        metric_value: Any
    ) -> Dict[str, Any]:
        """Create an alert object."""
        alert_id = f"alert_{entity_id}_{rule['rule_id']}_{datetime.utcnow().timestamp()}"
        
        # Format message
        message = rule["message_template"].format(
            value=metric_value,
            entity=f"{entity_type} {entity_id}"
        )
        
        alert = {
            "alert_id": alert_id,
            "entity_id": entity_id,
            "entity_type": entity_type,
            "rule_id": rule["rule_id"],
            "metric": rule["metric"].value,
            "metric_value": str(metric_value),
            "threshold": str(rule["threshold"]),
            "priority": rule["priority"].value,
            "channels": [ch.value for ch in rule["channels"]],
            "message": message,
            "created_at": datetime.utcnow(),
            "status": "active"
        }
        
        return alert
    
    async def _dispatch_alert(self, alert: Dict[str, Any]) -> None:
        """Dispatch alert through configured channels."""
        channels = alert["channels"]
        
        # Publish to Pulsar event stream
        if AlertChannel.PULSAR.value in channels:
            await self.event_publisher.publish_event(
                "risk.alert.created",
                alert
            )
        
        # Email notification
        if AlertChannel.EMAIL.value in channels:
            await self._send_email_alert(alert)
        
        # Slack notification
        if AlertChannel.SLACK.value in channels:
            await self._send_slack_alert(alert)
        
        # Webhook notification
        if AlertChannel.WEBHOOK.value in channels:
            await self._send_webhook_alert(alert)
    
    async def _send_email_alert(self, alert: Dict[str, Any]) -> None:
        """Send email alert (mock implementation)."""
        logger.info(f"Email alert sent: {alert['message']}")
        # In production, integrate with email service
    
    async def _send_slack_alert(self, alert: Dict[str, Any]) -> None:
        """Send Slack alert (mock implementation)."""
        logger.info(f"Slack alert sent: {alert['message']}")
        # In production, integrate with Slack API
    
    async def _send_webhook_alert(self, alert: Dict[str, Any]) -> None:
        """Send webhook alert (mock implementation)."""
        logger.info(f"Webhook alert sent: {alert['message']}")
        # In production, send HTTP POST to configured webhooks
    
    def _record_alert(self, alert: Dict[str, Any]) -> None:
        """Record alert in history and active alerts."""
        alert_key = f"{alert['entity_id']}:{alert['rule_id']}"
        
        # Add to history
        self.alert_history[alert['entity_id']].append(alert)
        
        # Add to active alerts
        self.active_alerts[alert['alert_id']] = alert
        
        # Set cooldown
        priority = AlertPriority(alert['priority'])
        cooldown_minutes = {
            AlertPriority.LOW: 60,
            AlertPriority.MEDIUM: 30,
            AlertPriority.HIGH: 15,
            AlertPriority.CRITICAL: 5
        }.get(priority, 30)
        
        self.alert_cooldowns[alert_key] = datetime.utcnow() + timedelta(minutes=cooldown_minutes)
    
    def _is_in_cooldown(self, alert_key: str) -> bool:
        """Check if alert is in cooldown period."""
        if alert_key not in self.alert_cooldowns:
            return False
        
        return datetime.utcnow() < self.alert_cooldowns[alert_key]
    
    async def acknowledge_alert(
        self,
        alert_id: str,
        acknowledged_by: str,
        notes: Optional[str] = None
    ) -> bool:
        """Acknowledge an alert."""
        if alert_id not in self.active_alerts:
            return False
        
        alert = self.active_alerts[alert_id]
        alert["status"] = "acknowledged"
        alert["acknowledged_by"] = acknowledged_by
        alert["acknowledged_at"] = datetime.utcnow()
        if notes:
            alert["notes"] = notes
        
        # Publish acknowledgment event
        await self.event_publisher.publish_event(
            "risk.alert.acknowledged",
            {
                "alert_id": alert_id,
                "acknowledged_by": acknowledged_by,
                "notes": notes
            }
        )
        
        return True
    
    async def resolve_alert(
        self,
        alert_id: str,
        resolved_by: str,
        resolution: str
    ) -> bool:
        """Resolve an alert."""
        if alert_id not in self.active_alerts:
            return False
        
        alert = self.active_alerts[alert_id]
        alert["status"] = "resolved"
        alert["resolved_by"] = resolved_by
        alert["resolved_at"] = datetime.utcnow()
        alert["resolution"] = resolution
        
        # Remove from active alerts
        del self.active_alerts[alert_id]
        
        # Publish resolution event
        await self.event_publisher.publish_event(
            "risk.alert.resolved",
            {
                "alert_id": alert_id,
                "resolved_by": resolved_by,
                "resolution": resolution
            }
        )
        
        return True
    
    def get_active_alerts(
        self,
        entity_id: Optional[str] = None,
        priority: Optional[AlertPriority] = None
    ) -> List[Dict[str, Any]]:
        """Get active alerts with optional filters."""
        alerts = list(self.active_alerts.values())
        
        if entity_id:
            alerts = [a for a in alerts if a["entity_id"] == entity_id]
        
        if priority:
            alerts = [a for a in alerts if a["priority"] == priority.value]
        
        return sorted(alerts, key=lambda a: a["created_at"], reverse=True)
    
    def get_alert_history(
        self,
        entity_id: str,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get alert history for an entity."""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        history = self.alert_history.get(entity_id, [])
        return [
            alert for alert in history
            if alert["created_at"] > cutoff
        ]
    
    def subscribe_to_alerts(
        self,
        subscriber_id: str,
        entity_ids: List[str],
        channels: List[AlertChannel]
    ) -> None:
        """Subscribe to alerts for specific entities."""
        for entity_id in entity_ids:
            self.subscriptions[entity_id].add({
                "subscriber_id": subscriber_id,
                "channels": channels
            })
    
    def unsubscribe_from_alerts(
        self,
        subscriber_id: str,
        entity_ids: Optional[List[str]] = None
    ) -> None:
        """Unsubscribe from alerts."""
        if entity_ids:
            for entity_id in entity_ids:
                self.subscriptions[entity_id] = {
                    sub for sub in self.subscriptions[entity_id]
                    if sub["subscriber_id"] != subscriber_id
                }
        else:
            # Remove from all subscriptions
            for entity_id in self.subscriptions:
                self.subscriptions[entity_id] = {
                    sub for sub in self.subscriptions[entity_id]
                    if sub["subscriber_id"] != subscriber_id
                }
    
    async def test_alert(
        self,
        entity_id: str,
        entity_type: str,
        metric: RiskMetric,
        value: Decimal
    ) -> Dict[str, Any]:
        """Send a test alert."""
        test_alert = {
            "alert_id": f"test_alert_{datetime.utcnow().timestamp()}",
            "entity_id": entity_id,
            "entity_type": entity_type,
            "rule_id": "test",
            "metric": metric.value,
            "metric_value": str(value),
            "threshold": "N/A",
            "priority": AlertPriority.LOW.value,
            "channels": [AlertChannel.PULSAR.value],
            "message": f"Test alert for {entity_type} {entity_id}: {metric.value} = {value}",
            "created_at": datetime.utcnow(),
            "status": "test"
        }
        
        await self._dispatch_alert(test_alert)
        return test_alert 