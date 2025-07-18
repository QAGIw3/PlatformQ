"""
Transaction Monitoring API endpoints for Compliance Service.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, date
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from platformq_shared import get_current_user
from ..services.transaction_monitor import TransactionMonitor

logger = logging.getLogger(__name__)

router = APIRouter()


class TransactionAlert(BaseModel):
    """Transaction monitoring alert"""
    alert_id: str
    transaction_id: str
    alert_type: str
    severity: str  # low, medium, high, critical
    description: str
    rule_triggered: str
    created_at: datetime
    status: str  # new, investigating, resolved, escalated


class MonitoringRule(BaseModel):
    """Transaction monitoring rule"""
    rule_name: str = Field(..., description="Name of the rule")
    rule_type: str = Field(..., description="Type of rule (velocity, amount, pattern)")
    parameters: Dict[str, Any] = Field(..., description="Rule parameters")
    enabled: bool = Field(True, description="Whether rule is active")
    severity: str = Field("medium", description="Alert severity")


def get_transaction_monitor(request) -> TransactionMonitor:
    """Dependency to get transaction monitor instance"""
    return request.app.state.transaction_monitor


@router.get("/alerts")
async def get_alerts(
    status: Optional[str] = Query(None, description="Filter by status"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    start_date: Optional[date] = Query(None, description="Start date"),
    end_date: Optional[date] = Query(None, description="End date"),
    limit: int = Query(100, le=1000),
    current_user: Dict = Depends(get_current_user),
    monitor: TransactionMonitor = Depends(get_transaction_monitor)
):
    """
    Get transaction monitoring alerts.
    
    Returns alerts based on filters. Compliance officers see all alerts,
    users see only their own.
    """
    try:
        # Determine scope based on role
        user_filter = None
        if "compliance_officer" not in current_user.get("roles", []):
            user_filter = current_user["id"]
        
        alerts = await monitor.get_alerts(
            user_id=user_filter,
            status=status,
            severity=severity,
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        
        return {
            "alerts": [
                {
                    "alert_id": a["id"],
                    "transaction_id": a["transaction_id"],
                    "alert_type": a["type"],
                    "severity": a["severity"],
                    "description": a["description"],
                    "rule_triggered": a["rule"],
                    "created_at": a["created_at"],
                    "status": a["status"]
                }
                for a in alerts
            ],
            "total": len(alerts)
        }
        
    except Exception as e:
        logger.error(f"Error getting alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/investigate")
async def investigate_alert(
    alert_id: str,
    notes: str,
    current_user: Dict = Depends(get_current_user),
    monitor: TransactionMonitor = Depends(get_transaction_monitor)
):
    """
    Mark an alert as under investigation.
    
    Only compliance officers can investigate alerts.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can investigate alerts"
            )
        
        result = await monitor.investigate_alert(
            alert_id=alert_id,
            investigator_id=current_user["id"],
            notes=notes
        )
        
        return {
            "alert_id": alert_id,
            "status": "investigating",
            "investigator": current_user["id"],
            "updated_at": result["updated_at"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error investigating alert: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/resolve")
async def resolve_alert(
    alert_id: str,
    resolution: str,
    false_positive: bool = False,
    current_user: Dict = Depends(get_current_user),
    monitor: TransactionMonitor = Depends(get_transaction_monitor)
):
    """
    Resolve a transaction monitoring alert.
    
    Only compliance officers can resolve alerts.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can resolve alerts"
            )
        
        result = await monitor.resolve_alert(
            alert_id=alert_id,
            resolver_id=current_user["id"],
            resolution=resolution,
            false_positive=false_positive
        )
        
        return {
            "alert_id": alert_id,
            "status": "resolved",
            "resolution": resolution,
            "false_positive": false_positive,
            "resolved_at": result["resolved_at"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resolving alert: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rules")
async def get_monitoring_rules(
    current_user: Dict = Depends(get_current_user),
    monitor: TransactionMonitor = Depends(get_transaction_monitor)
):
    """
    Get all transaction monitoring rules.
    
    Only accessible to compliance officers.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can view rules"
            )
        
        rules = await monitor.get_rules()
        
        return {
            "rules": rules,
            "total": len(rules)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rules")
async def create_monitoring_rule(
    rule: MonitoringRule,
    current_user: Dict = Depends(get_current_user),
    monitor: TransactionMonitor = Depends(get_transaction_monitor)
):
    """
    Create a new transaction monitoring rule.
    
    Only accessible to compliance officers.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can create rules"
            )
        
        result = await monitor.create_rule(
            rule_name=rule.rule_name,
            rule_type=rule.rule_type,
            parameters=rule.parameters,
            enabled=rule.enabled,
            severity=rule.severity,
            created_by=current_user["id"]
        )
        
        return {
            "rule_id": result["id"],
            "status": "created",
            "message": f"Rule '{rule.rule_name}' created successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/rules/{rule_id}")
async def update_monitoring_rule(
    rule_id: str,
    rule: MonitoringRule,
    current_user: Dict = Depends(get_current_user),
    monitor: TransactionMonitor = Depends(get_transaction_monitor)
):
    """
    Update a transaction monitoring rule.
    
    Only accessible to compliance officers.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can update rules"
            )
        
        result = await monitor.update_rule(
            rule_id=rule_id,
            updates=rule.dict(),
            updated_by=current_user["id"]
        )
        
        return {
            "rule_id": rule_id,
            "status": "updated",
            "message": f"Rule '{rule.rule_name}' updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_monitoring_statistics(
    days: int = Query(30, description="Number of days"),
    current_user: Dict = Depends(get_current_user),
    monitor: TransactionMonitor = Depends(get_transaction_monitor)
):
    """
    Get transaction monitoring statistics.
    
    Shows alert counts, patterns, and trends.
    """
    try:
        stats = await monitor.get_statistics(days=days)
        
        return {
            "period_days": days,
            "total_transactions": stats["total_transactions"],
            "total_alerts": stats["total_alerts"],
            "alerts_by_severity": stats["alerts_by_severity"],
            "alerts_by_type": stats["alerts_by_type"],
            "false_positive_rate": stats["false_positive_rate"],
            "average_resolution_time": stats["average_resolution_time"],
            "top_triggered_rules": stats["top_triggered_rules"]
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test-rule")
async def test_monitoring_rule(
    rule: MonitoringRule,
    test_transaction: Dict[str, Any],
    current_user: Dict = Depends(get_current_user),
    monitor: TransactionMonitor = Depends(get_transaction_monitor)
):
    """
    Test a monitoring rule against a sample transaction.
    
    Helps compliance officers validate rules before deployment.
    """
    try:
        if "compliance_officer" not in current_user.get("roles", []):
            raise HTTPException(
                status_code=403,
                detail="Only compliance officers can test rules"
            )
        
        result = await monitor.test_rule(
            rule=rule.dict(),
            transaction=test_transaction
        )
        
        return {
            "rule_name": rule.rule_name,
            "triggered": result["triggered"],
            "alert_details": result.get("alert_details"),
            "execution_time_ms": result["execution_time_ms"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error testing rule: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 