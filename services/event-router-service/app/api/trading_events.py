from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, Field
from enum import Enum

from ..core.event_router import EventRouter
from ..core.blockchain_event_handler import BlockchainEventHandler
from ..core.dlq_monitor import DLQMonitor
from ..schemas.event_schemas import BaseEvent


router = APIRouter(prefix="/trading", tags=["trading-events"])


class TradingEventType(str, Enum):
    """Trading event types"""
    TRADE_EXECUTED = "trade.executed"
    POSITION_UPDATED = "position.updated"
    RISK_ALERT = "risk.alert"
    MARGIN_CALL = "margin.call"
    LIQUIDATION_TRIGGERED = "liquidation.triggered"
    MARKET_ORDER_PLACED = "market.order.placed"
    LIMIT_ORDER_PLACED = "limit.order.placed"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_FILLED = "order.filled"
    STOP_LOSS_TRIGGERED = "stop_loss.triggered"
    PORTFOLIO_REBALANCED = "portfolio.rebalanced"
    STRATEGY_SIGNAL = "strategy.signal"


class RiskLevel(str, Enum):
    """Risk alert levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TradingEvent(BaseModel):
    """Trading event model"""
    event_type: TradingEventType
    trader_id: str = Field(..., description="Trader ID")
    market_id: str = Field(..., description="Market identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = Field(..., description="Event-specific data")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class TradeExecutedEvent(TradingEvent):
    """Trade execution event"""
    event_type: TradingEventType = TradingEventType.TRADE_EXECUTED
    data: Dict[str, Any] = Field(..., description="Must include: price, quantity, side, order_id")


class PositionUpdatedEvent(TradingEvent):
    """Position update event"""
    event_type: TradingEventType = TradingEventType.POSITION_UPDATED
    data: Dict[str, Any] = Field(..., description="Must include: position_size, entry_price, pnl")


class RiskAlertEvent(TradingEvent):
    """Risk alert event"""
    event_type: TradingEventType = TradingEventType.RISK_ALERT
    data: Dict[str, Any] = Field(..., description="Must include: risk_level, risk_metrics, alert_type")


class TradingEventRouter:
    """Routes trading events to appropriate processing pipelines"""
    
    def __init__(self, event_router: EventRouter, dlq_monitor: DLQMonitor):
        self.event_router = event_router
        self.dlq_monitor = dlq_monitor
        
        # Define trading event routing rules
        self.routing_rules = {
            TradingEventType.TRADE_EXECUTED: [
                "persistent://platformq/trading/trade-analytics",
                "persistent://platformq/trading/risk-assessment",
                "persistent://platformq/trading/compliance-monitoring"
            ],
            TradingEventType.POSITION_UPDATED: [
                "persistent://platformq/trading/portfolio-tracking",
                "persistent://platformq/trading/margin-monitoring"
            ],
            TradingEventType.RISK_ALERT: [
                "persistent://platformq/trading/risk-management",
                "persistent://platformq/trading/alert-processing"
            ],
            TradingEventType.LIQUIDATION_TRIGGERED: [
                "persistent://platformq/trading/liquidation-processing",
                "persistent://platformq/trading/emergency-alerts"
            ]
        }
        
    async def route_trading_event(self, event: TradingEvent) -> Dict[str, Any]:
        """Route trading event to appropriate topics"""
        topics = self.routing_rules.get(event.event_type, [])
        
        # Enrich event with routing metadata
        enriched_event = {
            **event.dict(),
            "routing_metadata": {
                "routed_at": datetime.utcnow().isoformat(),
                "target_topics": topics,
                "priority": self._get_event_priority(event)
            }
        }
        
        # Route to each target topic
        results = []
        for topic in topics:
            try:
                result = await self.event_router.route_event(
                    source_topic="trading.events",
                    event_data=enriched_event,
                    target_topic=topic
                )
                results.append({"topic": topic, "status": "success", "result": result})
            except Exception as e:
                results.append({"topic": topic, "status": "failed", "error": str(e)})
                
        return {
            "event_id": enriched_event.get("event_id"),
            "routing_results": results,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def _get_event_priority(self, event: TradingEvent) -> str:
        """Determine event priority based on type and content"""
        if event.event_type == TradingEventType.LIQUIDATION_TRIGGERED:
            return "critical"
        elif event.event_type == TradingEventType.RISK_ALERT:
            risk_level = event.data.get("risk_level", "medium")
            return "high" if risk_level in ["high", "critical"] else "medium"
        elif event.event_type == TradingEventType.TRADE_EXECUTED:
            # Large trades get higher priority
            quantity = Decimal(str(event.data.get("quantity", 0)))
            return "high" if quantity > 10000 else "normal"
        return "normal"


def get_trading_router(request) -> TradingEventRouter:
    """Get trading event router instance"""
    return request.app.state.trading_event_router


@router.post("/events")
async def submit_trading_event(
    event: TradingEvent,
    background_tasks: BackgroundTasks,
    trading_router: TradingEventRouter = Depends(get_trading_router)
):
    """Submit a trading event for routing and processing"""
    try:
        # Validate event based on type
        if event.event_type == TradingEventType.TRADE_EXECUTED:
            required_fields = ["price", "quantity", "side", "order_id"]
            if not all(field in event.data for field in required_fields):
                raise HTTPException(
                    status_code=400,
                    detail=f"Trade executed event must include: {required_fields}"
                )
        
        # Route event
        result = await trading_router.route_trading_event(event)
        
        # Schedule background monitoring
        background_tasks.add_task(
            monitor_event_processing,
            event.dict(),
            result
        )
        
        return {
            "status": "accepted",
            "event_type": event.event_type,
            "routing_result": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/events/batch")
async def submit_trading_events_batch(
    events: List[TradingEvent],
    background_tasks: BackgroundTasks,
    trading_router: TradingEventRouter = Depends(get_trading_router)
):
    """Submit multiple trading events in batch"""
    results = []
    
    for event in events:
        try:
            result = await trading_router.route_trading_event(event)
            results.append({
                "event": event.dict(),
                "status": "success",
                "result": result
            })
        except Exception as e:
            results.append({
                "event": event.dict(),
                "status": "failed",
                "error": str(e)
            })
    
    # Monitor batch processing
    background_tasks.add_task(
        monitor_batch_processing,
        results
    )
    
    return {
        "batch_size": len(events),
        "successful": sum(1 for r in results if r["status"] == "success"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "results": results
    }


@router.get("/event-mappings")
async def get_trading_event_mappings(
    trading_router: TradingEventRouter = Depends(get_trading_router)
):
    """Get current trading event routing mappings"""
    return {
        "mappings": trading_router.routing_rules,
        "supported_events": [e.value for e in TradingEventType]
    }


@router.put("/event-mappings/{event_type}")
async def update_trading_event_mapping(
    event_type: TradingEventType,
    topics: List[str],
    trading_router: TradingEventRouter = Depends(get_trading_router)
):
    """Update routing for a specific trading event type"""
    trading_router.routing_rules[event_type] = topics
    
    return {
        "event_type": event_type,
        "updated_topics": topics,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/dlq/stats")
async def get_trading_dlq_stats(
    trading_router: TradingEventRouter = Depends(get_trading_router)
):
    """Get dead letter queue statistics for trading events"""
    trading_topics = [
        "persistent://platformq/trading/trade-analytics-dlq",
        "persistent://platformq/trading/risk-assessment-dlq",
        "persistent://platformq/trading/compliance-monitoring-dlq"
    ]
    
    stats = {}
    for topic in trading_topics:
        try:
            metrics = await trading_router.dlq_monitor.get_topic_metrics(topic)
            stats[topic] = metrics
        except Exception as e:
            stats[topic] = {"error": str(e)}
    
    return {
        "dlq_stats": stats,
        "timestamp": datetime.utcnow().isoformat()
    }


async def monitor_event_processing(event: Dict[str, Any], routing_result: Dict[str, Any]):
    """Monitor event processing in background"""
    # Implementation for monitoring event processing
    # This could check if events are successfully processed downstream
    pass


async def monitor_batch_processing(results: List[Dict[str, Any]]):
    """Monitor batch processing results"""
    # Implementation for monitoring batch processing
    # This could aggregate metrics and alert on high failure rates
    pass 