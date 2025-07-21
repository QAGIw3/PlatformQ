from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

from ..models.event_models import (
    BlockchainEvent, EventFilter, EventSubscription,
    EventStatistics, MonitorStatus, AlertRule,
    ContractABI, EventType
)
from ..core.event_processor import EventProcessor


router = APIRouter(prefix="/events", tags=["Event Monitoring"])
logger = logging.getLogger(__name__)


# Dependency to get event processor
async def get_event_processor() -> EventProcessor:
    """Get event processor instance"""
    from ..main import event_processor
    return event_processor


@router.get("/monitors/status", response_model=Dict[str, Dict[str, Any]])
async def get_all_monitor_statuses(
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Get status of all blockchain monitors
    
    Returns current block, sync status, and health for each chain
    """
    return await event_processor.get_all_monitor_statuses()


@router.get("/monitors/{chain}/status", response_model=Dict[str, Any])
async def get_monitor_status(
    chain: str,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Get status of a specific blockchain monitor
    """
    status = await event_processor.get_monitor_status(chain)
    if not status:
        raise HTTPException(status_code=404, detail=f"Monitor not found for chain: {chain}")
    return status


@router.post("/subscriptions", response_model=EventSubscription)
async def create_subscription(
    subscription: EventSubscription,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Create a new event subscription
    
    Subscribe to blockchain events with optional webhook notifications
    """
    try:
        created = await event_processor.create_subscription(subscription)
        logger.info(f"Created subscription {created.subscription_id}")
        return created
    except Exception as e:
        logger.error(f"Error creating subscription: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/subscriptions/{subscription_id}", response_model=EventSubscription)
async def get_subscription(
    subscription_id: str,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Get subscription details
    """
    subscription = await event_processor.get_subscription(subscription_id)
    if not subscription:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return subscription


@router.patch("/subscriptions/{subscription_id}", response_model=EventSubscription)
async def update_subscription(
    subscription_id: str,
    updates: Dict[str, Any],
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Update subscription configuration
    """
    updated = await event_processor.update_subscription(subscription_id, updates)
    if not updated:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return updated


@router.delete("/subscriptions/{subscription_id}")
async def delete_subscription(
    subscription_id: str,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Delete (deactivate) a subscription
    """
    deleted = await event_processor.delete_subscription(subscription_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Subscription not found")
    return {"message": "Subscription deleted"}


@router.get("/search", response_model=List[BlockchainEvent])
async def search_events(
    chain: Optional[str] = Query(None),
    contract_address: Optional[str] = Query(None),
    event_name: Optional[str] = Query(None),
    event_type: Optional[EventType] = Query(None),
    from_block: Optional[int] = Query(None),
    to_block: Optional[int] = Query(None),
    transaction_hash: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000)
):
    """
    Search for blockchain events
    
    Query events by various filters
    """
    # In production, would query from database
    # For now, return empty list
    events = []
    
    return events


@router.get("/{event_id}", response_model=BlockchainEvent)
async def get_event(
    event_id: str,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Get specific event by ID
    """
    # Would query from cache/database
    event_data = await event_processor.event_cache.get(event_id)
    if not event_data:
        raise HTTPException(status_code=404, detail="Event not found")
    
    import json
    return BlockchainEvent(**json.loads(event_data))


@router.get("/statistics/summary", response_model=Dict[str, EventStatistics])
async def get_event_statistics(
    chain: Optional[str] = Query(None),
    period_hours: int = Query(24, ge=1, le=168)
):
    """
    Get event statistics
    
    Returns event counts, types, and webhook delivery stats
    """
    # In production, would aggregate from database
    stats = {}
    
    period_start = datetime.utcnow() - timedelta(hours=period_hours)
    period_end = datetime.utcnow()
    
    # Example statistics
    if chain:
        chains = [chain]
    else:
        chains = ["ethereum", "polygon"]  # Example chains
    
    for chain_name in chains:
        stats[chain_name] = EventStatistics(
            chain=chain_name,
            period_start=period_start,
            period_end=period_end,
            total_events=0,
            events_by_type={},
            events_by_status={},
            events_by_contract={},
            webhooks_sent=0,
            webhooks_successful=0,
            webhooks_failed=0,
            average_processing_time_ms=0.0,
            blocks_processed=0
        )
    
    return stats


@router.post("/alerts", response_model=AlertRule)
async def create_alert_rule(
    rule: AlertRule,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Create an alert rule
    
    Define conditions that trigger alerts when events match
    """
    try:
        # Save to cache
        await event_processor.alert_cache.put(rule.rule_id, rule.json())
        logger.info(f"Created alert rule {rule.rule_id}")
        return rule
    except Exception as e:
        logger.error(f"Error creating alert rule: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/alerts/{rule_id}", response_model=AlertRule)
async def get_alert_rule(
    rule_id: str,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Get alert rule details
    """
    import json
    rule_data = await event_processor.alert_cache.get(rule_id)
    if not rule_data:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    
    return AlertRule(**json.loads(rule_data))


@router.delete("/alerts/{rule_id}")
async def delete_alert_rule(
    rule_id: str,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Delete an alert rule
    """
    import json
    rule_data = await event_processor.alert_cache.get(rule_id)
    if not rule_data:
        raise HTTPException(status_code=404, detail="Alert rule not found")
    
    # Mark as inactive
    rule = AlertRule(**json.loads(rule_data))
    rule.is_active = False
    await event_processor.alert_cache.put(rule_id, rule.json())
    
    return {"message": "Alert rule deleted"}


@router.post("/contracts/abi", response_model=ContractABI)
async def register_contract_abi(
    abi: ContractABI,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Register contract ABI for event decoding
    
    Store ABI to enable automatic event decoding
    """
    # In production, would save to database
    # For now, add to monitor's ABI cache
    monitor = event_processor.monitors.get(abi.chain)
    if monitor:
        await monitor.load_contract_abi(abi.contract_address, abi.abi)
    
    return abi


@router.get("/contracts/{chain}/{address}/abi", response_model=ContractABI)
async def get_contract_abi(
    chain: str,
    address: str,
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Get contract ABI
    """
    monitor = event_processor.monitors.get(chain)
    if not monitor:
        raise HTTPException(status_code=404, detail=f"Chain not supported: {chain}")
    
    contract_abi = await monitor.load_contract_abi(address)
    if not contract_abi.abi:
        raise HTTPException(status_code=404, detail="ABI not found")
    
    return contract_abi


@router.post("/webhooks/test")
async def test_webhook(
    url: str,
    secret: Optional[str] = None,
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Test webhook endpoint
    
    Send a test event to verify webhook configuration
    """
    # Create test event
    test_event = BlockchainEvent(
        event_id="test-event-001",
        chain="ethereum",
        block_number=1000000,
        block_hash="0x" + "0" * 64,
        transaction_hash="0x" + "1" * 64,
        transaction_index=0,
        log_index=0,
        contract_address="0x" + "2" * 40,
        event_name="Test",
        event_type=EventType.CUSTOM,
        topics=[],
        data="0x",
        timestamp=datetime.utcnow(),
        decoded_data={"test": True, "message": "This is a test event"}
    )
    
    # Send test webhook
    async def send_test():
        import httpx
        import json
        import hmac
        import hashlib
        
        payload = {
            "event": test_event.dict(),
            "subscription_id": "test-subscription",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        headers = {"Content-Type": "application/json"}
        
        if secret:
            payload_bytes = json.dumps(payload, sort_keys=True).encode()
            signature = hmac.new(
                secret.encode(),
                payload_bytes,
                hashlib.sha256
            ).hexdigest()
            headers["X-Webhook-Signature"] = f"sha256={signature}"
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=payload, headers=headers, timeout=30)
                logger.info(f"Test webhook sent: {response.status_code}")
            except Exception as e:
                logger.error(f"Test webhook failed: {e}")
    
    background_tasks.add_task(send_test)
    
    return {
        "message": "Test webhook queued",
        "test_event": test_event.dict()
    }


@router.get("/health")
async def health_check(
    event_processor: EventProcessor = Depends(get_event_processor)
):
    """
    Health check endpoint
    
    Returns service health and monitor statuses
    """
    monitor_health = {}
    
    for chain, monitor in event_processor.monitors.items():
        status = monitor.get_status()
        monitor_health[chain] = {
            "active": status.is_active,
            "synced": status.is_synced,
            "blocks_behind": status.blocks_behind
        }
    
    return {
        "status": "healthy",
        "monitors": monitor_health,
        "event_queue_size": event_processor.event_queue.qsize()
    } 