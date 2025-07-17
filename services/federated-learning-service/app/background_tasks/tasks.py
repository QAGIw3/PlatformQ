import asyncio
import time
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# This is a temporary solution. In the next steps, the coordinator will be
# refactored into smaller services and injected here.
from .main import coordinator

async def monitor_and_start_session(session_id: str, min_participants: int, timeout_seconds: int):
    """Monitor session and start when enough participants join"""
    start_time = time.time()
    
    while time.time() - start_time < timeout_seconds:
        # Get session data
        cache = coordinator.ignite_client.get_cache("fl_sessions")
        session_json = cache.get(session_id)
        
        if session_json:
            session_data = json.loads(session_json)
            
            if len(session_data["participants"]) >= min_participants:
                # Start first round
                await coordinator.start_training_round(session_id, 1)
                logger.info(f"Started federated learning session {session_id} with {len(session_data['participants'])} participants")
                return
        
        # Wait before checking again
        await asyncio.sleep(10)
    
    # Timeout - cancel session
    coordinator.update_session_in_ignite(session_id, {
        "status": "CANCELLED",
        "cancel_reason": "Not enough participants joined within timeout"
    })
    logger.warning(f"Cancelled session {session_id} due to timeout")


async def check_and_trigger_aggregation(session_id: str, round_number: int):
    """Check if all participants submitted and trigger aggregation"""
    # Get session data
    cache = coordinator.ignite_client.get_cache("fl_sessions")
    session_data = json.loads(cache.get(session_id))
    
    # Get submitted updates
    updates_cache = coordinator.ignite_client.get_cache(f"fl_model_updates_{session_id}")
    update_keys = [k for k in updates_cache.keys() if f"round{round_number}" in k]
    
    if len(update_keys) >= len(session_data["participants"]):
        # All participants submitted - trigger aggregation
        update_uris = []
        for key in update_keys:
            update_data = json.loads(updates_cache.get(key))
            update_uris.append(update_data["update_uri"])
        
        await coordinator.aggregate_model_updates(session_id, round_number, update_uris)
        
        # Publish round completed event after aggregation
        # (In production, this would be triggered by aggregation job completion)
        asyncio.create_task(publish_round_completed(session_id, round_number))


async def publish_round_completed(session_id: str, round_number: int):
    """Publish round completed event"""
    # Wait for aggregation to complete (mock delay)
    await asyncio.sleep(30)
    
    # Get aggregated model info
    model_cache = coordinator.ignite_client.get_cache(f"fl_aggregated_models_{session_id}")
    model_data = json.loads(model_cache.get(f"aggregated_model_round_{round_number}"))
    
    # Get round metrics
    metrics_cache = coordinator.ignite_client.get_cache(f"fl_round_metrics_{session_id}")
    metrics_data = json.loads(metrics_cache.get(f"round_{round_number}_metrics"))
    
    # Publish event
    event_data = {
        "session_id": session_id,
        "round_number": round_number,
        "aggregated_model_uri": model_data["aggregated_model_uri"],
        "participants": [
            {
                "participant_id": p,
                "contribution_weight": 1.0 / metrics_data["num_participants"],
                "update_received": True
            }
            for p in json.loads(coordinator.ignite_client.get_cache("fl_sessions").get(session_id))["participants"]
        ],
        "aggregation_metrics": {
            "aggregation_method": "FedAvg",
            "total_samples": model_data["aggregated_weights"]["total_samples"],
            "avg_loss": metrics_data["participant_metrics"].get("avg_loss", 0),
            "model_divergence": metrics_data["convergence_metrics"].get("model_divergence"),
            "convergence_score": metrics_data["convergence_metrics"].get("convergence_score")
        },
        "verifiable_credential_id": None,  # Will be set after VC issuance
        "next_round_start": int((time.time() + 60) * 1000) if round_number < 10 else None,
        "timestamp": int(time.time() * 1000)
    }
    
    coordinator.event_publisher.publish(
        topic=f"persistent://platformq/{session_id}/federated-round-completed",
        schema_path="federated_round_completed.avsc",
        data=event_data
    )
    
    # Update session status
    session_data = json.loads(coordinator.ignite_client.get_cache("fl_sessions").get(session_id))
    
    if round_number < session_data["total_rounds"]:
        # Start next round
        await coordinator.start_training_round(session_id, round_number + 1)
    else:
        # Session completed
        coordinator.update_session_in_ignite(session_id, {
            "status": "COMPLETED",
            "completed_at": datetime.utcnow().isoformat()
        }) 