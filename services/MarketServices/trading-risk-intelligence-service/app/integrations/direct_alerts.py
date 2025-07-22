"""Direct communication for critical risk alerts"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from platformq_direct_comm import DirectCommunicator, MessageType
from platformq_direct_comm.exceptions import CommunicationError

logger = logging.getLogger(__name__)


class DirectAlertsIntegration:
    """
    Lightweight direct communication for broadcasting critical systemic risk alerts
    Uses enhanced features for reliability and performance
    """
    
    # Custom message types for systemic risk
    SYSTEMIC_RISK_ALERT = 2001
    CASCADE_WARNING = 2002
    CLUSTER_RISK_ALERT = 2003
    NETWORK_ANOMALY = 2004
    
    def __init__(self, service_id: str, ignite_client):
        self.service_id = service_id
        self.communicator = DirectCommunicator(
            service_id=service_id,
            ignite_client=ignite_client,
            batch_size=50,  # Smaller batch for alerts
            process_interval_ms=1.0,  # 1ms check interval
            enable_circuit_breaker=True,  # Critical for resilience
            enable_batching=False,  # Alerts should not be batched
            enable_compression=True,  # Compress large network data
            enable_replay=True  # Ensure critical alerts are delivered
        )
        
    async def start(self):
        """Start direct communication for alerts"""
        await self.communicator.start()
        logger.info("Direct alerts integration started with enhanced reliability")
        
    async def stop(self):
        """Stop direct communication"""
        await self.communicator.stop()
        
    async def broadcast_systemic_risk_alert(self,
                                          risk_data: Dict[str, Any],
                                          affected_traders: List[str],
                                          severity: str = "high") -> bool:
        """
        Broadcast systemic risk alert to all relevant services
        Critical for immediate risk mitigation
        """
        try:
            message_id = f"systemic_risk_{datetime.utcnow().timestamp()}"
            
            alert_data = {
                "alert_type": "systemic_risk",
                "severity": severity,
                "affected_traders": affected_traders,
                "risk_metrics": risk_data,
                "timestamp": datetime.utcnow().isoformat(),
                "source": self.service_id,
                "_message_id": message_id  # For replay acknowledgment
            }
            
            # Determine priority based on severity
            priority = 3 if severity == "critical" else 2 if severity == "high" else 1
            
            # Send to each service with replay guarantee for critical alerts
            services = ["risk-engine", "risk-management", "trading-platform"]
            
            for service in services:
                await self.communicator.send_direct(
                    target_service=service,
                    msg_type=self.SYSTEMIC_RISK_ALERT,
                    data=alert_data,
                    wait_response=False,
                    priority=priority,
                    ttl_ms=300000,  # Valid for 5 minutes
                    require_ack=(severity == "critical")  # Replay critical alerts
                )
            
            logger.info(f"Systemic risk alert broadcast: {len(affected_traders)} traders affected")
            return True
            
        except Exception as e:
            logger.error(f"Failed to broadcast systemic risk alert: {e}")
            return False
            
    async def send_cascade_warning(self,
                                 source_trader: str,
                                 cascade_path: List[List[str]],
                                 estimated_impact: Dict[str, Any]) -> bool:
        """
        Send cascade failure warning for immediate intervention
        Uses message replay to ensure delivery
        """
        try:
            message_id = f"cascade_{source_trader}_{datetime.utcnow().timestamp()}"
            
            warning_data = {
                "warning_type": "cascade_failure",
                "source_trader": source_trader,
                "cascade_paths": cascade_path[:5],  # Top 5 paths
                "total_exposure": estimated_impact.get("total_exposure"),
                "affected_count": estimated_impact.get("affected_count"),
                "cascade_depth": estimated_impact.get("cascade_depth"),
                "timestamp": datetime.utcnow().isoformat(),
                "_message_id": message_id
            }
            
            # Critical priority for cascade warnings with replay
            await self.communicator.send_direct(
                target_service="risk-engine",
                msg_type=self.CASCADE_WARNING,
                data=warning_data,
                wait_response=False,
                priority=3,
                ttl_ms=30000,  # Valid for 30 seconds
                require_ack=True  # Ensure delivery via replay
            )
            
            # Also alert trading platform
            await self.communicator.send_direct(
                target_service="trading-platform",
                msg_type=self.CASCADE_WARNING,
                data=warning_data,
                wait_response=False,
                priority=3,
                require_ack=True
            )
            
            logger.warning(f"Cascade warning sent for trader {source_trader}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send cascade warning: {e}")
            return False
            
    async def alert_risk_cluster(self,
                               cluster_id: int,
                               cluster_traders: List[str],
                               cluster_risk_score: float) -> bool:
        """
        Alert about high-risk trader clusters
        Uses compression for large cluster data
        """
        try:
            if cluster_risk_score < 0.7:  # Only alert for high-risk clusters
                return True
                
            alert_data = {
                "cluster_id": cluster_id,
                "traders": cluster_traders,  # Will be compressed if large
                "risk_score": cluster_risk_score,
                "size": len(cluster_traders),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Send to risk management for monitoring
            await self.communicator.send_direct(
                target_service="risk-management",
                msg_type=self.CLUSTER_RISK_ALERT,
                data=alert_data,
                wait_response=False,
                priority=2
            )
            
            logger.info(f"Risk cluster alert sent: {len(cluster_traders)} traders, score {cluster_risk_score}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send cluster alert: {e}")
            return False
            
    async def notify_network_anomaly(self,
                                   anomaly_type: str,
                                   metrics: Dict[str, Any],
                                   recommended_actions: List[str]) -> bool:
        """
        Notify about anomalies in the trading network
        Broadcasts to all services for coordinated response
        """
        try:
            notification = {
                "anomaly_type": anomaly_type,
                "metrics": metrics,
                "recommended_actions": recommended_actions,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Broadcast anomaly to all services
            await self.communicator.broadcast(
                msg_type=self.NETWORK_ANOMALY,
                data=notification,
                target_services={"risk-engine", "risk-management", "trading-platform"}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to notify network anomaly: {e}")
            return False
            
    def get_stats(self) -> Dict[str, Any]:
        """Get alert broadcasting statistics with circuit breaker info"""
        stats = self.communicator.get_stats()
        
        # Log circuit breaker status for monitoring
        if "circuit_breakers" in stats:
            for service, cb_stats in stats["circuit_breakers"].items():
                if cb_stats["state"] != "closed":
                    logger.warning(
                        f"Circuit breaker for {service} is {cb_stats['state']}: "
                        f"{cb_stats['consecutive_failures']} failures"
                    )
                    
        return stats 