"""
Monitoring Integration

Connects performance tracking with automated retraining
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, List

from .performance_tracker import ModelPerformanceTracker
from .auto_retrainer import AutomatedRetrainer, RetrainingTrigger

logger = logging.getLogger(__name__)


class MonitoringIntegration:
    """Integrates performance monitoring with automated actions"""
    
    def __init__(self,
                 performance_tracker: ModelPerformanceTracker,
                 auto_retrainer: AutomatedRetrainer,
                 check_interval: int = 300):  # 5 minutes
        self.performance_tracker = performance_tracker
        self.auto_retrainer = auto_retrainer
        self.check_interval = check_interval
        self._monitoring_task = None
        self._monitored_models: Dict[str, Dict[str, Any]] = {}
        
    async def start(self):
        """Start the monitoring integration"""
        if self._monitoring_task:
            logger.warning("Monitoring integration already started")
            return
            
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Started monitoring integration")
        
    async def stop(self):
        """Stop the monitoring integration"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            self._monitoring_task = None
            logger.info("Stopped monitoring integration")
            
    def register_model_for_monitoring(self,
                                    model_name: str,
                                    version: str,
                                    tenant_id: str,
                                    retraining_policy: Dict[str, Any]):
        """Register a model for automated monitoring and retraining"""
        key = f"{tenant_id}:{model_name}:{version}"
        self._monitored_models[key] = {
            "model_name": model_name,
            "version": version,
            "tenant_id": tenant_id,
            "policy": retraining_policy,
            "last_check": None,
            "last_retrain": None
        }
        logger.info(f"Registered {key} for automated monitoring")
        
    def unregister_model(self, model_name: str, version: str, tenant_id: str):
        """Unregister a model from monitoring"""
        key = f"{tenant_id}:{model_name}:{version}"
        if key in self._monitored_models:
            del self._monitored_models[key]
            logger.info(f"Unregistered {key} from monitoring")
            
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while True:
            try:
                await asyncio.sleep(self.check_interval)
                await self._check_all_models()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                
    async def _check_all_models(self):
        """Check all registered models for performance issues"""
        for key, model_info in self._monitored_models.items():
            try:
                await self._check_model(key, model_info)
            except Exception as e:
                logger.error(f"Error checking model {key}: {e}")
                
    async def _check_model(self, key: str, model_info: Dict[str, Any]):
        """Check a single model for performance issues"""
        model_name = model_info["model_name"]
        version = model_info["version"]
        tenant_id = model_info["tenant_id"]
        
        # Get performance report
        report = await self.performance_tracker.get_performance_report(
            model_name=model_name,
            version=version,
            tenant_id=tenant_id
        )
        
        if report.get("status") == "no_data":
            return
            
        current_metrics = report["current_metrics"]
        model_info["last_check"] = datetime.utcnow()
        
        # First register the policy if not already done
        await self.auto_retrainer.register_retraining_policy(
            model_name=model_name,
            tenant_id=tenant_id,
            policy=model_info["policy"]
        )
        
        # Check if retraining is needed
        trigger = await self.auto_retrainer.evaluate_retraining_need(
            model_name=model_name,
            version=version,
            tenant_id=tenant_id,
            performance_metrics=current_metrics
        )
        
        if trigger:
            # Check if we recently retrained to avoid loops
            if model_info["last_retrain"]:
                time_since_retrain = (datetime.utcnow() - model_info["last_retrain"]).total_seconds()
                min_interval = model_info["policy"].get("min_retraining_interval_hours", 24) * 3600
                
                if time_since_retrain < min_interval:
                    logger.info(f"Skipping retraining for {key} - too soon since last retrain")
                    return
                    
            # Trigger retraining
            job_id = await self.auto_retrainer.trigger_retraining(
                model_name=model_name,
                version=version,
                tenant_id=tenant_id,
                trigger=trigger,
                trigger_metrics=current_metrics
            )
            
            model_info["last_retrain"] = datetime.utcnow()
            logger.info(f"Triggered retraining job {job_id} for {key} due to {trigger.value}")
            
            # Send notification
            await self._send_retraining_notification(
                model_name=model_name,
                version=version,
                tenant_id=tenant_id,
                trigger=trigger,
                job_id=job_id,
                metrics=current_metrics
            )
            
    async def _send_retraining_notification(self,
                                          model_name: str,
                                          version: str,
                                          tenant_id: str,
                                          trigger: RetrainingTrigger,
                                          job_id: str,
                                          metrics: Dict[str, Any]):
        """Send notification about retraining"""
        # In production, integrate with notification service
        logger.info(f"Notification: Model {model_name} v{version} retraining triggered - {trigger.value}")
        
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status"""
        return {
            "active": self._monitoring_task is not None,
            "monitored_models": len(self._monitored_models),
            "models": [
                {
                    "key": key,
                    "model_name": info["model_name"],
                    "version": info["version"],
                    "tenant_id": info["tenant_id"],
                    "last_check": info["last_check"].isoformat() if info["last_check"] else None,
                    "last_retrain": info["last_retrain"].isoformat() if info["last_retrain"] else None
                }
                for key, info in self._monitored_models.items()
            ]
        }
        
    async def force_check(self, model_name: str, version: str, tenant_id: str):
        """Force an immediate check for a specific model"""
        key = f"{tenant_id}:{model_name}:{version}"
        if key in self._monitored_models:
            await self._check_model(key, self._monitored_models[key])
            return True
        return False 