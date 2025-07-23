"""
MLOps Manager

Manages ML model lifecycle, monitoring, and operations.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class ModelStage(Enum):
    """Model lifecycle stages"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class MLOpsManager:
    """
    Manages MLOps workflows and model lifecycle
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus,
                 model_registry: Any, model_monitor: Any, drift_detector: Any):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        self.model_registry = model_registry
        self.model_monitor = model_monitor
        self.drift_detector = drift_detector
        
        # Configuration
        self.config = {
            "monitoring": {
                "check_interval": 300,  # 5 minutes
                "drift_threshold": 0.1,
                "performance_threshold": 0.8,
                "alert_cooldown": 1800  # 30 minutes
            },
            "lifecycle": {
                "auto_promote": True,
                "staging_duration_days": 7,
                "min_staging_performance": 0.85,
                "archive_after_days": 90
            },
            "governance": {
                "require_approval": True,
                "approvers": ["ml-team", "data-team"],
                "audit_enabled": True
            }
        }
        
        # State tracking
        self.model_states: Dict[str, Dict[str, Any]] = {}
        self.alerts: Dict[str, List[Dict[str, Any]]] = {}
        
        # Metrics
        self.metrics = {
            "models_monitored": 0,
            "drift_detected": 0,
            "models_promoted": 0,
            "models_archived": 0,
            "alerts_triggered": 0
        }
    
    async def initialize(self):
        """Initialize MLOps manager"""
        logger.info("initializing_mlops_manager")
        
        # Load configuration
        await self._load_configuration()
        
        # Initialize components
        await self.model_monitor.initialize()
        await self.drift_detector.initialize()
        
        # Load existing models
        await self._load_model_states()
        
        # Start background tasks
        asyncio.create_task(self._monitor_models())
        asyncio.create_task(self._manage_lifecycle())
        asyncio.create_task(self._process_alerts())
        
        logger.info("mlops_manager_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.model_monitor.cleanup()
        await self.drift_detector.cleanup()
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/mlops-manager")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def _load_model_states(self):
        """Load model states from registry"""
        models = await self.model_registry.list_models()
        
        for model in models:
            model_id = model["id"]
            self.model_states[model_id] = {
                "model": model,
                "stage": ModelStage(model.get("stage", "development")),
                "monitoring": {
                    "enabled": model.get("stage") in ["staging", "production"],
                    "last_check": None,
                    "metrics": {},
                    "drift_score": 0.0
                },
                "alerts": [],
                "transitions": []
            }
        
        self.metrics["models_monitored"] = len([
            s for s in self.model_states.values() 
            if s["monitoring"]["enabled"]
        ])
    
    async def register_model(self, model_info: Dict[str, Any]) -> str:
        """
        Register a new model in MLOps
        
        Args:
            model_info: Model information including metadata, metrics, artifacts
            
        Returns:
            Model ID
        """
        # Register in model registry
        model = await self.model_registry.register_model(model_info)
        model_id = model["id"]
        
        # Create model state
        self.model_states[model_id] = {
            "model": model,
            "stage": ModelStage.DEVELOPMENT,
            "monitoring": {
                "enabled": False,
                "last_check": None,
                "metrics": model_info.get("metrics", {}),
                "drift_score": 0.0
            },
            "alerts": [],
            "transitions": [{
                "from": None,
                "to": ModelStage.DEVELOPMENT,
                "timestamp": datetime.utcnow(),
                "reason": "Initial registration"
            }]
        }
        
        # Emit event
        await self.event_bus.publish(
            "mlops.model.registered",
            {
                "model_id": model_id,
                "name": model_info.get("name"),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Model registered in MLOps: {model_id}")
        return model_id
    
    async def promote_model(self, model_id: str, target_stage: str, 
                          reason: str = None) -> bool:
        """
        Promote model to a new stage
        
        Args:
            model_id: Model ID
            target_stage: Target stage (staging, production)
            reason: Reason for promotion
            
        Returns:
            Success status
        """
        model_state = self.model_states.get(model_id)
        if not model_state:
            raise ValueError(f"Model not found: {model_id}")
        
        current_stage = model_state["stage"]
        target_stage_enum = ModelStage(target_stage)
        
        # Validate transition
        if not self._validate_transition(current_stage, target_stage_enum):
            raise ValueError(f"Invalid transition: {current_stage.value} -> {target_stage}")
        
        # Check governance requirements
        if self.config["governance"]["require_approval"]:
            approval = await self._check_approval(model_id, target_stage)
            if not approval:
                logger.warning(f"Promotion requires approval: {model_id}")
                return False
        
        # Perform promotion checks
        if target_stage_enum == ModelStage.PRODUCTION:
            if not await self._check_production_readiness(model_id):
                raise ValueError("Model not ready for production")
        
        # Update model stage
        model_state["stage"] = target_stage_enum
        model_state["transitions"].append({
            "from": current_stage,
            "to": target_stage_enum,
            "timestamp": datetime.utcnow(),
            "reason": reason or "Manual promotion"
        })
        
        # Enable monitoring for staging/production
        if target_stage_enum in [ModelStage.STAGING, ModelStage.PRODUCTION]:
            model_state["monitoring"]["enabled"] = True
            self.metrics["models_monitored"] += 1
        
        # Update in registry
        await self.model_registry.update_model_stage(model_id, target_stage)
        
        # Update metrics
        self.metrics["models_promoted"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "mlops.model.promoted",
            {
                "model_id": model_id,
                "from_stage": current_stage.value,
                "to_stage": target_stage,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Model promoted: {model_id} -> {target_stage}")
        return True
    
    async def get_model_status(self, model_id: str) -> Dict[str, Any]:
        """Get comprehensive model status"""
        model_state = self.model_states.get(model_id)
        if not model_state:
            raise ValueError(f"Model not found: {model_id}")
        
        # Get latest monitoring data
        monitoring_data = None
        if model_state["monitoring"]["enabled"]:
            monitoring_data = await self.model_monitor.get_model_metrics(model_id)
        
        return {
            "model_id": model_id,
            "stage": model_state["stage"].value,
            "monitoring": {
                "enabled": model_state["monitoring"]["enabled"],
                "last_check": model_state["monitoring"]["last_check"],
                "drift_score": model_state["monitoring"]["drift_score"],
                "current_metrics": monitoring_data
            },
            "alerts": model_state["alerts"][-10:],  # Last 10 alerts
            "transitions": model_state["transitions"],
            "health_status": self._calculate_health_status(model_state)
        }
    
    async def trigger_retraining(self, model_id: str, reason: str) -> str:
        """
        Trigger model retraining
        
        Args:
            model_id: Model ID
            reason: Reason for retraining
            
        Returns:
            Retraining job ID
        """
        model_state = self.model_states.get(model_id)
        if not model_state:
            raise ValueError(f"Model not found: {model_id}")
        
        # Get model configuration
        model = model_state["model"]
        
        # Create retraining configuration
        retraining_config = {
            "base_model_id": model_id,
            "name": f"{model['name']}_retrain_{datetime.utcnow().strftime('%Y%m%d')}",
            "framework": model["framework"],
            "model_type": model["model_type"],
            "dataset": model.get("dataset", {}),
            "hyperparameters": model.get("hyperparameters", {}),
            "reason": reason
        }
        
        # Submit retraining job
        # This would integrate with the training orchestrator
        job_id = f"retrain_{model_id}_{datetime.utcnow().timestamp()}"
        
        # Emit event
        await self.event_bus.publish(
            "mlops.retraining.triggered",
            {
                "model_id": model_id,
                "job_id": job_id,
                "reason": reason,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Retraining triggered for model {model_id}: {reason}")
        return job_id
    
    async def _monitor_models(self):
        """Monitor model performance and drift"""
        while True:
            try:
                await asyncio.sleep(self.config["monitoring"]["check_interval"])
                
                for model_id, model_state in self.model_states.items():
                    if not model_state["monitoring"]["enabled"]:
                        continue
                    
                    try:
                        # Check model performance
                        metrics = await self.model_monitor.check_model_performance(model_id)
                        model_state["monitoring"]["metrics"] = metrics
                        model_state["monitoring"]["last_check"] = datetime.utcnow()
                        
                        # Check for drift
                        drift_score = await self.drift_detector.check_drift(model_id)
                        model_state["monitoring"]["drift_score"] = drift_score
                        
                        # Handle alerts
                        await self._check_alerts(model_id, metrics, drift_score)
                        
                    except Exception as e:
                        logger.error(f"Error monitoring model {model_id}: {e}")
                
            except Exception as e:
                logger.error(f"Error in model monitoring: {e}")
    
    async def _check_alerts(self, model_id: str, metrics: Dict[str, Any], 
                          drift_score: float):
        """Check and trigger alerts"""
        model_state = self.model_states.get(model_id)
        if not model_state:
            return
        
        alerts = []
        
        # Check drift
        if drift_score > self.config["monitoring"]["drift_threshold"]:
            alerts.append({
                "type": "drift",
                "severity": "high" if drift_score > 0.2 else "medium",
                "message": f"Data drift detected: {drift_score:.3f}",
                "value": drift_score
            })
            self.metrics["drift_detected"] += 1
        
        # Check performance
        performance = metrics.get("performance", 1.0)
        if performance < self.config["monitoring"]["performance_threshold"]:
            alerts.append({
                "type": "performance",
                "severity": "high" if performance < 0.7 else "medium",
                "message": f"Performance degradation: {performance:.3f}",
                "value": performance
            })
        
        # Process alerts
        for alert in alerts:
            await self._process_alert(model_id, alert)
    
    async def _process_alert(self, model_id: str, alert: Dict[str, Any]):
        """Process individual alert"""
        model_state = self.model_states.get(model_id)
        if not model_state:
            return
        
        # Check cooldown
        last_alert = next(
            (a for a in reversed(model_state["alerts"]) 
             if a["type"] == alert["type"]),
            None
        )
        
        if last_alert:
            time_since_last = (datetime.utcnow() - last_alert["timestamp"]).seconds
            if time_since_last < self.config["monitoring"]["alert_cooldown"]:
                return
        
        # Add alert
        alert["timestamp"] = datetime.utcnow()
        alert["model_id"] = model_id
        model_state["alerts"].append(alert)
        
        # Update metrics
        self.metrics["alerts_triggered"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "mlops.alert.triggered",
            {
                "model_id": model_id,
                "alert": alert,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        # Trigger actions based on alert
        if alert["severity"] == "high":
            if alert["type"] == "drift":
                await self.trigger_retraining(model_id, "High data drift detected")
            elif alert["type"] == "performance":
                # Could trigger rollback or other actions
                pass
    
    async def _manage_lifecycle(self):
        """Manage model lifecycle transitions"""
        while True:
            try:
                await asyncio.sleep(3600)  # Check every hour
                
                for model_id, model_state in self.model_states.items():
                    # Auto-promote from staging to production
                    if (model_state["stage"] == ModelStage.STAGING and 
                        self.config["lifecycle"]["auto_promote"]):
                        
                        # Check staging duration
                        last_transition = model_state["transitions"][-1]
                        days_in_staging = (datetime.utcnow() - last_transition["timestamp"]).days
                        
                        if days_in_staging >= self.config["lifecycle"]["staging_duration_days"]:
                            # Check performance
                            metrics = model_state["monitoring"].get("metrics", {})
                            performance = metrics.get("performance", 0)
                            
                            if performance >= self.config["lifecycle"]["min_staging_performance"]:
                                await self.promote_model(
                                    model_id, 
                                    ModelStage.PRODUCTION.value,
                                    "Auto-promotion after successful staging"
                                )
                    
                    # Archive old models
                    if model_state["stage"] == ModelStage.PRODUCTION:
                        created_at = model_state["model"].get("created_at")
                        if created_at:
                            days_old = (datetime.utcnow() - created_at).days
                            
                            if days_old > self.config["lifecycle"]["archive_after_days"]:
                                await self._archive_model(model_id)
                
            except Exception as e:
                logger.error(f"Error in lifecycle management: {e}")
    
    async def _archive_model(self, model_id: str):
        """Archive a model"""
        model_state = self.model_states.get(model_id)
        if not model_state:
            return
        
        # Update stage
        model_state["stage"] = ModelStage.ARCHIVED
        model_state["monitoring"]["enabled"] = False
        
        # Update in registry
        await self.model_registry.update_model_stage(model_id, ModelStage.ARCHIVED.value)
        
        # Update metrics
        self.metrics["models_archived"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "mlops.model.archived",
            {
                "model_id": model_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Model archived: {model_id}")
    
    def _validate_transition(self, from_stage: ModelStage, 
                           to_stage: ModelStage) -> bool:
        """Validate stage transition"""
        valid_transitions = {
            ModelStage.DEVELOPMENT: [ModelStage.STAGING, ModelStage.ARCHIVED],
            ModelStage.STAGING: [ModelStage.PRODUCTION, ModelStage.DEVELOPMENT, ModelStage.ARCHIVED],
            ModelStage.PRODUCTION: [ModelStage.STAGING, ModelStage.ARCHIVED],
            ModelStage.ARCHIVED: [ModelStage.DEVELOPMENT]
        }
        
        return to_stage in valid_transitions.get(from_stage, [])
    
    async def _check_approval(self, model_id: str, target_stage: str) -> bool:
        """Check if promotion is approved"""
        # This would integrate with an approval system
        # For now, return True
        return True
    
    async def _check_production_readiness(self, model_id: str) -> bool:
        """Check if model is ready for production"""
        model_state = self.model_states.get(model_id)
        if not model_state:
            return False
        
        # Check performance metrics
        metrics = model_state["monitoring"].get("metrics", {})
        performance = metrics.get("performance", 0)
        
        if performance < self.config["lifecycle"]["min_staging_performance"]:
            return False
        
        # Check drift score
        drift_score = model_state["monitoring"].get("drift_score", 0)
        if drift_score > self.config["monitoring"]["drift_threshold"]:
            return False
        
        # Check for critical alerts
        recent_alerts = [
            a for a in model_state["alerts"][-10:]
            if a["severity"] == "high"
        ]
        
        if recent_alerts:
            return False
        
        return True
    
    def _calculate_health_status(self, model_state: Dict[str, Any]) -> str:
        """Calculate overall model health status"""
        if not model_state["monitoring"]["enabled"]:
            return "not_monitored"
        
        # Check for recent high severity alerts
        recent_alerts = [
            a for a in model_state["alerts"][-5:]
            if (datetime.utcnow() - a["timestamp"]).days < 1
        ]
        
        high_severity_alerts = [a for a in recent_alerts if a["severity"] == "high"]
        if high_severity_alerts:
            return "critical"
        
        medium_severity_alerts = [a for a in recent_alerts if a["severity"] == "medium"]
        if medium_severity_alerts:
            return "warning"
        
        # Check drift and performance
        drift_score = model_state["monitoring"].get("drift_score", 0)
        metrics = model_state["monitoring"].get("metrics", {})
        performance = metrics.get("performance", 1.0)
        
        if drift_score > 0.15 or performance < 0.75:
            return "degraded"
        
        return "healthy"
    
    async def _process_alerts(self):
        """Process and aggregate alerts"""
        while True:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                # Aggregate alerts by model
                for model_id in self.alerts:
                    alerts = self.alerts[model_id]
                    if not alerts:
                        continue
                    
                    # Group by type
                    alert_summary = {}
                    for alert in alerts:
                        alert_type = alert["type"]
                        if alert_type not in alert_summary:
                            alert_summary[alert_type] = []
                        alert_summary[alert_type].append(alert)
                    
                    # Emit summary
                    await self.event_bus.publish(
                        "mlops.alert.summary",
                        {
                            "model_id": model_id,
                            "summary": alert_summary,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                
                # Clear old alerts
                self.alerts.clear()
                
            except Exception as e:
                logger.error(f"Error processing alerts: {e}")
    
    async def get_mlops_metrics(self) -> Dict[str, Any]:
        """Get MLOps metrics"""
        return {
            **self.metrics,
            "total_models": len(self.model_states),
            "models_by_stage": {
                stage.value: sum(1 for s in self.model_states.values() 
                               if s["stage"] == stage)
                for stage in ModelStage
            },
            "healthy_models": sum(
                1 for s in self.model_states.values()
                if self._calculate_health_status(s) == "healthy"
            )
        } 