"""
Federated Learning Coordinator

Orchestrates privacy-preserving distributed model training across multiple clients.
"""

import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from enum import Enum
import uuid
import numpy as np

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class FederatedStatus(Enum):
    """Federated learning round status"""
    INITIALIZING = "initializing"
    RECRUITING = "recruiting"
    TRAINING = "training"
    AGGREGATING = "aggregating"
    COMPLETED = "completed"
    FAILED = "failed"


class ClientStatus(Enum):
    """Client status in federated learning"""
    AVAILABLE = "available"
    SELECTED = "selected"
    TRAINING = "training"
    COMPLETED = "completed"
    FAILED = "failed"
    DISCONNECTED = "disconnected"


class FederatedCoordinator:
    """
    Coordinates federated learning across distributed clients
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus,
                 model_registry: Any, client_manager: Any, aggregation_strategy: Any,
                 privacy_mechanism: Any):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        self.model_registry = model_registry
        self.client_manager = client_manager
        self.aggregation_strategy = aggregation_strategy
        self.privacy_mechanism = privacy_mechanism
        
        # Federated learning sessions
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.active_rounds: Dict[str, Dict[str, Any]] = {}
        
        # Configuration
        self.config = {
            "rounds": {
                "max_rounds": 100,
                "min_clients_per_round": 2,
                "client_fraction": 0.1,
                "round_timeout": 600,  # 10 minutes
                "convergence_threshold": 0.001
            },
            "privacy": {
                "differential_privacy": True,
                "epsilon": 1.0,
                "delta": 1e-5,
                "secure_aggregation": True,
                "homomorphic_encryption": False
            },
            "client_selection": {
                "strategy": "random",  # random, performance, resource
                "reliability_threshold": 0.8,
                "min_data_samples": 100
            },
            "aggregation": {
                "strategy": "fedavg",  # fedavg, fedprox, scaffold
                "learning_rate": 0.01,
                "momentum": 0.9,
                "proximal_term": 0.01
            }
        }
        
        # Metrics
        self.metrics = {
            "sessions_created": 0,
            "rounds_completed": 0,
            "clients_participated": 0,
            "avg_round_time": 0,
            "convergence_rate": 0
        }
    
    async def initialize(self):
        """Initialize federated coordinator"""
        logger.info("initializing_federated_coordinator")
        
        # Load configuration
        await self._load_configuration()
        
        # Initialize components
        await self.client_manager.initialize()
        await self.aggregation_strategy.initialize(self.config["aggregation"])
        await self.privacy_mechanism.initialize(self.config["privacy"])
        
        # Start background tasks
        asyncio.create_task(self._monitor_sessions())
        asyncio.create_task(self._process_client_updates())
        
        logger.info("federated_coordinator_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        # Stop all active sessions
        for session_id in list(self.sessions.keys()):
            await self.stop_session(session_id)
        
        await self.client_manager.cleanup()
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/federated-learning")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def create_session(self, session_config: Dict[str, Any]) -> str:
        """
        Create a new federated learning session
        
        Args:
            session_config: Session configuration including:
                - name: Session name
                - model_config: Base model configuration
                - dataset_config: Dataset requirements
                - training_config: Training hyperparameters
                - privacy_config: Privacy settings
                - convergence_criteria: Convergence conditions
                
        Returns:
            Session ID
        """
        session_id = str(uuid.uuid4())
        
        # Validate configuration
        self._validate_session_config(session_config)
        
        # Create session
        session = {
            "id": session_id,
            "config": session_config,
            "status": FederatedStatus.INITIALIZING,
            "created_at": datetime.utcnow(),
            "rounds": [],
            "current_round": 0,
            "global_model": None,
            "metrics": {
                "loss_history": [],
                "accuracy_history": [],
                "client_participation": [],
                "privacy_budget": {
                    "epsilon_spent": 0.0,
                    "delta_spent": 0.0
                }
            },
            "convergence": {
                "converged": False,
                "best_loss": float('inf'),
                "patience_counter": 0
            }
        }
        
        # Initialize global model
        session["global_model"] = await self._initialize_global_model(session_config)
        
        # Store session
        self.sessions[session_id] = session
        
        # Update metrics
        self.metrics["sessions_created"] += 1
        
        # Start session
        asyncio.create_task(self._run_session(session_id))
        
        # Emit event
        await self.event_bus.publish(
            "federated.session.created",
            {
                "session_id": session_id,
                "name": session_config.get("name"),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Federated learning session created: {session_id}")
        return session_id
    
    async def get_session_status(self, session_id: str) -> Dict[str, Any]:
        """Get federated learning session status"""
        session = self.sessions.get(session_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")
        
        return {
            "id": session_id,
            "status": session["status"].value,
            "current_round": session["current_round"],
            "total_rounds": len(session["rounds"]),
            "metrics": session["metrics"],
            "convergence": session["convergence"],
            "created_at": session["created_at"].isoformat()
        }
    
    async def stop_session(self, session_id: str) -> bool:
        """Stop a federated learning session"""
        session = self.sessions.get(session_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")
        
        # Update status
        session["status"] = FederatedStatus.COMPLETED
        
        # Save final model
        if session["global_model"]:
            await self._save_final_model(session)
        
        # Emit event
        await self.event_bus.publish(
            "federated.session.stopped",
            {
                "session_id": session_id,
                "rounds_completed": session["current_round"],
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Federated learning session stopped: {session_id}")
        return True
    
    async def _run_session(self, session_id: str):
        """Run federated learning session"""
        session = self.sessions.get(session_id)
        if not session:
            return
        
        try:
            # Update status
            session["status"] = FederatedStatus.RECRUITING
            
            # Run rounds
            max_rounds = session["config"].get("max_rounds", self.config["rounds"]["max_rounds"])
            
            for round_num in range(max_rounds):
                session["current_round"] = round_num + 1
                
                # Run single round
                round_success = await self._run_round(session_id, round_num + 1)
                
                if not round_success:
                    logger.warning(f"Round {round_num + 1} failed for session {session_id}")
                    continue
                
                # Check convergence
                if session["convergence"]["converged"]:
                    logger.info(f"Session {session_id} converged at round {round_num + 1}")
                    break
                
                # Check early stopping
                if session["convergence"]["patience_counter"] > 10:
                    logger.info(f"Early stopping for session {session_id}")
                    break
            
            # Complete session
            session["status"] = FederatedStatus.COMPLETED
            
            # Save final model
            await self._save_final_model(session)
            
            # Emit event
            await self.event_bus.publish(
                "federated.session.completed",
                {
                    "session_id": session_id,
                    "rounds_completed": session["current_round"],
                    "converged": session["convergence"]["converged"],
                    "final_metrics": session["metrics"],
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Federated learning session completed: {session_id}")
            
        except Exception as e:
            logger.error(f"Session {session_id} failed: {e}")
            session["status"] = FederatedStatus.FAILED
            
            # Emit event
            await self.event_bus.publish(
                "federated.session.failed",
                {
                    "session_id": session_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    async def _run_round(self, session_id: str, round_num: int) -> bool:
        """Run a single federated learning round"""
        session = self.sessions.get(session_id)
        if not session:
            return False
        
        round_id = f"{session_id}_round_{round_num}"
        start_time = datetime.utcnow()
        
        try:
            # Create round record
            round_data = {
                "id": round_id,
                "session_id": session_id,
                "round_num": round_num,
                "status": FederatedStatus.RECRUITING,
                "started_at": start_time,
                "clients": {},
                "aggregated_update": None,
                "metrics": {}
            }
            
            self.active_rounds[round_id] = round_data
            
            # Select clients
            selected_clients = await self._select_clients(session)
            if len(selected_clients) < self.config["rounds"]["min_clients_per_round"]:
                logger.warning(f"Not enough clients for round {round_num}")
                return False
            
            round_data["clients"] = {
                client_id: {"status": ClientStatus.SELECTED}
                for client_id in selected_clients
            }
            
            # Update status
            round_data["status"] = FederatedStatus.TRAINING
            session["status"] = FederatedStatus.TRAINING
            
            # Distribute model to clients
            await self._distribute_model(session, selected_clients)
            
            # Wait for client updates
            client_updates = await self._collect_client_updates(
                round_id, 
                selected_clients,
                timeout=self.config["rounds"]["round_timeout"]
            )
            
            if not client_updates:
                logger.warning(f"No client updates received for round {round_num}")
                return False
            
            # Update status
            round_data["status"] = FederatedStatus.AGGREGATING
            session["status"] = FederatedStatus.AGGREGATING
            
            # Apply privacy mechanisms
            if self.config["privacy"]["differential_privacy"]:
                client_updates = await self.privacy_mechanism.apply_differential_privacy(
                    client_updates,
                    epsilon=self.config["privacy"]["epsilon"],
                    delta=self.config["privacy"]["delta"]
                )
                
                # Update privacy budget
                session["metrics"]["privacy_budget"]["epsilon_spent"] += self.config["privacy"]["epsilon"]
                session["metrics"]["privacy_budget"]["delta_spent"] += self.config["privacy"]["delta"]
            
            # Aggregate updates
            aggregated_update = await self.aggregation_strategy.aggregate(
                client_updates,
                session["global_model"],
                round_num
            )
            
            round_data["aggregated_update"] = aggregated_update
            
            # Update global model
            session["global_model"] = await self._apply_update(
                session["global_model"],
                aggregated_update
            )
            
            # Evaluate global model
            eval_metrics = await self._evaluate_global_model(session)
            round_data["metrics"] = eval_metrics
            
            # Update session metrics
            session["metrics"]["loss_history"].append(eval_metrics.get("loss", 0))
            session["metrics"]["accuracy_history"].append(eval_metrics.get("accuracy", 0))
            session["metrics"]["client_participation"].append(len(client_updates))
            
            # Check convergence
            self._check_convergence(session, eval_metrics)
            
            # Complete round
            round_data["status"] = FederatedStatus.COMPLETED
            round_data["completed_at"] = datetime.utcnow()
            
            # Update round time metric
            round_time = (round_data["completed_at"] - start_time).total_seconds()
            self._update_avg_round_time(round_time)
            
            # Store round data
            session["rounds"].append({
                "round_num": round_num,
                "clients_participated": len(client_updates),
                "metrics": eval_metrics,
                "duration": round_time
            })
            
            # Update metrics
            self.metrics["rounds_completed"] += 1
            self.metrics["clients_participated"] += len(client_updates)
            
            # Emit event
            await self.event_bus.publish(
                "federated.round.completed",
                {
                    "session_id": session_id,
                    "round_num": round_num,
                    "clients_participated": len(client_updates),
                    "metrics": eval_metrics,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Round {round_num} completed for session {session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Round {round_num} failed for session {session_id}: {e}")
            round_data["status"] = FederatedStatus.FAILED
            return False
        
        finally:
            # Clean up round data
            if round_id in self.active_rounds:
                del self.active_rounds[round_id]
    
    async def _select_clients(self, session: Dict[str, Any]) -> List[str]:
        """Select clients for a training round"""
        strategy = self.config["client_selection"]["strategy"]
        
        # Get available clients
        available_clients = await self.client_manager.get_available_clients(
            min_data_samples=self.config["client_selection"]["min_data_samples"],
            reliability_threshold=self.config["client_selection"]["reliability_threshold"]
        )
        
        if not available_clients:
            return []
        
        # Calculate number of clients to select
        num_clients = max(
            self.config["rounds"]["min_clients_per_round"],
            int(len(available_clients) * self.config["rounds"]["client_fraction"])
        )
        
        # Select clients based on strategy
        if strategy == "random":
            import random
            selected = random.sample(available_clients, min(num_clients, len(available_clients)))
        
        elif strategy == "performance":
            # Select based on past performance
            client_scores = await self.client_manager.get_client_scores(available_clients)
            sorted_clients = sorted(client_scores.items(), key=lambda x: x[1], reverse=True)
            selected = [client_id for client_id, _ in sorted_clients[:num_clients]]
        
        elif strategy == "resource":
            # Select based on available resources
            client_resources = await self.client_manager.get_client_resources(available_clients)
            sorted_clients = sorted(client_resources.items(), key=lambda x: x[1]["compute_power"], reverse=True)
            selected = [client_id for client_id, _ in sorted_clients[:num_clients]]
        
        else:
            selected = available_clients[:num_clients]
        
        return selected
    
    async def _distribute_model(self, session: Dict[str, Any], client_ids: List[str]):
        """Distribute global model to selected clients"""
        model_data = {
            "session_id": session["id"],
            "round_num": session["current_round"],
            "model_state": session["global_model"],
            "training_config": session["config"]["training_config"]
        }
        
        # Send model to each client
        for client_id in client_ids:
            await self.client_manager.send_model_to_client(client_id, model_data)
    
    async def _collect_client_updates(self, round_id: str, client_ids: List[str], 
                                    timeout: int) -> Dict[str, Any]:
        """Collect updates from clients"""
        updates = {}
        deadline = datetime.utcnow() + timedelta(seconds=timeout)
        
        while datetime.utcnow() < deadline:
            # Check for client updates
            for client_id in client_ids:
                if client_id in updates:
                    continue
                
                update = await self.client_manager.get_client_update(client_id, round_id)
                if update:
                    updates[client_id] = update
                    
                    # Update round data
                    if round_id in self.active_rounds:
                        self.active_rounds[round_id]["clients"][client_id]["status"] = ClientStatus.COMPLETED
            
            # Check if we have enough updates
            if len(updates) >= self.config["rounds"]["min_clients_per_round"]:
                break
            
            await asyncio.sleep(1)
        
        return updates
    
    async def _apply_update(self, model: Any, update: Any) -> Any:
        """Apply aggregated update to global model"""
        # This would apply the update to the model
        # Implementation depends on the model framework
        return model
    
    async def _evaluate_global_model(self, session: Dict[str, Any]) -> Dict[str, float]:
        """Evaluate the global model"""
        # This would evaluate the model on a validation set
        # For now, return mock metrics
        import random
        
        # Simulate improving metrics
        round_num = session["current_round"]
        base_loss = 1.0 / (1 + round_num * 0.1)
        base_accuracy = min(0.95, 0.5 + round_num * 0.05)
        
        return {
            "loss": base_loss + random.uniform(-0.05, 0.05),
            "accuracy": base_accuracy + random.uniform(-0.02, 0.02),
            "val_loss": base_loss + random.uniform(-0.05, 0.05),
            "val_accuracy": base_accuracy + random.uniform(-0.02, 0.02)
        }
    
    def _check_convergence(self, session: Dict[str, Any], metrics: Dict[str, float]):
        """Check if training has converged"""
        current_loss = metrics.get("loss", float('inf'))
        
        # Check if loss improved
        if current_loss < session["convergence"]["best_loss"] - self.config["rounds"]["convergence_threshold"]:
            session["convergence"]["best_loss"] = current_loss
            session["convergence"]["patience_counter"] = 0
        else:
            session["convergence"]["patience_counter"] += 1
        
        # Check convergence criteria
        if len(session["metrics"]["loss_history"]) > 10:
            recent_losses = session["metrics"]["loss_history"][-10:]
            loss_variance = np.var(recent_losses)
            
            if loss_variance < self.config["rounds"]["convergence_threshold"] ** 2:
                session["convergence"]["converged"] = True
                self.metrics["convergence_rate"] = session["current_round"] / len(session["rounds"])
    
    async def _initialize_global_model(self, session_config: Dict[str, Any]) -> Any:
        """Initialize the global model"""
        model_config = session_config.get("model_config", {})
        
        # This would initialize the actual model based on framework
        # For now, return a placeholder
        return {
            "architecture": model_config.get("architecture"),
            "parameters": {},
            "optimizer_state": {}
        }
    
    async def _save_final_model(self, session: Dict[str, Any]):
        """Save the final trained model"""
        model_info = {
            "name": f"{session['config']['name']}_federated",
            "framework": "federated",
            "model_type": session["config"]["model_config"]["architecture"],
            "training_method": "federated_learning",
            "session_id": session["id"],
            "rounds_completed": session["current_round"],
            "metrics": {
                "final_loss": session["metrics"]["loss_history"][-1] if session["metrics"]["loss_history"] else None,
                "final_accuracy": session["metrics"]["accuracy_history"][-1] if session["metrics"]["accuracy_history"] else None,
                "privacy_budget": session["metrics"]["privacy_budget"]
            },
            "model_state": session["global_model"]
        }
        
        # Register in model registry
        await self.model_registry.register_model(model_info)
    
    def _validate_session_config(self, config: Dict[str, Any]):
        """Validate session configuration"""
        required_fields = ["name", "model_config", "training_config"]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
    
    def _update_avg_round_time(self, round_time: float):
        """Update average round time metric"""
        completed = self.metrics["rounds_completed"]
        
        if completed == 1:
            self.metrics["avg_round_time"] = round_time
        else:
            current_avg = self.metrics["avg_round_time"]
            self.metrics["avg_round_time"] = (
                (current_avg * (completed - 1) + round_time) / completed
            )
    
    async def _monitor_sessions(self):
        """Monitor active sessions"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                for session_id, session in list(self.sessions.items()):
                    if session["status"] in [FederatedStatus.COMPLETED, FederatedStatus.FAILED]:
                        continue
                    
                    # Check for stalled sessions
                    if session["rounds"]:
                        last_round = session["rounds"][-1]
                        time_since_last = (datetime.utcnow() - last_round["completed_at"]).seconds
                        
                        if time_since_last > 1800:  # 30 minutes
                            logger.warning(f"Session {session_id} appears stalled")
                            session["status"] = FederatedStatus.FAILED
                
            except Exception as e:
                logger.error(f"Error monitoring sessions: {e}")
    
    async def _process_client_updates(self):
        """Process incoming client updates"""
        while True:
            try:
                # This would process client updates from a queue
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing client updates: {e}")
    
    async def get_federated_metrics(self) -> Dict[str, Any]:
        """Get federated learning metrics"""
        return {
            **self.metrics,
            "active_sessions": sum(
                1 for s in self.sessions.values()
                if s["status"] not in [FederatedStatus.COMPLETED, FederatedStatus.FAILED]
            ),
            "total_sessions": len(self.sessions),
            "active_rounds": len(self.active_rounds)
        } 