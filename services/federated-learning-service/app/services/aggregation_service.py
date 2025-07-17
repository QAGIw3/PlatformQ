import logging
import json
import uuid
import time
from typing import Dict, Any, List

from pyignite import Client as IgniteClient
from platformq.shared.event_publisher import EventPublisher

from ..core.config import settings

logger = logging.getLogger(__name__)

class AggregationService:
    def __init__(self, ignite_client: IgniteClient, event_publisher: EventPublisher):
        self.ignite_client = ignite_client
        self.event_publisher = event_publisher

    async def start_training_round(self, session_id: str, round_number: int):
        """Start a new training round"""
        session_data = self._get_session(session_id)
        
        participants_cache = self.ignite_client.get_cache("fl_participants")
        participant_keys = [k for k in participants_cache.keys() if k.startswith(f"{session_id}:")]
        
        event_data = {
            "session_id": session_id,
            "round_number": round_number,
            "training_config": {
                "model_type": session_data["model_type"],
                "algorithm": session_data["algorithm"],
                "dataset_requirements": session_data["dataset_requirements"],
                "privacy_parameters": session_data["privacy_parameters"],
                "training_parameters": session_data["training_parameters"]
            },
            "timestamp": int(time.time() * 1000)
        }
        
        for key in participant_keys:
            participant_data = json.loads(participants_cache.get(key))
            tenant_id = participant_data["tenant_id"]
            
            self.event_publisher.publish(
                topic=f"persistent://platformq/{tenant_id}/federated-learning-training",
                schema_path="federated_training_round_started.avsc",
                data=event_data
            )
        
        self._update_session(session_id, {
            "status": "TRAINING",
            "current_round": round_number,
            "round_start_time": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        })
    
    async def aggregate_model_updates(self, session_id: str, round_number: int, update_uris: List[str]):
        """Trigger model aggregation"""
        config = {
            "session_id": session_id,
            "round_number": round_number,
            "aggregation_config": {
                "update_uris": update_uris,
                "aggregation_strategy": "FedAvg"
            }
        }
        
        job_id = await self.submit_spark_job("aggregation", config)
        
        self._update_session(session_id, {
            "status": "AGGREGATING",
            "aggregation_job_id": job_id
        })
        
        return job_id

    async def submit_spark_job(self, job_type: str, config: Dict[str, Any]) -> str:
        """Submit Spark job for federated learning or aggregation"""
        spark_submit_cmd = [
            "spark-submit",
            "--master", settings.spark_master_url,
            "--deploy-mode", "cluster",
            "--conf", "spark.executor.memory=4g",
            "--conf", "spark.executor.cores=4",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--packages", "org.apache.ignite:ignite-spark:2.14.0",
            "--py-files", "/app/processing/spark/ml/federated_learning.py"
        ]
        
        if job_type == "training":
            spark_submit_cmd.extend([
                "/app/processing/spark/ml/federated_learning.py",
                config["session_id"],
                config["tenant_id"],
                config["participant_id"],
                json.dumps(config["training_config"])
            ])
        elif job_type == "aggregation":
            spark_submit_cmd.extend([
                "/app/processing/spark/ml/federated_aggregator.py",
                config["session_id"],
                str(config["round_number"]),
                json.dumps(config["aggregation_config"])
            ])
        
        job_id = f"{job_type}_{config['session_id']}_{uuid.uuid4()}"
        logger.info(f"Submitted Spark job {job_id}: {' '.join(spark_submit_cmd)}")
        
        return job_id

    def _get_session(self, session_id: str) -> Dict[str, Any]:
        cache = self.ignite_client.get_cache("fl_sessions")
        session_json = cache.get(session_id)
        if not session_json:
            return None
        return json.loads(session_json)

    def _update_session(self, session_id: str, updates: Dict[str, Any]):
        cache = self.ignite_client.get_cache("fl_sessions")
        session_data = json.loads(cache.get(session_id))
        session_data.update(updates)
        cache.put(session_id, json.dumps(session_data)) 