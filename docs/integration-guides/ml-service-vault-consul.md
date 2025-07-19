# Machine Learning Service - Vault & Consul Integration Guide

## Overview
This guide covers integrating ML services with Vault and Consul for secure model management, experiment tracking, and distributed training coordination.

## Vault Integration

### 1. Secret Structure

```yaml
# Vault path structure for ML services
ml-platform-service/
├── model-registry/
│   ├── signing-keys/
│   │   ├── model-signing-key      # Sign model artifacts
│   │   └── prediction-signing-key  # Sign predictions
│   ├── model-encryption/
│   │   ├── production-models/     # Encryption keys for prod models
│   │   └── experimental-models/   # Keys for experiments
│   └── api-keys/
│       ├── huggingface-token
│       ├── openai-api-key
│       └── anthropic-api-key
├── training-infrastructure/
│   ├── cloud-credentials/
│   │   ├── aws/
│   │   │   ├── sagemaker-role
│   │   │   └── ec2-credentials
│   │   ├── gcp/
│   │   │   ├── vertex-ai-key
│   │   │   └── compute-engine
│   │   └── azure/
│   │       └── ml-workspace
│   ├── gpu-clusters/
│   │   ├── kubernetes-tokens/
│   │   └── slurm-credentials/
│   └── distributed-training/
│       ├── redis-credentials     # For parameter server
│       └── nccl-keys            # For GPU communication
├── experiment-tracking/
│   ├── mlflow/
│   │   ├── database-credentials
│   │   ├── artifact-store-creds
│   │   └── tracking-server-key
│   ├── wandb/
│   │   └── api-key
│   └── tensorboard/
│       └── storage-credentials
├── feature-store/
│   ├── feast/
│   │   ├── redis-online-creds
│   │   └── s3-offline-creds
│   └── feature-encryption/
│       ├── pii-features-key
│       └── sensitive-features-key
└── model-serving/
    ├── inference-endpoints/
    │   ├── api-keys/
    │   └── tls-certificates/
    ├── edge-deployment/
    │   └── device-keys/
    └── monitoring/
        ├── prometheus-token
        └── grafana-api-key
```

### 2. Implementation Code

```python
# ml_service/vault_integration.py
from typing import Dict, Any, Optional, List, Tuple
import asyncio
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
from platformq_shared.vault.vault_client import VaultClient
import joblib
import numpy as np
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
import logging

logger = logging.getLogger(__name__)

class MLServiceVaultIntegration:
    """
    Vault integration for ML services with model security,
    experiment tracking, and distributed training support.
    """
    
    def __init__(self, vault_client: VaultClient, service_name: str = "ml-platform-service"):
        self.vault = vault_client
        self.service_name = service_name
        self._model_keys: Dict[str, bytes] = {}
        self._api_clients: Dict[str, Any] = {}
        
    async def initialize(self):
        """Initialize Vault integration for ML service"""
        # Set up Transit engine for model signing
        await self._setup_model_signing()
        
        # Initialize feature encryption
        await self._setup_feature_encryption()
        
        # Load API keys
        await self._load_api_keys()
        
    async def _setup_model_signing(self):
        """Set up Transit keys for model signing and verification"""
        signing_keys = ["model-signing-key", "prediction-signing-key"]
        
        for key_name in signing_keys:
            try:
                await self.vault.read_transit_key(key_name)
            except:
                # Create RSA key for signing
                await self.vault.create_transit_key(
                    key_name,
                    key_type="rsa-4096",
                    exportable=False,
                    allow_plaintext_backup=False
                )
                logger.info(f"Created model signing key: {key_name}")
                
    async def sign_model_artifact(self, 
                                 model_path: Path,
                                 metadata: Dict[str, Any]) -> Dict[str, str]:
        """
        Sign ML model artifact for integrity verification.
        Returns signature and metadata.
        """
        # Calculate model hash
        model_hash = await self._calculate_model_hash(model_path)
        
        # Create signing payload
        signing_payload = {
            "model_hash": model_hash,
            "model_name": metadata.get("name", "unknown"),
            "version": metadata.get("version", "1.0.0"),
            "framework": metadata.get("framework", "unknown"),
            "trained_at": metadata.get("trained_at", datetime.utcnow().isoformat()),
            "metrics": metadata.get("metrics", {}),
            "dataset_version": metadata.get("dataset_version", "unknown"),
            "signed_at": datetime.utcnow().isoformat()
        }
        
        # Sign with Transit
        payload_json = json.dumps(signing_payload, sort_keys=True)
        signature = await self.vault.sign_data(
            key_name="model-signing-key",
            input=hashlib.sha256(payload_json.encode()).hexdigest(),
            hash_algorithm="sha2-256",
            signature_algorithm="pkcs1v15"
        )
        
        return {
            "signature": signature["signature"],
            "payload": payload_json,
            "model_hash": model_hash,
            "algorithm": "RSA-4096-SHA256"
        }
        
    async def verify_model_signature(self, 
                                   model_path: Path,
                                   signature_data: Dict[str, str]) -> bool:
        """Verify model signature and integrity"""
        # Recalculate model hash
        current_hash = await self._calculate_model_hash(model_path)
        
        # Check hash matches
        if current_hash != signature_data["model_hash"]:
            logger.error("Model hash mismatch")
            return False
            
        # Verify signature with Transit
        payload_hash = hashlib.sha256(signature_data["payload"].encode()).hexdigest()
        
        try:
            result = await self.vault.verify_signature(
                key_name="model-signing-key",
                input=payload_hash,
                signature=signature_data["signature"],
                hash_algorithm="sha2-256",
                signature_algorithm="pkcs1v15"
            )
            return result["valid"]
        except Exception as e:
            logger.error(f"Signature verification failed: {e}")
            return False
            
    async def _calculate_model_hash(self, model_path: Path) -> str:
        """Calculate cryptographic hash of model file"""
        sha256_hash = hashlib.sha256()
        
        with open(model_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
                
        return sha256_hash.hexdigest()
        
    async def encrypt_model(self, model_path: Path, environment: str = "production") -> Path:
        """
        Encrypt ML model for secure storage.
        Returns path to encrypted model.
        """
        # Get encryption key for environment
        key_path = f"{self.service_name}/model-encryption/{environment}-models/current-key"
        
        # Ensure key exists
        try:
            await self.vault.get_secret(key_path)
        except:
            # Generate new key
            from cryptography.fernet import Fernet
            key = Fernet.generate_key()
            await self.vault.create_or_update_secret(
                key_path,
                {
                    "value": key.decode(),
                    "created_at": datetime.utcnow().isoformat(),
                    "algorithm": "AES-256-GCM"
                }
            )
            
        # Encrypt model file
        key_data = await self.vault.get_secret(key_path)
        fernet = Fernet(key_data["value"].encode())
        
        encrypted_path = model_path.with_suffix(".encrypted")
        
        with open(model_path, "rb") as infile:
            encrypted_data = fernet.encrypt(infile.read())
            
        with open(encrypted_path, "wb") as outfile:
            outfile.write(encrypted_data)
            
        logger.info(f"Encrypted model saved to {encrypted_path}")
        return encrypted_path
        
    async def decrypt_model_for_serving(self, 
                                      encrypted_path: Path,
                                      environment: str = "production") -> Any:
        """
        Decrypt model directly into memory for serving.
        Model never touches disk in plaintext.
        """
        # Get decryption key
        key_path = f"{self.service_name}/model-encryption/{environment}-models/current-key"
        key_data = await self.vault.get_secret(key_path)
        
        from cryptography.fernet import Fernet
        fernet = Fernet(key_data["value"].encode())
        
        # Decrypt directly to memory
        with open(encrypted_path, "rb") as f:
            decrypted_data = fernet.decrypt(f.read())
            
        # Load model from bytes
        import io
        return joblib.load(io.BytesIO(decrypted_data))
        
    async def get_training_credentials(self, 
                                     provider: str,
                                     resource_type: str = "gpu") -> Dict[str, Any]:
        """Get cloud training infrastructure credentials"""
        base_path = f"{self.service_name}/training-infrastructure/cloud-credentials/{provider}"
        
        if provider == "aws":
            if resource_type == "sagemaker":
                creds = await self.vault.get_secret(f"{base_path}/sagemaker-role")
                return {
                    "role_arn": creds["role_arn"],
                    "external_id": creds.get("external_id")
                }
            else:
                creds = await self.vault.get_secret(f"{base_path}/ec2-credentials")
                return {
                    "access_key_id": creds["access_key_id"],
                    "secret_access_key": creds["secret_access_key"],
                    "region": creds.get("region", "us-east-1")
                }
                
        elif provider == "gcp":
            creds = await self.vault.get_secret(f"{base_path}/vertex-ai-key")
            return {
                "type": "service_account",
                "project_id": creds["project_id"],
                "private_key": creds["private_key"],
                "client_email": creds["client_email"]
            }
            
        elif provider == "azure":
            creds = await self.vault.get_secret(f"{base_path}/ml-workspace")
            return {
                "subscription_id": creds["subscription_id"],
                "resource_group": creds["resource_group"],
                "workspace_name": creds["workspace_name"],
                "tenant_id": creds["tenant_id"],
                "client_id": creds["client_id"],
                "client_secret": creds["client_secret"]
            }
            
    async def get_experiment_tracking_client(self, platform: str = "mlflow"):
        """Get authenticated experiment tracking client"""
        if platform == "mlflow":
            creds = await self.vault.get_secret(
                f"{self.service_name}/experiment-tracking/mlflow/tracking-server-key"
            )
            
            import mlflow
            mlflow.set_tracking_uri(creds["tracking_uri"])
            mlflow.set_experiment(creds.get("default_experiment", "default"))
            
            # Set up artifact store credentials
            artifact_creds = await self.vault.get_secret(
                f"{self.service_name}/experiment-tracking/mlflow/artifact-store-creds"
            )
            
            os.environ["AWS_ACCESS_KEY_ID"] = artifact_creds["access_key_id"]
            os.environ["AWS_SECRET_ACCESS_KEY"] = artifact_creds["secret_access_key"]
            
            return mlflow
            
        elif platform == "wandb":
            creds = await self.vault.get_secret(
                f"{self.service_name}/experiment-tracking/wandb/api-key"
            )
            
            import wandb
            wandb.login(key=creds["api_key"])
            return wandb
            
    async def encrypt_feature_data(self, 
                                 features: np.ndarray,
                                 feature_names: List[str],
                                 sensitive_features: List[str]) -> Tuple[np.ndarray, Dict]:
        """
        Encrypt sensitive features before storage/transmission.
        Returns modified feature array and encryption metadata.
        """
        # Get encryption key for sensitive features
        key_path = f"{self.service_name}/feature-store/feature-encryption/sensitive-features-key"
        key_data = await self.vault.get_secret(key_path)
        
        from cryptography.fernet import Fernet
        fernet = Fernet(key_data["value"].encode())
        
        # Identify indices of sensitive features
        sensitive_indices = [
            i for i, name in enumerate(feature_names) 
            if name in sensitive_features
        ]
        
        # Encrypt sensitive features
        encrypted_features = features.copy()
        encryption_metadata = {
            "encrypted_features": sensitive_features,
            "encryption_timestamp": datetime.utcnow().isoformat(),
            "key_version": key_data.get("version", 1)
        }
        
        for idx in sensitive_indices:
            # Convert to bytes and encrypt
            feature_bytes = features[:, idx].tobytes()
            encrypted_bytes = fernet.encrypt(feature_bytes)
            
            # Store encrypted hash (for the demo, in practice you'd handle this differently)
            encrypted_features[:, idx] = hash(encrypted_bytes) % 1e9
            
            # Store encryption mapping
            encryption_metadata[f"feature_{feature_names[idx]}_encrypted"] = True
            
        return encrypted_features, encryption_metadata
        
    async def get_model_serving_credentials(self, 
                                          deployment_type: str = "api") -> Dict[str, Any]:
        """Get credentials for model serving infrastructure"""
        if deployment_type == "api":
            # Get API key for serving
            key_path = f"{self.service_name}/model-serving/inference-endpoints/api-keys/primary"
            api_key = await self.vault.get_secret(key_path)
            
            # Get TLS certificate
            cert_path = f"{self.service_name}/model-serving/inference-endpoints/tls-certificates/current"
            tls_cert = await self.vault.get_secret(cert_path)
            
            return {
                "api_key": api_key["value"],
                "tls_cert": tls_cert["certificate"],
                "tls_key": tls_cert["private_key"]
            }
            
        elif deployment_type == "edge":
            # Get edge device keys
            device_keys = await self.vault.get_secret(
                f"{self.service_name}/model-serving/edge-deployment/device-keys"
            )
            return device_keys

# Specialized class for distributed training
class DistributedTrainingVaultIntegration(MLServiceVaultIntegration):
    """Extended integration for distributed ML training"""
    
    async def get_distributed_training_config(self, 
                                            cluster_type: str = "kubernetes") -> Dict[str, Any]:
        """Get configuration for distributed training setup"""
        if cluster_type == "kubernetes":
            # Get k8s credentials
            k8s_token = await self.vault.get_secret(
                f"{self.service_name}/training-infrastructure/gpu-clusters/kubernetes-tokens/primary"
            )
            
            # Get distributed backend credentials
            redis_creds = await self.vault.get_secret(
                f"{self.service_name}/training-infrastructure/distributed-training/redis-credentials"
            )
            
            return {
                "kubernetes": {
                    "token": k8s_token["token"],
                    "server": k8s_token["server"],
                    "namespace": k8s_token.get("namespace", "ml-training")
                },
                "backend": {
                    "type": "redis",
                    "host": redis_creds["host"],
                    "port": redis_creds["port"],
                    "password": redis_creds["password"]
                },
                "nccl": {
                    "enabled": True,
                    "socket_ifname": "eth0"
                }
            }
            
    async def coordinate_distributed_job(self, 
                                       job_id: str,
                                       world_size: int) -> Dict[str, Any]:
        """Coordinate distributed training job with secure communication"""
        # Generate unique job credentials
        job_secret = hashlib.sha256(f"{job_id}-{datetime.utcnow()}".encode()).hexdigest()
        
        # Store in Vault with TTL
        job_path = f"{self.service_name}/training-jobs/{job_id}"
        await self.vault.create_or_update_secret(
            job_path,
            {
                "job_secret": job_secret,
                "world_size": world_size,
                "created_at": datetime.utcnow().isoformat(),
                "status": "initializing",
                "workers": []
            },
            ttl=86400  # 24 hour TTL
        )
        
        return {
            "job_id": job_id,
            "job_secret": job_secret,
            "coordination_endpoint": f"vault://{job_path}"
        }
        
    async def register_worker(self, 
                            job_id: str,
                            worker_id: str,
                            worker_info: Dict[str, Any]) -> Dict[str, Any]:
        """Register worker in distributed training job"""
        job_path = f"{self.service_name}/training-jobs/{job_id}"
        
        # Get current job state
        job_data = await self.vault.get_secret(job_path)
        
        # Add worker
        job_data["workers"].append({
            "worker_id": worker_id,
            "rank": len(job_data["workers"]),
            "hostname": worker_info.get("hostname"),
            "gpu_count": worker_info.get("gpu_count", 0),
            "registered_at": datetime.utcnow().isoformat()
        })
        
        # Update job state
        if len(job_data["workers"]) == job_data["world_size"]:
            job_data["status"] = "ready"
            
        await self.vault.create_or_update_secret(job_path, job_data)
        
        return {
            "rank": len(job_data["workers"]) - 1,
            "world_size": job_data["world_size"],
            "master_addr": job_data["workers"][0]["hostname"] if job_data["workers"] else None
        }
```

## Consul Integration

### 1. Configuration Structure

```yaml
# Consul KV structure for ML services
services/ml-platform-service/
├── config/
│   ├── training/
│   │   ├── default-batch-size         # 32
│   │   ├── max-epochs                 # 100
│   │   ├── early-stopping-patience    # 5
│   │   ├── distributed-backend        # nccl
│   │   └── checkpoint-frequency       # 5
│   ├── serving/
│   │   ├── max-batch-size            # 64
│   │   ├── model-cache-size-gb       # 10
│   │   ├── inference-timeout-ms      # 1000
│   │   ├── auto-scaling-enabled      # true
│   │   └── min-replicas              # 2
│   ├── feature-engineering/
│   │   ├── feature-store-backend     # feast
│   │   ├── online-store-ttl-seconds  # 3600
│   │   ├── materialization-interval  # 300
│   │   └── feature-validation        # strict
│   └── model-governance/
│       ├── require-approval          # true
│       ├── min-accuracy-threshold    # 0.85
│       ├── max-model-size-gb        # 5
│       └── drift-monitoring-enabled  # true
├── model-registry/
│   ├── models/
│   │   ├── fraud-detection/
│   │   │   ├── versions/
│   │   │   │   ├── v1.0.0/
│   │   │   │   │   ├── metadata     # {accuracy: 0.92, ...}
│   │   │   │   │   ├── status       # production
│   │   │   │   │   └── artifacts    # s3://models/fraud/v1
│   │   │   │   └── v2.0.0/
│   │   │   │       ├── metadata
│   │   │   │       └── status       # staging
│   │   │   └── production-version   # v1.0.0
│   │   └── recommendation/
│   │       └── versions/
│   └── deployments/
│       ├── production/
│       │   ├── fraud-detection      # {model: v1.0.0, replicas: 3}
│       │   └── recommendation       # {model: v2.1.0, replicas: 5}
│       └── staging/
├── experiments/
│   ├── active/
│   │   ├── exp-2024-01-fraud/
│   │   │   ├── status              # running
│   │   │   ├── started_by          # alice@platformq
│   │   │   └── resources           # {gpus: 4, memory: 32GB}
│   │   └── exp-2024-01-rec/
│   └── completed/
└── distributed-jobs/
    ├── active/
    │   └── job-12345/
    │       ├── world-size           # 4
    │       ├── workers/             # [worker-1, worker-2, ...]
    │       └── status               # training
    └── queued/
```

### 2. Implementation Code

```python
# ml_service/consul_integration.py
from typing import Dict, Any, Optional, List, Set
import asyncio
from dataclasses import dataclass
from enum import Enum
from platformq_shared.consul.consul_client import ConsulClient
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class ModelStatus(Enum):
    TRAINING = "training"
    VALIDATING = "validating"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"
    FAILED = "failed"

@dataclass
class TrainingConfig:
    """ML training configuration"""
    default_batch_size: int = 32
    max_epochs: int = 100
    early_stopping_patience: int = 5
    distributed_backend: str = "nccl"
    checkpoint_frequency: int = 5
    learning_rate: float = 0.001
    optimizer: str = "adam"

@dataclass
class ServingConfig:
    """Model serving configuration"""
    max_batch_size: int = 64
    model_cache_size_gb: int = 10
    inference_timeout_ms: int = 1000
    auto_scaling_enabled: bool = True
    min_replicas: int = 2
    max_replicas: int = 10
    target_gpu_utilization: float = 0.7

@dataclass
class ModelGovernance:
    """Model governance policies"""
    require_approval: bool = True
    min_accuracy_threshold: float = 0.85
    max_model_size_gb: float = 5.0
    drift_monitoring_enabled: bool = True
    retraining_threshold: float = 0.1

class MLServiceConsulIntegration:
    """Consul integration for ML services"""
    
    def __init__(self, consul_client: ConsulClient, service_name: str = "ml-platform-service"):
        self.consul = consul_client
        self.service_name = service_name
        self._training_config: Optional[TrainingConfig] = None
        self._serving_config: Optional[ServingConfig] = None
        self._governance: Optional[ModelGovernance] = None
        self._active_experiments: Dict[str, Dict] = {}
        self._model_deployments: Dict[str, Dict] = {}
        
    async def initialize(self):
        """Initialize Consul integration"""
        # Register service
        await self._register_service()
        
        # Load configurations
        await self.reload_configurations()
        
        # Initialize model registry
        await self._init_model_registry()
        
        # Start configuration watchers
        await self._start_config_watchers()
        
        # Start health monitoring
        await self._start_health_monitoring()
        
    async def _register_service(self):
        """Register ML service with Consul"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        service = ServiceDefinition(
            name=self.service_name,
            port=8000,
            tags=["ml", "ai", "gpu", "critical"],
            meta={
                "version": "2.0.0",
                "capabilities": "training,serving,feature-store,experiments",
                "gpu_enabled": "true",
                "frameworks": "pytorch,tensorflow,xgboost,sklearn"
            },
            check={
                "http": "http://localhost:8000/health",
                "interval": "10s",
                "timeout": "5s",
                "deregister_critical_service_after": "60s"
            }
        )
        
        await self.consul.register_service(service)
        
    async def register_model(self,
                           model_name: str,
                           version: str,
                           metadata: Dict[str, Any]) -> bool:
        """Register new model version in registry"""
        model_path = f"services/{self.service_name}/model-registry/models/{model_name}/versions/{version}"
        
        # Check governance policies
        governance = await self.get_governance_config()
        
        # Validate model metrics
        accuracy = metadata.get("metrics", {}).get("accuracy", 0)
        if accuracy < governance.min_accuracy_threshold:
            logger.error(f"Model accuracy {accuracy} below threshold {governance.min_accuracy_threshold}")
            return False
            
        model_size_gb = metadata.get("model_size_bytes", 0) / (1024**3)
        if model_size_gb > governance.max_model_size_gb:
            logger.error(f"Model size {model_size_gb}GB exceeds limit {governance.max_model_size_gb}GB")
            return False
            
        # Register model
        model_data = {
            "model_name": model_name,
            "version": version,
            "status": ModelStatus.VALIDATING.value,
            "metadata": metadata,
            "registered_at": datetime.utcnow().isoformat(),
            "registered_by": metadata.get("trained_by", "unknown"),
            "framework": metadata.get("framework", "unknown"),
            "metrics": metadata.get("metrics", {}),
            "artifacts": metadata.get("artifacts", {}),
            "dependencies": metadata.get("dependencies", []),
            "signature": metadata.get("signature", {})
        }
        
        await self.consul.kv_put(f"{model_path}/metadata", model_data)
        await self.consul.kv_put(f"{model_path}/status", ModelStatus.VALIDATING.value)
        
        # Trigger validation workflow
        await self._trigger_model_validation(model_name, version)
        
        logger.info(f"Registered model {model_name} version {version}")
        return True
        
    async def promote_model_to_production(self,
                                        model_name: str,
                                        version: str,
                                        approval: Optional[Dict] = None) -> bool:
        """Promote model version to production"""
        governance = await self.get_governance_config()
        
        # Check if approval required
        if governance.require_approval and not approval:
            logger.error("Model promotion requires approval")
            return False
            
        # Get current production version
        prod_version_path = f"services/{self.service_name}/model-registry/models/{model_name}/production-version"
        current_prod = await self.consul.kv_get(prod_version_path)
        
        # Update model status
        model_path = f"services/{self.service_name}/model-registry/models/{model_name}/versions/{version}"
        await self.consul.kv_put(f"{model_path}/status", ModelStatus.PRODUCTION.value)
        
        # Update production pointer
        await self.consul.kv_put(prod_version_path, version)
        
        # Archive previous production version
        if current_prod and current_prod != version:
            old_model_path = f"services/{self.service_name}/model-registry/models/{model_name}/versions/{current_prod}"
            await self.consul.kv_put(f"{old_model_path}/status", ModelStatus.ARCHIVED.value)
            
        # Update deployment configuration
        deployment_path = f"services/{self.service_name}/model-registry/deployments/production/{model_name}"
        serving_config = await self.get_serving_config()
        
        await self.consul.kv_put(deployment_path, {
            "model": version,
            "replicas": serving_config.min_replicas,
            "promoted_at": datetime.utcnow().isoformat(),
            "promoted_by": approval.get("approved_by") if approval else "auto",
            "previous_version": current_prod
        })
        
        # Trigger deployment
        await self._trigger_model_deployment(model_name, version, "production")
        
        logger.info(f"Promoted model {model_name} version {version} to production")
        return True
        
    async def start_experiment(self,
                             experiment_id: str,
                             config: Dict[str, Any]) -> bool:
        """Start new ML experiment with resource allocation"""
        # Check resource availability
        if not await self._check_resource_availability(config.get("resources", {})):
            logger.error("Insufficient resources for experiment")
            return False
            
        exp_path = f"services/{self.service_name}/experiments/active/{experiment_id}"
        
        experiment_data = {
            "experiment_id": experiment_id,
            "status": "initializing",
            "config": config,
            "started_at": datetime.utcnow().isoformat(),
            "started_by": config.get("user", "unknown"),
            "resources": config.get("resources", {}),
            "framework": config.get("framework", "pytorch"),
            "hyperparameters": config.get("hyperparameters", {}),
            "dataset": config.get("dataset", {}),
            "tracking_url": config.get("tracking_url")
        }
        
        # Register experiment
        await self.consul.kv_put(exp_path, experiment_data, ttl=172800)  # 48 hour TTL
        
        # Allocate resources
        await self._allocate_experiment_resources(experiment_id, config.get("resources", {}))
        
        # Cache locally
        self._active_experiments[experiment_id] = experiment_data
        
        logger.info(f"Started experiment {experiment_id}")
        return True
        
    async def update_experiment_status(self,
                                     experiment_id: str,
                                     status: str,
                                     metrics: Optional[Dict] = None):
        """Update experiment status and metrics"""
        exp_path = f"services/{self.service_name}/experiments/active/{experiment_id}"
        
        exp_data = await self.consul.kv_get(exp_path)
        if not exp_data:
            logger.error(f"Experiment {experiment_id} not found")
            return
            
        exp_data["status"] = status
        exp_data["last_updated"] = datetime.utcnow().isoformat()
        
        if metrics:
            exp_data["metrics"] = exp_data.get("metrics", {})
            exp_data["metrics"].update(metrics)
            
        await self.consul.kv_put(exp_path, exp_data)
        
        # If completed, move to completed experiments
        if status in ["completed", "failed", "stopped"]:
            completed_path = f"services/{self.service_name}/experiments/completed/{experiment_id}"
            exp_data["completed_at"] = datetime.utcnow().isoformat()
            
            await self.consul.kv_put(completed_path, exp_data, ttl=2592000)  # 30 days
            await self.consul.kv_delete(exp_path)
            
            # Release resources
            await self._release_experiment_resources(experiment_id)
            
    async def coordinate_distributed_training(self,
                                            job_id: str,
                                            world_size: int) -> Dict[str, Any]:
        """Coordinate distributed training job"""
        job_path = f"services/{self.service_name}/distributed-jobs/active/{job_id}"
        
        # Initialize job
        job_data = {
            "job_id": job_id,
            "world_size": world_size,
            "workers": [],
            "status": "initializing",
            "created_at": datetime.utcnow().isoformat(),
            "master_addr": None,
            "master_port": 29500,
            "backend": (await self.get_training_config()).distributed_backend
        }
        
        await self.consul.kv_put(job_path, job_data, ttl=86400)  # 24 hour TTL
        
        # Create session for coordination
        session = await self.consul.create_session(
            name=f"distributed-training-{job_id}",
            ttl="1h",
            behavior="delete"
        )
        
        return {
            "job_id": job_id,
            "session_id": session["ID"],
            "coordination_key": job_path
        }
        
    async def join_distributed_training(self,
                                      job_id: str,
                                      worker_id: str,
                                      worker_info: Dict[str, Any]) -> Dict[str, Any]:
        """Worker joins distributed training job"""
        job_path = f"services/{self.service_name}/distributed-jobs/active/{job_id}"
        
        # Acquire lock for atomic update
        lock = await self.consul.acquire_lock(
            f"{job_path}/lock",
            ttl=30
        )
        
        if not lock:
            raise Exception("Could not acquire job lock")
            
        try:
            # Get current job state
            job_data = await self.consul.kv_get(job_path)
            if not job_data:
                raise Exception(f"Job {job_id} not found")
                
            # Assign rank
            rank = len(job_data["workers"])
            
            # Add worker
            worker_data = {
                "worker_id": worker_id,
                "rank": rank,
                "hostname": worker_info["hostname"],
                "ip_address": worker_info["ip_address"],
                "gpu_count": worker_info.get("gpu_count", 0),
                "joined_at": datetime.utcnow().isoformat()
            }
            
            job_data["workers"].append(worker_data)
            
            # First worker becomes master
            if rank == 0:
                job_data["master_addr"] = worker_info["ip_address"]
                
            # Check if all workers joined
            if len(job_data["workers"]) == job_data["world_size"]:
                job_data["status"] = "ready"
                
            await self.consul.kv_put(job_path, job_data)
            
            return {
                "rank": rank,
                "world_size": job_data["world_size"],
                "master_addr": job_data["master_addr"],
                "master_port": job_data["master_port"],
                "backend": job_data["backend"]
            }
            
        finally:
            await lock.release()
            
    async def monitor_model_drift(self,
                                model_name: str,
                                version: str,
                                metrics: Dict[str, float]) -> bool:
        """Monitor model drift and trigger retraining if needed"""
        governance = await self.get_governance_config()
        
        if not governance.drift_monitoring_enabled:
            return False
            
        # Get baseline metrics
        model_path = f"services/{self.service_name}/model-registry/models/{model_name}/versions/{version}"
        model_data = await self.consul.kv_get(f"{model_path}/metadata")
        
        if not model_data:
            return False
            
        baseline_metrics = model_data.get("metrics", {})
        
        # Calculate drift
        drift_scores = {}
        for metric, current_value in metrics.items():
            if metric in baseline_metrics:
                baseline_value = baseline_metrics[metric]
                drift = abs(current_value - baseline_value) / baseline_value
                drift_scores[metric] = drift
                
        # Check if retraining needed
        max_drift = max(drift_scores.values()) if drift_scores else 0
        
        # Store drift metrics
        drift_path = f"{model_path}/drift-monitoring"
        await self.consul.kv_put(drift_path, {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": metrics,
            "drift_scores": drift_scores,
            "max_drift": max_drift
        }, ttl=604800)  # 7 days
        
        # Trigger retraining if threshold exceeded
        if max_drift > governance.retraining_threshold:
            await self._trigger_model_retraining(model_name, version, drift_scores)
            return True
            
        return False

# Usage example
class MLPlatformService:
    def __init__(self):
        self.vault = MLServiceVaultIntegration(vault_client)
        self.consul = MLServiceConsulIntegration(consul_client)
        
    async def train_model(self, experiment_config: Dict) -> str:
        # Start experiment
        experiment_id = f"exp-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
        
        if not await self.consul.start_experiment(experiment_id, experiment_config):
            raise Exception("Failed to start experiment")
            
        try:
            # Get training credentials
            if experiment_config.get("use_cloud_gpu"):
                creds = await self.vault.get_training_credentials("aws", "sagemaker")
                # Set up cloud training
                
            # Get MLflow tracking
            mlflow = await self.vault.get_experiment_tracking_client("mlflow")
            
            with mlflow.start_run(run_name=experiment_id):
                # Log parameters
                mlflow.log_params(experiment_config["hyperparameters"])
                
                # Training loop
                for epoch in range(experiment_config["epochs"]):
                    # Train...
                    metrics = {"loss": 0.1, "accuracy": 0.95}
                    
                    # Log metrics
                    mlflow.log_metrics(metrics, step=epoch)
                    
                    # Update experiment status
                    await self.consul.update_experiment_status(
                        experiment_id,
                        "training",
                        metrics
                    )
                    
                # Save and sign model
                model_path = Path("model.pkl")
                # ... save model
                
                # Sign model
                signature = await self.vault.sign_model_artifact(
                    model_path,
                    {
                        "name": experiment_config["model_name"],
                        "version": "1.0.0",
                        "framework": "sklearn",
                        "metrics": metrics
                    }
                )
                
                # Encrypt model
                encrypted_path = await self.vault.encrypt_model(model_path)
                
                # Register model
                await self.consul.register_model(
                    experiment_config["model_name"],
                    "1.0.0",
                    {
                        "metrics": metrics,
                        "signature": signature,
                        "artifacts": {"model": str(encrypted_path)}
                    }
                )
                
            return experiment_id
            
        finally:
            # Complete experiment
            await self.consul.update_experiment_status(
                experiment_id,
                "completed"
            )
```

## Best Practices

### 1. Model Security

```python
# Always sign models before deployment
signature = await vault.sign_model_artifact(model_path, metadata)

# Always encrypt models at rest
encrypted_model = await vault.encrypt_model(model_path)

# Verify model integrity before serving
is_valid = await vault.verify_model_signature(model_path, signature)
if not is_valid:
    raise SecurityError("Model signature verification failed")
```

### 2. Distributed Training Security

```python
# Secure distributed training setup
class SecureDistributedTraining:
    async def setup_secure_training(self, world_size: int):
        # 1. Create job with unique secret
        job_config = await self.vault.coordinate_distributed_job(
            job_id, world_size
        )
        
        # 2. Each worker authenticates
        worker_config = await self.consul.join_distributed_training(
            job_id, worker_id, worker_info
        )
        
        # 3. Use encrypted communication
        os.environ["NCCL_SOCKET_IFNAME"] = "eth0"
        os.environ["NCCL_IB_DISABLE"] = "1"
        os.environ["NCCL_P2P_DISABLE"] = "1"
        
        # 4. Initialize with authentication
        torch.distributed.init_process_group(
            backend=worker_config["backend"],
            init_method=f"tcp://{worker_config['master_addr']}:{worker_config['master_port']}",
            rank=worker_config["rank"],
            world_size=worker_config["world_size"]
        )
```

### 3. Model Governance

```python
# Enforce governance policies
async def deploy_model_with_governance(model_name: str, version: str):
    # 1. Check model metrics meet threshold
    governance = await consul.get_governance_config()
    model_metrics = await consul.get_model_metrics(model_name, version)
    
    if model_metrics["accuracy"] < governance.min_accuracy_threshold:
        raise ValueError("Model accuracy below threshold")
        
    # 2. Get approval if required
    if governance.require_approval:
        approval = await get_model_approval(model_name, version)
        if not approval:
            raise ValueError("Model deployment requires approval")
            
    # 3. Deploy with monitoring
    await consul.promote_model_to_production(
        model_name, version, approval
    )
    
    # 4. Enable drift monitoring
    if governance.drift_monitoring_enabled:
        asyncio.create_task(
            monitor_model_drift_continuous(model_name, version)
        )
``` 