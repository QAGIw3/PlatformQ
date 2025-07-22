"""
Vault and Consul Integration for Unified ML Platform Service

Manages:
- MLflow credentials and API keys
- Training infrastructure credentials (Spark, K8s)
- Model serving secrets
- Feature store credentials
- Model artifact encryption
- Marketplace API keys
- Federated learning certificates
"""

import os
import json
import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import hvac
import consul.aio
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import base64

logger = logging.getLogger(__name__)


class VaultConsulIntegration:
    """Handles Vault and Consul integration for ML Platform service"""
    
    def __init__(self):
        # Vault configuration
        self.vault_addr = os.environ.get('VAULT_ADDR', 'http://vault:8200')
        self.vault_token = os.environ.get('VAULT_TOKEN')
        self.service_name = os.environ.get('SERVICE_NAME', 'unified-ml-platform-service')
        
        # Consul configuration
        self.consul_host = os.environ.get('CONSUL_HOST', 'consul')
        self.consul_port = int(os.environ.get('CONSUL_PORT', '8500'))
        
        # Service paths
        self.vault_ml_path = f"secret/data/ml-platform/{self.service_name}"
        self.vault_transit_path = "transit"
        self.vault_pki_path = "pki"
        self.consul_kv_prefix = f"ml-platform/{self.service_name}"
        
        # Clients
        self.vault_client = None
        self.consul_client = None
        
        # Cached secrets and configs
        self._mlflow_credentials = {}
        self._training_credentials = {}
        self._serving_credentials = {}
        self._feature_store_config = {}
        self._encryption_keys = {}
        self._marketplace_keys = {}
        self._federated_certs = {}
        
        # Rotation tracking
        self._rotation_tasks = {}
        self._lease_renewal_tasks = {}

    async def initialize(self):
        """Initialize Vault and Consul clients"""
        try:
            # Initialize Vault client
            self.vault_client = hvac.Client(url=self.vault_addr, token=self.vault_token)
            if not self.vault_client.is_authenticated():
                raise Exception("Vault authentication failed")
            
            # Initialize Consul client
            self.consul_client = consul.aio.Consul(
                host=self.consul_host,
                port=self.consul_port
            )
            
            # Register service with Consul
            await self._register_service()
            
            # Initialize secrets and configurations
            await self._initialize_mlflow_credentials()
            await self._initialize_training_credentials()
            await self._initialize_serving_credentials()
            await self._initialize_feature_store_config()
            await self._initialize_encryption_keys()
            await self._initialize_marketplace_keys()
            await self._initialize_federated_certificates()
            
            # Load ML platform configurations from Consul
            await self._load_ml_configurations()
            
            # Start rotation tasks
            await self._start_rotation_tasks()
            
            logger.info("ML Platform Vault and Consul integration initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Vault/Consul integration: {e}")
            raise

    async def _register_service(self):
        """Register ML Platform service with Consul"""
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        
        await self.consul_client.agent.service.register(
            name=self.service_name,
            service_id=service_id,
            address=os.environ.get('SERVICE_HOST', 'localhost'),
            port=int(os.environ.get('SERVICE_PORT', '8000')),
            tags=[
                "ml-platform",
                "model-serving",
                "training",
                "federated-learning",
                "neuromorphic"
            ],
            check=consul.Check.http(
                f"http://{os.environ.get('SERVICE_HOST', 'localhost')}:{os.environ.get('SERVICE_PORT', '8000')}/health",
                interval="10s",
                timeout="5s",
                deregister_critical_service_after="30s"
            )
        )

    async def _initialize_mlflow_credentials(self):
        """Initialize MLflow tracking server credentials"""
        try:
            # Get or create MLflow database credentials
            db_creds = await self._get_or_create_database_credentials("mlflow")
            self._mlflow_credentials['database'] = db_creds
            
            # Get or create MLflow API keys
            api_response = self.vault_client.read(f"{self.vault_ml_path}/mlflow-api")
            if not api_response:
                # Generate new API keys
                api_keys = {
                    'tracking_api_key': self._generate_api_key(),
                    'artifact_api_key': self._generate_api_key(),
                    's3_access_key': self._generate_api_key(16),
                    's3_secret_key': self._generate_api_key(32)
                }
                self.vault_client.write(f"{self.vault_ml_path}/mlflow-api", **api_keys)
                self._mlflow_credentials['api'] = api_keys
            else:
                self._mlflow_credentials['api'] = api_response['data']['data']
            
            # Configure MLflow artifact encryption
            await self._setup_artifact_encryption()
            
        except Exception as e:
            logger.error(f"Failed to initialize MLflow credentials: {e}")
            raise

    async def _initialize_training_credentials(self):
        """Initialize training infrastructure credentials"""
        try:
            # Spark cluster credentials
            spark_creds = await self._get_or_create_spark_credentials()
            self._training_credentials['spark'] = spark_creds
            
            # Kubernetes service account tokens
            k8s_token = await self._get_or_create_k8s_token()
            self._training_credentials['kubernetes'] = k8s_token
            
            # GPU cluster credentials
            gpu_creds = await self._get_or_create_gpu_credentials()
            self._training_credentials['gpu'] = gpu_creds
            
            # Distributed training certificates
            training_certs = await self._get_or_create_training_certificates()
            self._training_credentials['certificates'] = training_certs
            
        except Exception as e:
            logger.error(f"Failed to initialize training credentials: {e}")
            raise

    async def _initialize_serving_credentials(self):
        """Initialize model serving credentials"""
        try:
            # Triton Inference Server credentials
            triton_creds = {
                'api_key': self._generate_api_key(),
                'model_repository_key': self._generate_api_key(32)
            }
            self._serving_credentials['triton'] = triton_creds
            
            # TorchServe credentials
            torchserve_creds = {
                'management_api_key': self._generate_api_key(),
                'inference_api_key': self._generate_api_key()
            }
            self._serving_credentials['torchserve'] = torchserve_creds
            
            # TensorFlow Serving credentials
            tfserving_creds = {
                'api_key': self._generate_api_key(),
                'monitoring_key': self._generate_api_key()
            }
            self._serving_credentials['tfserving'] = tfserving_creds
            
            # Store in Vault
            self.vault_client.write(
                f"{self.vault_ml_path}/serving-credentials",
                **self._serving_credentials
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize serving credentials: {e}")
            raise

    async def _initialize_feature_store_config(self):
        """Initialize feature store configuration and credentials"""
        try:
            # Feast configuration
            feast_config = {
                'project': 'platformq-ml',
                'provider': 'local',
                'online_store': {
                    'type': 'ignite',
                    'host': os.environ.get('IGNITE_HOST', 'ignite'),
                    'port': int(os.environ.get('IGNITE_PORT', '10800')),
                    'cache_name': 'feast_online_features'
                },
                'offline_store': {
                    'type': 'spark',
                    'spark_conf': {
                        'spark.master': os.environ.get('SPARK_MASTER', 'spark://spark-master:7077'),
                        'spark.sql.warehouse.dir': '/data/feast/warehouse'
                    }
                },
                'registry': {
                    'type': 'sql',
                    'connection_string': await self._get_feature_store_connection()
                }
            }
            
            # Store in Consul
            await self.consul_client.kv.put(
                f"{self.consul_kv_prefix}/feast-config",
                json.dumps(feast_config)
            )
            
            self._feature_store_config = feast_config
            
        except Exception as e:
            logger.error(f"Failed to initialize feature store config: {e}")
            raise

    async def _initialize_encryption_keys(self):
        """Initialize encryption keys for model artifacts"""
        try:
            # Create or get model encryption key in Transit engine
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/ml-model-artifacts")
            except:
                # Create new encryption key
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/ml-model-artifacts",
                    type="aes256-gcm96",
                    derived=True,
                    exportable=False
                )
            
            # Create or get feature encryption key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/ml-features")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/ml-features",
                    type="aes256-gcm96",
                    derived=True,
                    exportable=False
                )
            
            # Create or get prediction encryption key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/ml-predictions")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/ml-predictions",
                    type="aes256-gcm96",
                    derived=True,
                    exportable=False
                )
            
            self._encryption_keys = {
                'model_artifacts': 'ml-model-artifacts',
                'features': 'ml-features',
                'predictions': 'ml-predictions'
            }
            
        except Exception as e:
            logger.error(f"Failed to initialize encryption keys: {e}")
            raise

    async def _initialize_marketplace_keys(self):
        """Initialize marketplace integration keys"""
        try:
            # Model licensing keys
            licensing_keys = {
                'api_key': self._generate_api_key(32),
                'signing_key': self._generate_api_key(64),
                'blockchain_wallet': await self._get_marketplace_wallet()
            }
            
            # Store in Vault
            self.vault_client.write(
                f"{self.vault_ml_path}/marketplace-keys",
                **licensing_keys
            )
            
            self._marketplace_keys = licensing_keys
            
        except Exception as e:
            logger.error(f"Failed to initialize marketplace keys: {e}")
            raise

    async def _initialize_federated_certificates(self):
        """Initialize federated learning certificates"""
        try:
            # Generate or get federated learning CA certificate
            ca_cert = await self._get_or_create_federated_ca()
            
            # Generate client certificates for federated nodes
            client_certs = await self._generate_federated_client_certs()
            
            self._federated_certs = {
                'ca_certificate': ca_cert,
                'client_certificates': client_certs,
                'aggregation_key': self._generate_api_key(64)
            }
            
            # Store in Vault
            self.vault_client.write(
                f"{self.vault_ml_path}/federated-certs",
                **self._federated_certs
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize federated certificates: {e}")
            raise

    async def _load_ml_configurations(self):
        """Load ML platform configurations from Consul"""
        try:
            # Training configurations
            training_config = await self._get_consul_config("training-config", {
                'max_concurrent_jobs': 10,
                'default_timeout': 3600,
                'resource_limits': {
                    'cpu': '4',
                    'memory': '16Gi',
                    'gpu': '1'
                },
                'hyperparameter_tuning': {
                    'strategy': 'bayesian',
                    'max_trials': 50,
                    'parallel_trials': 5
                }
            })
            
            # Model serving configurations
            serving_config = await self._get_consul_config("serving-config", {
                'default_replicas': 2,
                'auto_scaling': {
                    'enabled': True,
                    'min_replicas': 1,
                    'max_replicas': 10,
                    'target_cpu': 70
                },
                'canary_deployment': {
                    'enabled': True,
                    'initial_traffic': 10,
                    'increment': 10,
                    'threshold': 95
                }
            })
            
            # Monitoring configurations
            monitoring_config = await self._get_consul_config("monitoring-config", {
                'drift_detection': {
                    'enabled': True,
                    'threshold': 0.15,
                    'window_size': 1000
                },
                'performance_tracking': {
                    'latency_p99_threshold': 100,
                    'error_rate_threshold': 0.01
                },
                'alert_channels': ['email', 'slack', 'pagerduty']
            })
            
            # Neuromorphic computing configurations
            neuromorphic_config = await self._get_consul_config("neuromorphic-config", {
                'spike_threshold': 1.0,
                'refractory_period': 2.0,
                'learning_rate': 0.01,
                'max_neurons': 1000000,
                'event_buffer_size': 10000
            })
            
        except Exception as e:
            logger.error(f"Failed to load ML configurations: {e}")
            raise

    async def get_mlflow_config(self) -> Dict[str, Any]:
        """Get MLflow configuration with credentials"""
        return {
            'tracking_uri': f"http://mlflow-server:5000",
            'artifact_uri': f"s3://mlflow-artifacts",
            'database_uri': self._build_database_uri(self._mlflow_credentials['database']),
            'api_keys': self._mlflow_credentials['api'],
            'encryption_enabled': True,
            'encryption_key': self._encryption_keys['model_artifacts']
        }

    async def get_training_config(self) -> Dict[str, Any]:
        """Get training infrastructure configuration"""
        config = await self._get_consul_config("training-config")
        config['credentials'] = self._training_credentials
        return config

    async def get_serving_config(self) -> Dict[str, Any]:
        """Get model serving configuration"""
        config = await self._get_consul_config("serving-config")
        config['credentials'] = self._serving_credentials
        return config

    async def get_feature_store_config(self) -> Dict[str, Any]:
        """Get feature store configuration"""
        return self._feature_store_config

    async def encrypt_model_artifact(self, data: bytes, context: str = "") -> str:
        """Encrypt model artifact using Transit engine"""
        try:
            # Base64 encode the data
            encoded_data = base64.b64encode(data).decode('utf-8')
            
            # Encrypt using Transit engine
            response = self.vault_client.write(
                f"{self.vault_transit_path}/encrypt/{self._encryption_keys['model_artifacts']}",
                plaintext=encoded_data,
                context=base64.b64encode(context.encode()).decode('utf-8') if context else None
            )
            
            return response['data']['ciphertext']
            
        except Exception as e:
            logger.error(f"Failed to encrypt model artifact: {e}")
            raise

    async def decrypt_model_artifact(self, ciphertext: str, context: str = "") -> bytes:
        """Decrypt model artifact using Transit engine"""
        try:
            # Decrypt using Transit engine
            response = self.vault_client.write(
                f"{self.vault_transit_path}/decrypt/{self._encryption_keys['model_artifacts']}",
                ciphertext=ciphertext,
                context=base64.b64encode(context.encode()).decode('utf-8') if context else None
            )
            
            # Base64 decode the result
            plaintext = base64.b64decode(response['data']['plaintext'])
            return plaintext
            
        except Exception as e:
            logger.error(f"Failed to decrypt model artifact: {e}")
            raise

    async def encrypt_features(self, features: Dict[str, Any]) -> str:
        """Encrypt feature data"""
        try:
            # Serialize features
            feature_bytes = json.dumps(features).encode()
            encoded_data = base64.b64encode(feature_bytes).decode('utf-8')
            
            # Encrypt
            response = self.vault_client.write(
                f"{self.vault_transit_path}/encrypt/{self._encryption_keys['features']}",
                plaintext=encoded_data
            )
            
            return response['data']['ciphertext']
            
        except Exception as e:
            logger.error(f"Failed to encrypt features: {e}")
            raise

    async def sign_model_metadata(self, metadata: Dict[str, Any]) -> str:
        """Sign model metadata for marketplace"""
        try:
            # Serialize metadata
            metadata_bytes = json.dumps(metadata, sort_keys=True).encode()
            
            # Use marketplace signing key
            signature = self._sign_data(
                metadata_bytes,
                self._marketplace_keys['signing_key']
            )
            
            return base64.b64encode(signature).decode('utf-8')
            
        except Exception as e:
            logger.error(f"Failed to sign model metadata: {e}")
            raise

    async def get_federated_node_certificate(self, node_id: str) -> Dict[str, str]:
        """Get certificate for federated learning node"""
        if node_id in self._federated_certs['client_certificates']:
            return self._federated_certs['client_certificates'][node_id]
        
        # Generate new certificate for node
        cert = await self._generate_node_certificate(node_id)
        self._federated_certs['client_certificates'][node_id] = cert
        
        # Update in Vault
        self.vault_client.write(
            f"{self.vault_ml_path}/federated-certs",
            **self._federated_certs
        )
        
        return cert

    async def validate_federated_aggregation(self, aggregation_data: bytes, signature: str) -> bool:
        """Validate federated learning aggregation signature"""
        try:
            expected_signature = self._sign_data(
                aggregation_data,
                self._federated_certs['aggregation_key']
            )
            
            return base64.b64encode(expected_signature).decode('utf-8') == signature
            
        except Exception as e:
            logger.error(f"Failed to validate aggregation: {e}")
            return False

    async def get_model_deployment_config(self, model_name: str) -> Dict[str, Any]:
        """Get deployment configuration for a specific model"""
        try:
            # Check Consul for model-specific config
            config_key = f"{self.consul_kv_prefix}/deployments/{model_name}"
            _, config_data = await self.consul_client.kv.get(config_key)
            
            if config_data:
                return json.loads(config_data['Value'])
            
            # Return default config
            return {
                'replicas': 2,
                'resources': {
                    'cpu': '1',
                    'memory': '2Gi'
                },
                'auto_scaling': True,
                'canary_enabled': False
            }
            
        except Exception as e:
            logger.error(f"Failed to get deployment config: {e}")
            return {}

    async def rotate_serving_credentials(self):
        """Rotate model serving API keys"""
        try:
            logger.info("Rotating model serving credentials")
            
            # Generate new credentials
            new_creds = {
                'triton': {
                    'api_key': self._generate_api_key(),
                    'model_repository_key': self._generate_api_key(32)
                },
                'torchserve': {
                    'management_api_key': self._generate_api_key(),
                    'inference_api_key': self._generate_api_key()
                },
                'tfserving': {
                    'api_key': self._generate_api_key(),
                    'monitoring_key': self._generate_api_key()
                }
            }
            
            # Store new credentials
            self.vault_client.write(
                f"{self.vault_ml_path}/serving-credentials",
                **new_creds
            )
            
            # Update local cache
            self._serving_credentials = new_creds
            
            # Notify serving engines via Consul event
            await self.consul_client.event.fire(
                "ml-serving-credential-rotation",
                json.dumps({
                    'timestamp': datetime.utcnow().isoformat(),
                    'service': self.service_name
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to rotate serving credentials: {e}")
            raise

    # Helper methods
    async def _get_or_create_database_credentials(self, database: str) -> Dict[str, Any]:
        """Get or create database credentials"""
        # Implementation for dynamic database credentials
        return {
            'username': f"ml_{database}_user",
            'password': self._generate_api_key(32),
            'host': f"{database}-db",
            'port': 5432,
            'database': database
        }

    async def _get_or_create_spark_credentials(self) -> Dict[str, Any]:
        """Get or create Spark cluster credentials"""
        return {
            'master_url': os.environ.get('SPARK_MASTER', 'spark://spark-master:7077'),
            'app_name': 'platformq-ml-training',
            'executor_memory': '4g',
            'executor_cores': '2'
        }

    async def _get_or_create_k8s_token(self) -> Dict[str, Any]:
        """Get or create Kubernetes service account token"""
        return {
            'token': self._generate_api_key(64),
            'namespace': 'ml-training',
            'service_account': 'ml-platform-sa'
        }

    async def _get_or_create_gpu_credentials(self) -> Dict[str, Any]:
        """Get or create GPU cluster credentials"""
        return {
            'cluster_endpoint': os.environ.get('GPU_CLUSTER', 'gpu-cluster:8080'),
            'api_key': self._generate_api_key(32),
            'resource_pool': 'ml-training'
        }

    async def _get_or_create_training_certificates(self) -> Dict[str, Any]:
        """Get or create distributed training certificates"""
        # Generate certificates for secure distributed training
        return {
            'ca_cert': 'ca-certificate-pem',
            'client_cert': 'client-certificate-pem',
            'client_key': 'client-key-pem'
        }

    async def _setup_artifact_encryption(self):
        """Setup encryption for MLflow artifacts"""
        # Configure S3-compatible storage with encryption
        pass

    async def _get_feature_store_connection(self) -> str:
        """Get feature store database connection string"""
        creds = await self._get_or_create_database_credentials("feast")
        return f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"

    async def _get_marketplace_wallet(self) -> str:
        """Get or create marketplace blockchain wallet"""
        # This would integrate with blockchain gateway service
        return "0x" + self._generate_api_key(40)

    async def _get_or_create_federated_ca(self) -> str:
        """Get or create federated learning CA certificate"""
        # Generate CA certificate for federated learning
        return "federated-ca-certificate-pem"

    async def _generate_federated_client_certs(self) -> Dict[str, Dict[str, str]]:
        """Generate initial federated client certificates"""
        return {}

    async def _generate_node_certificate(self, node_id: str) -> Dict[str, str]:
        """Generate certificate for a federated node"""
        return {
            'certificate': f'node-{node_id}-cert-pem',
            'private_key': f'node-{node_id}-key-pem',
            'issued_at': datetime.utcnow().isoformat()
        }

    async def _get_consul_config(self, key: str, default: Any = None) -> Any:
        """Get configuration from Consul KV"""
        try:
            _, data = await self.consul_client.kv.get(f"{self.consul_kv_prefix}/{key}")
            if data:
                return json.loads(data['Value'])
            elif default is not None:
                # Store default in Consul
                await self.consul_client.kv.put(
                    f"{self.consul_kv_prefix}/{key}",
                    json.dumps(default)
                )
                return default
            return {}
        except Exception as e:
            logger.error(f"Failed to get Consul config {key}: {e}")
            return default or {}

    def _generate_api_key(self, length: int = 32) -> str:
        """Generate a secure API key"""
        import secrets
        return secrets.token_urlsafe(length)

    def _sign_data(self, data: bytes, key: str) -> bytes:
        """Sign data using HMAC"""
        import hmac
        import hashlib
        return hmac.new(key.encode(), data, hashlib.sha256).digest()

    def _build_database_uri(self, creds: Dict[str, Any]) -> str:
        """Build database connection URI"""
        return f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"

    async def _start_rotation_tasks(self):
        """Start credential rotation tasks"""
        # Rotate serving credentials every 24 hours
        self._rotation_tasks['serving'] = asyncio.create_task(
            self._rotate_periodically(
                self.rotate_serving_credentials,
                interval=86400  # 24 hours
            )
        )

    async def _rotate_periodically(self, rotation_func, interval: int):
        """Periodically rotate credentials"""
        while True:
            await asyncio.sleep(interval)
            try:
                await rotation_func()
            except Exception as e:
                logger.error(f"Rotation failed: {e}")

    async def close(self):
        """Cleanup resources"""
        # Cancel rotation tasks
        for task in self._rotation_tasks.values():
            task.cancel()
        
        # Deregister from Consul
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        await self.consul_client.agent.service.deregister(service_id)
        
        # Close Consul client
        await self.consul_client.close() 