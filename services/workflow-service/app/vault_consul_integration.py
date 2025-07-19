"""
Vault and Consul Integration for Workflow Service

Manages:
- Airflow credentials and API keys
- Workflow signing keys for verifiable credentials
- Distributed locking for workflow coordination
- Task execution secrets
- Service-to-service authentication tokens
- Dynamic workflow configurations
"""

import os
import json
import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import hvac
import consul.aio
import base64
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import uuid

logger = logging.getLogger(__name__)


class VaultConsulIntegration:
    """Handles Vault and Consul integration for Workflow service"""
    
    def __init__(self):
        # Vault configuration
        self.vault_addr = os.environ.get('VAULT_ADDR', 'http://vault:8200')
        self.vault_token = os.environ.get('VAULT_TOKEN')
        self.service_name = os.environ.get('SERVICE_NAME', 'workflow-service')
        
        # Consul configuration
        self.consul_host = os.environ.get('CONSUL_HOST', 'consul')
        self.consul_port = int(os.environ.get('CONSUL_PORT', '8500'))
        
        # Service paths
        self.vault_workflow_path = f"secret/data/workflow/{self.service_name}"
        self.vault_transit_path = "transit"
        self.vault_pki_path = "pki"
        self.consul_kv_prefix = f"workflow/{self.service_name}"
        
        # Clients
        self.vault_client = None
        self.consul_client = None
        
        # Cached credentials and configs
        self._airflow_credentials = {}
        self._signing_keys = {}
        self._service_tokens = {}
        self._task_secrets = {}
        self._workflow_configs = {}
        self._active_locks = {}
        
        # Rotation tracking
        self._rotation_tasks = {}
        self._lease_renewal_tasks = {}
        self._lock_refresh_tasks = {}

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
            await self._initialize_airflow_credentials()
            await self._initialize_signing_keys()
            await self._initialize_service_tokens()
            await self._initialize_task_secrets()
            
            # Load workflow configurations from Consul
            await self._load_workflow_configurations()
            
            # Start rotation and maintenance tasks
            await self._start_maintenance_tasks()
            
            logger.info("Workflow service Vault and Consul integration initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Vault/Consul integration: {e}")
            raise

    async def _register_service(self):
        """Register workflow service with Consul"""
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        
        await self.consul_client.agent.service.register(
            name=self.service_name,
            service_id=service_id,
            address=os.environ.get('SERVICE_HOST', 'localhost'),
            port=int(os.environ.get('SERVICE_PORT', '8000')),
            tags=[
                "workflow",
                "orchestration",
                "airflow",
                "verifiable-credentials",
                "distributed-tasks"
            ],
            check=consul.Check.http(
                f"http://{os.environ.get('SERVICE_HOST', 'localhost')}:{os.environ.get('SERVICE_PORT', '8000')}/health",
                interval="10s",
                timeout="5s",
                deregister_critical_service_after="30s"
            )
        )

    async def _initialize_airflow_credentials(self):
        """Initialize Airflow credentials"""
        try:
            # Get or create Airflow admin credentials
            airflow_response = self.vault_client.read(f"{self.vault_workflow_path}/airflow")
            if not airflow_response:
                # Generate new credentials
                airflow_creds = {
                    'username': 'workflow_service',
                    'password': self._generate_secure_password(),
                    'api_key': self._generate_api_key(64),
                    'webserver_url': os.environ.get('AIRFLOW_URL', 'http://airflow-webserver:8080')
                }
                self.vault_client.write(f"{self.vault_workflow_path}/airflow", **airflow_creds)
                self._airflow_credentials = airflow_creds
            else:
                self._airflow_credentials = airflow_response['data']['data']
            
            # Get or create database credentials for Airflow metadata
            db_creds = await self._get_or_create_database_credentials("airflow")
            self._airflow_credentials['database'] = db_creds
            
        except Exception as e:
            logger.error(f"Failed to initialize Airflow credentials: {e}")
            raise

    async def _initialize_signing_keys(self):
        """Initialize signing keys for verifiable credentials"""
        try:
            # Create or get workflow signing key in Transit engine
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/workflow-signing")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/workflow-signing",
                    type="rsa-2048",
                    exportable=True  # Needed for VC signing
                )
            
            # Create or get task attestation key
            try:
                self.vault_client.read(f"{self.vault_transit_path}/keys/task-attestation")
            except:
                self.vault_client.write(
                    f"{self.vault_transit_path}/keys/task-attestation",
                    type="rsa-2048",
                    exportable=False
                )
            
            # Generate or get DID document for workflow service
            did_response = self.vault_client.read(f"{self.vault_workflow_path}/did-document")
            if not did_response:
                did_doc = await self._generate_did_document()
                self.vault_client.write(f"{self.vault_workflow_path}/did-document", **did_doc)
                self._signing_keys['did'] = did_doc
            else:
                self._signing_keys['did'] = did_response['data']['data']
            
            self._signing_keys['workflow'] = 'workflow-signing'
            self._signing_keys['task'] = 'task-attestation'
            
        except Exception as e:
            logger.error(f"Failed to initialize signing keys: {e}")
            raise

    async def _initialize_service_tokens(self):
        """Initialize service-to-service authentication tokens"""
        try:
            services = [
                'digital-asset-service',
                'simulation-service',
                'data-platform-service',
                'ml-platform-service',
                'verifiable-credential-service'
            ]
            
            for service in services:
                token_response = self.vault_client.read(f"{self.vault_workflow_path}/tokens/{service}")
                if not token_response:
                    # Generate new service token
                    token = {
                        'token': self._generate_api_key(64),
                        'refresh_token': self._generate_api_key(64),
                        'expires_at': (datetime.utcnow() + timedelta(days=30)).isoformat()
                    }
                    self.vault_client.write(f"{self.vault_workflow_path}/tokens/{service}", **token)
                    self._service_tokens[service] = token
                else:
                    self._service_tokens[service] = token_response['data']['data']
            
        except Exception as e:
            logger.error(f"Failed to initialize service tokens: {e}")
            raise

    async def _initialize_task_secrets(self):
        """Initialize task execution secrets"""
        try:
            # Common task environment variables
            task_env = {
                'SPARK_MASTER': os.environ.get('SPARK_MASTER', 'spark://spark-master:7077'),
                'HADOOP_CONF_DIR': '/opt/hadoop/conf',
                'PYTHONPATH': '/opt/airflow/dags:/opt/platformq/libs'
            }
            
            # Task-specific credentials
            task_creds = {
                'spark_jobs': {
                    'user': 'workflow_spark',
                    'keytab': base64.b64encode(b'spark-keytab-content').decode()
                },
                'ml_training': {
                    'mlflow_tracking_uri': 'http://mlflow-server:5000',
                    'mlflow_token': self._generate_api_key(32)
                },
                'data_processing': {
                    's3_access_key': self._generate_api_key(20),
                    's3_secret_key': self._generate_api_key(40)
                }
            }
            
            self._task_secrets = {
                'environment': task_env,
                'credentials': task_creds
            }
            
            # Store in Vault
            self.vault_client.write(
                f"{self.vault_workflow_path}/task-secrets",
                **self._task_secrets
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize task secrets: {e}")
            raise

    async def _load_workflow_configurations(self):
        """Load workflow configurations from Consul"""
        try:
            # Workflow execution configuration
            execution_config = await self._get_consul_config("execution-config", {
                'max_concurrent_workflows': 50,
                'max_concurrent_tasks': 200,
                'default_timeout': 3600,
                'retry_policy': {
                    'max_retries': 3,
                    'retry_delay': 60,
                    'exponential_backoff': True
                },
                'resource_limits': {
                    'cpu_per_task': 2,
                    'memory_per_task': '4Gi',
                    'max_task_duration': 14400  # 4 hours
                }
            })
            
            # Workflow templates
            templates_config = await self._get_consul_config("workflow-templates", {
                'asset_processing': {
                    'dag_id': 'asset_processing_template',
                    'schedule': None,
                    'default_args': {
                        'retries': 2,
                        'retry_delay': timedelta(minutes=5)
                    }
                },
                'ml_training': {
                    'dag_id': 'ml_training_template',
                    'schedule': '@daily',
                    'default_args': {
                        'retries': 1,
                        'email_on_failure': True
                    }
                },
                'data_pipeline': {
                    'dag_id': 'data_pipeline_template',
                    'schedule': '@hourly',
                    'default_args': {
                        'depends_on_past': True
                    }
                }
            })
            
            # Task routing configuration
            routing_config = await self._get_consul_config("task-routing", {
                'compute_intensive': {
                    'queue': 'gpu_queue',
                    'pool': 'gpu_pool'
                },
                'io_intensive': {
                    'queue': 'io_queue',
                    'pool': 'io_pool'
                },
                'default': {
                    'queue': 'default',
                    'pool': 'default_pool'
                }
            })
            
            self._workflow_configs = {
                'execution': execution_config,
                'templates': templates_config,
                'routing': routing_config
            }
            
        except Exception as e:
            logger.error(f"Failed to load workflow configurations: {e}")
            raise

    async def get_airflow_config(self) -> Dict[str, Any]:
        """Get Airflow configuration with credentials"""
        return {
            'base_url': self._airflow_credentials['webserver_url'],
            'username': self._airflow_credentials['username'],
            'password': self._airflow_credentials['password'],
            'api_key': self._airflow_credentials['api_key'],
            'database_uri': self._build_database_uri(self._airflow_credentials['database'])
        }

    async def get_service_token(self, service_name: str) -> Optional[str]:
        """Get authentication token for service-to-service communication"""
        if service_name in self._service_tokens:
            token_data = self._service_tokens[service_name]
            
            # Check if token is expired
            expires_at = datetime.fromisoformat(token_data['expires_at'])
            if datetime.utcnow() > expires_at:
                # Refresh token
                await self._refresh_service_token(service_name)
            
            return token_data['token']
        
        return None

    async def sign_workflow_credential(self, credential_data: Dict[str, Any]) -> str:
        """Sign a workflow verifiable credential"""
        try:
            # Serialize credential data
            data_bytes = json.dumps(credential_data, sort_keys=True).encode()
            data_b64 = base64.b64encode(data_bytes).decode()
            
            # Sign using Transit engine
            response = self.vault_client.write(
                f"{self.vault_transit_path}/sign/{self._signing_keys['workflow']}",
                input=data_b64,
                signature_algorithm="pss"
            )
            
            return response['data']['signature']
            
        except Exception as e:
            logger.error(f"Failed to sign workflow credential: {e}")
            raise

    async def verify_task_attestation(self, attestation_data: bytes, signature: str) -> bool:
        """Verify task completion attestation"""
        try:
            data_b64 = base64.b64encode(attestation_data).decode()
            
            response = self.vault_client.write(
                f"{self.vault_transit_path}/verify/{self._signing_keys['task']}",
                input=data_b64,
                signature=signature,
                signature_algorithm="pss"
            )
            
            return response['data']['valid']
            
        except Exception as e:
            logger.error(f"Failed to verify task attestation: {e}")
            return False

    async def acquire_workflow_lock(self, workflow_id: str, ttl: int = 3600) -> bool:
        """Acquire distributed lock for workflow execution"""
        try:
            lock_key = f"{self.consul_kv_prefix}/locks/workflows/{workflow_id}"
            session_id = await self._create_consul_session(ttl)
            
            # Try to acquire lock
            success = await self.consul_client.kv.put(
                lock_key,
                json.dumps({
                    'holder': self.service_name,
                    'acquired_at': datetime.utcnow().isoformat(),
                    'session_id': session_id
                }),
                acquire=session_id
            )
            
            if success:
                self._active_locks[workflow_id] = {
                    'session_id': session_id,
                    'lock_key': lock_key
                }
                
                # Start lock refresh task
                self._lock_refresh_tasks[workflow_id] = asyncio.create_task(
                    self._refresh_lock_periodically(workflow_id, session_id, ttl)
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to acquire workflow lock: {e}")
            return False

    async def release_workflow_lock(self, workflow_id: str):
        """Release distributed lock for workflow"""
        try:
            if workflow_id in self._active_locks:
                lock_data = self._active_locks[workflow_id]
                
                # Cancel refresh task
                if workflow_id in self._lock_refresh_tasks:
                    self._lock_refresh_tasks[workflow_id].cancel()
                    del self._lock_refresh_tasks[workflow_id]
                
                # Release lock
                await self.consul_client.kv.delete(
                    lock_data['lock_key'],
                    recurse=False
                )
                
                # Destroy session
                await self.consul_client.session.destroy(lock_data['session_id'])
                
                del self._active_locks[workflow_id]
                
        except Exception as e:
            logger.error(f"Failed to release workflow lock: {e}")

    async def get_task_secrets(self, task_type: str) -> Dict[str, Any]:
        """Get secrets for task execution"""
        secrets = {
            'environment': self._task_secrets['environment'].copy()
        }
        
        if task_type in self._task_secrets['credentials']:
            secrets['credentials'] = self._task_secrets['credentials'][task_type]
        
        # Add dynamic secrets based on task type
        if task_type == 'ml_training':
            # Get fresh MLflow token
            mlflow_token = await self._get_mlflow_token()
            secrets['credentials']['mlflow_token'] = mlflow_token
        
        return secrets

    async def get_workflow_config(self, config_type: str = 'execution') -> Dict[str, Any]:
        """Get workflow configuration"""
        return self._workflow_configs.get(config_type, {})

    async def update_workflow_template(self, template_name: str, template_config: Dict[str, Any]):
        """Update workflow template configuration"""
        try:
            templates = self._workflow_configs.get('templates', {})
            templates[template_name] = template_config
            
            await self.consul_client.kv.put(
                f"{self.consul_kv_prefix}/workflow-templates",
                json.dumps(templates)
            )
            
            self._workflow_configs['templates'] = templates
            
            # Fire event for template update
            await self.consul_client.event.fire(
                "workflow-template-update",
                json.dumps({
                    'template': template_name,
                    'timestamp': datetime.utcnow().isoformat()
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to update workflow template: {e}")
            raise

    async def get_workflow_metrics(self) -> Dict[str, Any]:
        """Get workflow execution metrics from Consul"""
        try:
            metrics_key = f"{self.consul_kv_prefix}/metrics"
            _, metrics_data = await self.consul_client.kv.get(metrics_key)
            
            if metrics_data:
                return json.loads(metrics_data['Value'])
            
            return {
                'active_workflows': 0,
                'completed_today': 0,
                'failed_today': 0,
                'average_duration': 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get workflow metrics: {e}")
            return {}

    async def rotate_airflow_credentials(self):
        """Rotate Airflow credentials"""
        try:
            logger.info("Rotating Airflow credentials")
            
            # Generate new password
            new_password = self._generate_secure_password()
            
            # Update in Airflow (would require Airflow API)
            # This is a placeholder - actual implementation would use Airflow API
            
            # Update in Vault
            self._airflow_credentials['password'] = new_password
            self._airflow_credentials['api_key'] = self._generate_api_key(64)
            
            self.vault_client.write(
                f"{self.vault_workflow_path}/airflow",
                **self._airflow_credentials
            )
            
            # Notify services
            await self.consul_client.event.fire(
                "airflow-credential-rotation",
                json.dumps({
                    'timestamp': datetime.utcnow().isoformat(),
                    'service': self.service_name
                })
            )
            
        except Exception as e:
            logger.error(f"Failed to rotate Airflow credentials: {e}")
            raise

    # Helper methods
    async def _get_or_create_database_credentials(self, database: str) -> Dict[str, Any]:
        """Get or create database credentials"""
        return {
            'host': f'{database}-postgres',
            'port': 5432,
            'database': database,
            'username': f'{database}_user',
            'password': self._generate_secure_password()
        }

    async def _generate_did_document(self) -> Dict[str, Any]:
        """Generate DID document for workflow service"""
        return {
            'id': f'did:platformq:workflow:{uuid.uuid4().hex}',
            'created': datetime.utcnow().isoformat(),
            'publicKey': [{
                'id': '#signing-key-1',
                'type': 'RsaVerificationKey2018',
                'controller': f'did:platformq:workflow:{uuid.uuid4().hex}',
                'publicKeyPem': 'public-key-pem'  # Would get from Transit
            }]
        }

    async def _refresh_service_token(self, service_name: str):
        """Refresh expired service token"""
        if service_name in self._service_tokens:
            token_data = self._service_tokens[service_name]
            
            # Use refresh token to get new access token
            # This is a placeholder - actual implementation would call auth service
            new_token = {
                'token': self._generate_api_key(64),
                'refresh_token': token_data['refresh_token'],
                'expires_at': (datetime.utcnow() + timedelta(days=30)).isoformat()
            }
            
            self.vault_client.write(
                f"{self.vault_workflow_path}/tokens/{service_name}",
                **new_token
            )
            
            self._service_tokens[service_name] = new_token

    async def _create_consul_session(self, ttl: int) -> str:
        """Create Consul session for distributed locking"""
        response = await self.consul_client.session.create(
            name=f"{self.service_name}-lock",
            ttl=ttl,
            behavior='delete'
        )
        return response['ID']

    async def _refresh_lock_periodically(self, workflow_id: str, session_id: str, ttl: int):
        """Periodically refresh lock to prevent expiration"""
        refresh_interval = ttl // 3  # Refresh at 1/3 of TTL
        
        while workflow_id in self._active_locks:
            try:
                await asyncio.sleep(refresh_interval)
                await self.consul_client.session.renew(session_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Failed to refresh lock for workflow {workflow_id}: {e}")
                break

    async def _get_mlflow_token(self) -> str:
        """Get fresh MLflow authentication token"""
        # This would integrate with ML platform service
        return self._generate_api_key(32)

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

    def _generate_secure_password(self, length: int = 24) -> str:
        """Generate a secure password"""
        import secrets
        import string
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return ''.join(secrets.choice(alphabet) for _ in range(length))

    def _build_database_uri(self, db_config: Dict[str, Any]) -> str:
        """Build database connection URI"""
        return f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

    async def _start_maintenance_tasks(self):
        """Start maintenance and rotation tasks"""
        # Rotate Airflow credentials every 30 days
        self._rotation_tasks['airflow'] = asyncio.create_task(
            self._rotate_periodically(
                self.rotate_airflow_credentials,
                interval=2592000  # 30 days
            )
        )
        
        # Refresh service tokens
        self._rotation_tasks['tokens'] = asyncio.create_task(
            self._refresh_tokens_periodically()
        )

    async def _rotate_periodically(self, rotation_func, interval: int):
        """Periodically rotate credentials"""
        while True:
            await asyncio.sleep(interval)
            try:
                await rotation_func()
            except Exception as e:
                logger.error(f"Rotation failed: {e}")

    async def _refresh_tokens_periodically(self):
        """Refresh service tokens before expiration"""
        while True:
            await asyncio.sleep(86400)  # Check daily
            try:
                for service, token_data in self._service_tokens.items():
                    expires_at = datetime.fromisoformat(token_data['expires_at'])
                    if expires_at - datetime.utcnow() < timedelta(days=7):
                        await self._refresh_service_token(service)
            except Exception as e:
                logger.error(f"Token refresh failed: {e}")

    async def close(self):
        """Cleanup resources"""
        # Release all active locks
        for workflow_id in list(self._active_locks.keys()):
            await self.release_workflow_lock(workflow_id)
        
        # Cancel all tasks
        for task in self._rotation_tasks.values():
            task.cancel()
        
        for task in self._lock_refresh_tasks.values():
            task.cancel()
        
        # Deregister from Consul
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        await self.consul_client.agent.service.deregister(service_id)
        
        # Close Consul client
        await self.consul_client.close() 