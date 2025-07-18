#!/usr/bin/env python3
"""
Setup Vault Configuration for PlatformQ

This script initializes HashiCorp Vault with the necessary secrets
and configuration for PlatformQ services.
"""

import os
import sys
import json
import logging
import argparse
from typing import Dict, Any, List
import hvac
import secrets
import string

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VaultSetup:
    """Setup Vault for PlatformQ services"""
    
    def __init__(self, vault_url: str, root_token: str):
        self.client = hvac.Client(url=vault_url, token=root_token)
        if not self.client.is_authenticated():
            raise ValueError("Failed to authenticate with Vault")
        logger.info("Connected to Vault successfully")
    
    def setup_auth_methods(self):
        """Setup authentication methods"""
        # Enable AppRole auth method
        try:
            self.client.sys.enable_auth_method(
                method_type='approle',
                description='AppRole auth for services'
            )
            logger.info("Enabled AppRole auth method")
        except Exception as e:
            logger.warning(f"AppRole might already be enabled: {e}")
        
        # Enable userpass auth method for developers
        try:
            self.client.sys.enable_auth_method(
                method_type='userpass',
                description='Username/password auth for developers'
            )
            logger.info("Enabled userpass auth method")
        except Exception as e:
            logger.warning(f"Userpass might already be enabled: {e}")
    
    def setup_secret_engines(self):
        """Setup secret engines"""
        # Enable KV v2 secret engine
        try:
            self.client.sys.enable_secrets_engine(
                backend_type='kv-v2',
                path='secret',
                description='KV v2 secret storage for PlatformQ'
            )
            logger.info("Enabled KV v2 secret engine")
        except Exception as e:
            logger.warning(f"KV v2 might already be enabled: {e}")
        
        # Enable database secret engine
        try:
            self.client.sys.enable_secrets_engine(
                backend_type='database',
                path='database',
                description='Dynamic database credentials'
            )
            logger.info("Enabled database secret engine")
        except Exception as e:
            logger.warning(f"Database engine might already be enabled: {e}")
    
    def create_policies(self):
        """Create Vault policies for services"""
        # Base service policy
        base_service_policy = """
        # Read own service configuration
        path "secret/data/{{identity.entity.aliases.auth_approle_*.metadata.service_name}}/*" {
            capabilities = ["read", "list"]
        }
        
        # Read shared configuration
        path "secret/data/shared/*" {
            capabilities = ["read", "list"]
        }
        
        # Read feature flags
        path "secret/data/feature_flags/*" {
            capabilities = ["read", "list"]
        }
        
        # Create and update own service secrets
        path "secret/data/{{identity.entity.aliases.auth_approle_*.metadata.service_name}}/dynamic/*" {
            capabilities = ["create", "update", "read"]
        }
        """
        
        self.client.sys.create_or_update_policy(
            name='service-base',
            policy=base_service_policy
        )
        logger.info("Created base service policy")
        
        # Admin policy
        admin_policy = """
        # Full access to secret/
        path "secret/*" {
            capabilities = ["create", "read", "update", "delete", "list"]
        }
        
        # Manage auth methods
        path "auth/*" {
            capabilities = ["create", "read", "update", "delete", "list", "sudo"]
        }
        
        # Manage policies
        path "sys/policies/*" {
            capabilities = ["create", "read", "update", "delete", "list"]
        }
        
        # Manage secret engines
        path "sys/mounts/*" {
            capabilities = ["create", "read", "update", "delete", "list"]
        }
        """
        
        self.client.sys.create_or_update_policy(
            name='admin',
            policy=admin_policy
        )
        logger.info("Created admin policy")
    
    def setup_shared_secrets(self):
        """Setup shared secrets used by multiple services"""
        # JWT secret
        jwt_secret = secrets.token_urlsafe(64)
        self.client.secrets.kv.v2.create_or_update_secret(
            path='shared/jwt',
            secret={
                'secret_key': jwt_secret,
                'algorithm': 'HS256'
            }
        )
        logger.info("Created JWT secret")
        
        # Database connection info
        self.client.secrets.kv.v2.create_or_update_secret(
            path='shared/database',
            secret={
                'host': os.getenv('DB_HOST', 'postgres'),
                'port': int(os.getenv('DB_PORT', 5432)),
                'username': os.getenv('DB_USER', 'platformq'),
                'password': os.getenv('DB_PASSWORD', self._generate_password()),
                'database': os.getenv('DB_NAME', 'platformq')
            }
        )
        logger.info("Created database secrets")
        
        # Pulsar authentication
        self.client.secrets.kv.v2.create_or_update_secret(
            path='shared/pulsar',
            secret={
                'url': os.getenv('PULSAR_URL', 'pulsar://pulsar:6650'),
                'token': os.getenv('PULSAR_TOKEN', ''),
                'tls_enabled': False
            }
        )
        logger.info("Created Pulsar secrets")
        
        # MinIO credentials
        self.client.secrets.kv.v2.create_or_update_secret(
            path='shared/minio',
            secret={
                'endpoint': os.getenv('MINIO_ENDPOINT', 'minio:9000'),
                'access_key': os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                'secret_key': os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                'secure': False
            }
        )
        logger.info("Created MinIO secrets")
    
    def setup_service_secrets(self, services: List[str]):
        """Setup secrets for specific services"""
        for service in services:
            # Create service-specific database credentials
            db_password = self._generate_password()
            self.client.secrets.kv.v2.create_or_update_secret(
                path=f'{service}/database',
                secret={
                    'username': f'{service}_user',
                    'password': db_password,
                    'database': service.replace('-', '_')
                }
            )
            
            # Create service-specific secrets placeholder
            self.client.secrets.kv.v2.create_or_update_secret(
                path=f'{service}/secrets',
                secret={
                    'api_key': secrets.token_urlsafe(32)
                }
            )
            
            logger.info(f"Created secrets for service: {service}")
    
    def setup_app_roles(self, services: List[str]):
        """Setup AppRole authentication for services"""
        for service in services:
            # Create role
            self.client.auth.approle.create_or_update_approle(
                role_name=service,
                token_policies=['service-base'],
                token_ttl='1h',
                token_max_ttl='24h',
                secret_id_ttl='720h',  # 30 days
                bind_secret_id=True,
                metadata={'service_name': service}
            )
            
            # Get role ID
            role_id = self.client.auth.approle.read_role_id(service)['data']['role_id']
            
            # Generate secret ID
            secret_id_response = self.client.auth.approle.generate_secret_id(
                role_name=service,
                metadata={'service_name': service}
            )
            secret_id = secret_id_response['data']['secret_id']
            
            logger.info(f"Created AppRole for {service}")
            logger.info(f"  Role ID: {role_id}")
            logger.info(f"  Secret ID: {secret_id}")
            
            # Save to file for service to use
            with open(f'.vault-{service}.json', 'w') as f:
                json.dump({
                    'role_id': role_id,
                    'secret_id': secret_id
                }, f, indent=2)
    
    def setup_feature_flags(self):
        """Setup initial feature flags"""
        # Global feature flags
        self.client.secrets.kv.v2.create_or_update_secret(
            path='feature_flags/global',
            secret={
                'distributed_tracing': True,
                'advanced_analytics': False,
                'ml_predictions': False,
                'blockchain_integration': True,
                'federated_learning': False
            }
        )
        
        # Service-specific feature flags
        service_flags = {
            'auth-service': {
                'biometric_auth': False,
                'passwordless_login': False,
                'risk_based_auth': True
            },
            'digital-asset-service': {
                'nft_support': True,
                'batch_minting': False,
                'lazy_minting': True
            },
            'analytics-service': {
                'real_time_dashboards': True,
                'predictive_analytics': False,
                'custom_reports': True
            }
        }
        
        for service, flags in service_flags.items():
            self.client.secrets.kv.v2.create_or_update_secret(
                path=f'feature_flags/{service}',
                secret=flags
            )
        
        logger.info("Created feature flags")
    
    def _generate_password(self, length: int = 32) -> str:
        """Generate a secure random password"""
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return ''.join(secrets.choice(alphabet) for _ in range(length))


def main():
    """Main setup function"""
    parser = argparse.ArgumentParser(description='Setup Vault for PlatformQ')
    parser.add_argument(
        '--vault-url',
        default=os.getenv('VAULT_URL', 'http://localhost:8200'),
        help='Vault server URL'
    )
    parser.add_argument(
        '--root-token',
        default=os.getenv('VAULT_ROOT_TOKEN'),
        help='Vault root token'
    )
    parser.add_argument(
        '--services',
        nargs='+',
        default=[
            'auth-service',
            'digital-asset-service',
            'data-lake-service',
            'analytics-service',
            'workflow-service',
            'notification-service',
            'search-service',
            'graph-intelligence-service',
            'federated-learning-service',
            'simulation-service',
            'storage-proxy-service',
            'compute-marketplace',
            'dataset-marketplace'
        ],
        help='List of services to configure'
    )
    
    args = parser.parse_args()
    
    if not args.root_token:
        logger.error("Vault root token is required")
        sys.exit(1)
    
    try:
        setup = VaultSetup(args.vault_url, args.root_token)
        
        # Run setup steps
        setup.setup_auth_methods()
        setup.setup_secret_engines()
        setup.create_policies()
        setup.setup_shared_secrets()
        setup.setup_service_secrets(args.services)
        setup.setup_app_roles(args.services)
        setup.setup_feature_flags()
        
        logger.info("Vault setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 