#!/usr/bin/env python3
"""
Configure Kong OAuth2/OIDC Authentication

This script sets up OAuth2 and OIDC plugins for Kong services,
integrating with the auth-service as the identity provider.
"""

import os
import requests
import json
import logging
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KongOAuthConfig:
    """Configure OAuth2/OIDC for Kong"""
    
    def __init__(self):
        self.kong_admin_url = os.environ.get('KONG_ADMIN_URL', 'http://kong:8001')
        self.auth_service_url = os.environ.get('AUTH_SERVICE_URL', 'http://auth-service:8000')
        
        # Services that require authentication
        self.protected_services = [
            'trading-platform-service',
            'order-matching-service',
            'digital-asset-service',
            'data-platform-service',
            'analytics-service',
            'compliance-service',
            'risk-management-service',
            'governance-service',
            'workflow-service',
            'unified-ml-platform-service'
        ]
        
        # Public services (no auth required)
        self.public_services = [
            'market-data-service',  # Public market data
            'search-service',       # Public search
        ]
        
    def configure_oidc_plugin(self, service_name: str):
        """Configure OIDC plugin for a service"""
        try:
            oidc_config = {
                'name': 'oidc',
                'service': {'name': service_name},
                'config': {
                    'issuer': f"{self.auth_service_url}/oauth2",
                    'client_id': f"{service_name}-client",
                    'client_secret': self.generate_client_secret(service_name),
                    'redirect_uri': [f"http://localhost:8000/{service_name}/callback"],
                    'scope': ['openid', 'profile', 'email'],
                    'response_type': ['code'],
                    'auth_methods': ['authorization_code', 'session'],
                    'session': {
                        'secret': self.generate_session_secret(service_name),
                        'cookie_name': 'platformq_session',
                        'cookie_lifetime': 3600,
                        'cookie_secure': False,  # Set to True in production
                        'cookie_httponly': True,
                        'cookie_samesite': 'Lax'
                    },
                    'logout_path': '/logout',
                    'logout_redirect_uri': ['/'],
                    'discovery': f"{self.auth_service_url}/oauth2/.well-known/openid-configuration",
                    'introspection_endpoint': f"{self.auth_service_url}/oauth2/introspect",
                    'bearer_only': 'no',
                    'realm': 'platformq',
                    'verify_signature': True,
                    'verify_claims': True,
                    'cache_ttl': 300
                },
                'tags': [service_name, 'oidc', 'auth']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/services/{service_name}/plugins/{service_name}-oidc",
                json=oidc_config
            )
            response.raise_for_status()
            logger.info(f"Configured OIDC plugin for {service_name}")
            
        except Exception as e:
            logger.error(f"Error configuring OIDC for {service_name}: {e}")
            
    def configure_jwt_plugin(self, service_name: str):
        """Configure JWT plugin for API access"""
        try:
            jwt_config = {
                'name': 'jwt',
                'service': {'name': service_name},
                'config': {
                    'uri_param_names': ['jwt'],
                    'cookie_names': ['platformq_jwt'],
                    'header_names': ['Authorization'],
                    'claims_to_verify': ['exp', 'nbf'],
                    'maximum_expiration': 86400,  # 24 hours
                    'run_on_preflight': False,
                    'secret_is_base64': False
                },
                'tags': [service_name, 'jwt', 'auth']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/services/{service_name}/plugins/{service_name}-jwt",
                json=jwt_config
            )
            response.raise_for_status()
            logger.info(f"Configured JWT plugin for {service_name}")
            
            # Create JWT credentials for the service
            self.create_jwt_credentials(service_name)
            
        except Exception as e:
            logger.error(f"Error configuring JWT for {service_name}: {e}")
            
    def configure_oauth2_plugin(self, service_name: str):
        """Configure OAuth2 plugin for a service"""
        try:
            oauth2_config = {
                'name': 'oauth2',
                'service': {'name': service_name},
                'config': {
                    'scopes': self.get_service_scopes(service_name),
                    'mandatory_scope': True,
                    'token_expiration': 3600,  # 1 hour
                    'refresh_token_ttl': 2592000,  # 30 days
                    'enable_authorization_code': True,
                    'enable_client_credentials': True,
                    'enable_implicit_grant': False,  # Disabled for security
                    'enable_password_grant': False,   # Disabled for security
                    'hide_credentials': True,
                    'accept_http_if_already_terminated': True,
                    'anonymous': None,
                    'global_credentials': False,
                    'auth_header_name': 'Authorization',
                    'reuse_refresh_token': False,
                    'provision_key': self.generate_provision_key(service_name)
                },
                'tags': [service_name, 'oauth2', 'auth']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/services/{service_name}/plugins/{service_name}-oauth2",
                json=oauth2_config
            )
            response.raise_for_status()
            logger.info(f"Configured OAuth2 plugin for {service_name}")
            
        except Exception as e:
            logger.error(f"Error configuring OAuth2 for {service_name}: {e}")
            
    def configure_acl_plugin(self, service_name: str):
        """Configure ACL plugin for role-based access"""
        try:
            # Define ACL groups for each service
            allowed_groups = self.get_service_acl_groups(service_name)
            
            acl_config = {
                'name': 'acl',
                'service': {'name': service_name},
                'config': {
                    'allow': allowed_groups,
                    'hide_groups_header': False
                },
                'tags': [service_name, 'acl', 'auth']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/services/{service_name}/plugins/{service_name}-acl",
                json=acl_config
            )
            response.raise_for_status()
            logger.info(f"Configured ACL plugin for {service_name} with groups: {allowed_groups}")
            
        except Exception as e:
            logger.error(f"Error configuring ACL for {service_name}: {e}")
            
    def configure_request_transformer(self, service_name: str):
        """Configure request transformer to add auth headers"""
        try:
            transformer_config = {
                'name': 'request-transformer',
                'service': {'name': service_name},
                'config': {
                    'add': {
                        'headers': [
                            'X-Service-Name:$(service.name)',
                            'X-Consumer-ID:$(consumer.id)',
                            'X-Consumer-Username:$(consumer.username)',
                            'X-Authenticated-Scope:$(authenticated_scope)',
                            'X-Authenticated-Groups:$(authenticated_groups)'
                        ]
                    },
                    'remove': {
                        'headers': ['Cookie']  # Remove cookies before forwarding
                    }
                },
                'tags': [service_name, 'transformer', 'auth']
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/services/{service_name}/plugins/{service_name}-transformer",
                json=transformer_config
            )
            response.raise_for_status()
            logger.info(f"Configured request transformer for {service_name}")
            
        except Exception as e:
            logger.error(f"Error configuring request transformer for {service_name}: {e}")
            
    def create_consumers(self):
        """Create Kong consumers for different user types"""
        consumers = [
            {
                'username': 'platformq-admin',
                'custom_id': 'admin-001',
                'tags': ['admin', 'internal']
            },
            {
                'username': 'platformq-trader',
                'custom_id': 'trader-001',
                'tags': ['trader', 'user']
            },
            {
                'username': 'platformq-analyst',
                'custom_id': 'analyst-001',
                'tags': ['analyst', 'user']
            },
            {
                'username': 'platformq-api',
                'custom_id': 'api-001',
                'tags': ['api', 'service']
            }
        ]
        
        for consumer in consumers:
            try:
                response = requests.put(
                    f"{self.kong_admin_url}/consumers/{consumer['username']}",
                    json=consumer
                )
                response.raise_for_status()
                logger.info(f"Created consumer: {consumer['username']}")
                
                # Add ACL groups
                self.add_consumer_acl_groups(consumer['username'])
                
                # Create OAuth2 application
                self.create_oauth2_application(consumer['username'])
                
            except Exception as e:
                logger.error(f"Error creating consumer {consumer['username']}: {e}")
                
    def add_consumer_acl_groups(self, username: str):
        """Add ACL groups to a consumer"""
        groups_mapping = {
            'platformq-admin': ['admin', 'trader', 'analyst', 'user'],
            'platformq-trader': ['trader', 'user'],
            'platformq-analyst': ['analyst', 'user'],
            'platformq-api': ['api', 'service']
        }
        
        groups = groups_mapping.get(username, ['user'])
        
        for group in groups:
            try:
                response = requests.post(
                    f"{self.kong_admin_url}/consumers/{username}/acls",
                    json={'group': group}
                )
                response.raise_for_status()
                logger.info(f"Added ACL group '{group}' to consumer '{username}'")
            except Exception as e:
                logger.error(f"Error adding ACL group to {username}: {e}")
                
    def create_oauth2_application(self, username: str):
        """Create OAuth2 application for a consumer"""
        try:
            app_config = {
                'name': f"{username}-app",
                'client_id': f"{username}-client-id",
                'client_secret': self.generate_client_secret(username),
                'redirect_uris': [
                    'http://localhost:3000/callback',
                    'http://localhost:8000/callback'
                ],
                'tags': [username, 'oauth2-app']
            }
            
            response = requests.post(
                f"{self.kong_admin_url}/consumers/{username}/oauth2",
                json=app_config
            )
            response.raise_for_status()
            logger.info(f"Created OAuth2 application for '{username}'")
            
        except Exception as e:
            logger.error(f"Error creating OAuth2 app for {username}: {e}")
            
    def create_jwt_credentials(self, service_name: str):
        """Create JWT credentials for service-to-service auth"""
        try:
            # Create a service consumer
            consumer_data = {
                'username': f"{service_name}-service",
                'custom_id': f"service-{service_name}",
                'tags': ['service', 'jwt', service_name]
            }
            
            response = requests.put(
                f"{self.kong_admin_url}/consumers/{consumer_data['username']}",
                json=consumer_data
            )
            response.raise_for_status()
            
            # Create JWT credential
            jwt_cred = {
                'key': f"{service_name}-jwt-key",
                'secret': self.generate_jwt_secret(service_name),
                'algorithm': 'HS256'
            }
            
            response = requests.post(
                f"{self.kong_admin_url}/consumers/{consumer_data['username']}/jwt",
                json=jwt_cred
            )
            response.raise_for_status()
            logger.info(f"Created JWT credentials for {service_name}")
            
        except Exception as e:
            logger.error(f"Error creating JWT credentials for {service_name}: {e}")
            
    def get_service_scopes(self, service_name: str) -> List[str]:
        """Get OAuth2 scopes for a service"""
        scope_mapping = {
            'trading-platform-service': ['trade:read', 'trade:write', 'account:read'],
            'order-matching-service': ['order:read', 'order:write', 'order:cancel'],
            'digital-asset-service': ['asset:read', 'asset:write', 'asset:transfer'],
            'analytics-service': ['analytics:read', 'report:generate'],
            'compliance-service': ['compliance:read', 'compliance:approve'],
            'risk-management-service': ['risk:read', 'risk:override'],
            'governance-service': ['governance:read', 'governance:vote', 'governance:propose'],
            'unified-ml-platform-service': ['ml:read', 'ml:train', 'ml:predict']
        }
        
        return scope_mapping.get(service_name, ['read', 'write'])
        
    def get_service_acl_groups(self, service_name: str) -> List[str]:
        """Get ACL groups allowed for a service"""
        acl_mapping = {
            'trading-platform-service': ['trader', 'admin'],
            'order-matching-service': ['trader', 'admin', 'api'],
            'analytics-service': ['analyst', 'trader', 'admin'],
            'compliance-service': ['admin'],
            'risk-management-service': ['admin', 'risk-manager'],
            'governance-service': ['user', 'admin'],
            'unified-ml-platform-service': ['analyst', 'admin', 'api']
        }
        
        return acl_mapping.get(service_name, ['user', 'admin'])
        
    def generate_client_secret(self, identifier: str) -> str:
        """Generate a client secret (in production, use secure generation)"""
        import hashlib
        return hashlib.sha256(f"platformq-{identifier}-secret".encode()).hexdigest()[:32]
        
    def generate_session_secret(self, identifier: str) -> str:
        """Generate a session secret"""
        import hashlib
        return hashlib.sha256(f"platformq-session-{identifier}".encode()).hexdigest()[:32]
        
    def generate_provision_key(self, identifier: str) -> str:
        """Generate a provision key"""
        import hashlib
        return hashlib.sha256(f"platformq-provision-{identifier}".encode()).hexdigest()[:32]
        
    def generate_jwt_secret(self, identifier: str) -> str:
        """Generate a JWT secret"""
        import hashlib
        return hashlib.sha256(f"platformq-jwt-{identifier}".encode()).hexdigest()
        
    def configure_all(self):
        """Configure OAuth2/OIDC for all protected services"""
        logger.info("Starting OAuth2/OIDC configuration...")
        
        # Create consumers first
        self.create_consumers()
        
        # Configure auth plugins for protected services
        for service_name in self.protected_services:
            logger.info(f"Configuring authentication for {service_name}")
            
            # Configure OIDC for user authentication
            self.configure_oidc_plugin(service_name)
            
            # Configure JWT for API authentication
            self.configure_jwt_plugin(service_name)
            
            # Configure ACL for role-based access
            self.configure_acl_plugin(service_name)
            
            # Configure request transformer
            self.configure_request_transformer(service_name)
            
        logger.info("OAuth2/OIDC configuration completed")
        
        # Print credentials summary
        self.print_credentials_summary()
        
    def print_credentials_summary(self):
        """Print summary of created credentials"""
        print("\n" + "="*60)
        print("OAuth2/OIDC Configuration Summary")
        print("="*60)
        print("\nConsumers created:")
        print("- platformq-admin (full access)")
        print("- platformq-trader (trading access)")
        print("- platformq-analyst (analytics access)")
        print("- platformq-api (API access)")
        print("\nProtected services configured:")
        for service in self.protected_services:
            print(f"- {service}")
        print("\nPublic services:")
        for service in self.public_services:
            print(f"- {service}")
        print("\nNext steps:")
        print("1. Configure auth-service with OAuth2/OIDC endpoints")
        print("2. Update client applications with OAuth2 credentials")
        print("3. Test authentication flow")
        print("="*60)


if __name__ == "__main__":
    config = KongOAuthConfig()
    config.configure_all() 