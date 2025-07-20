#!/usr/bin/env python3
"""
Cloudify script to create Vault secrets engine and policies for a tenant.
"""

import os
import sys
import time
import json
import logging
from typing import Dict, Any, List, Optional
from cloudify import ctx
from cloudify.state import ctx_parameters as inputs
from cloudify.exceptions import NonRecoverableError, RecoverableError
import hvac
from hvac.exceptions import InvalidRequest, InvalidPath, Forbidden

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('vault_provisioner')


class VaultProvisioner:
    """Handles Vault secrets engine and policy provisioning for tenants."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.vault_addr = config['vault_addr']
        self.vault_token = config['vault_token']
        self.tenant_id = config['tenant_id']
        self.reseller_id = config.get('reseller_id')
        self.customer_id = config.get('customer_id')
        self.secrets_path = config.get('secrets_path', f"tenant/{self.tenant_id}")
        
        # Initialize Vault client
        self.client = hvac.Client(
            url=self.vault_addr,
            token=self.vault_token,
            verify=config.get('verify_ssl', True)
        )
        
        if not self.client.is_authenticated():
            raise NonRecoverableError("Failed to authenticate with Vault")
            
    def enable_secrets_engine(self):
        """Enable KV secrets engine for the tenant."""
        try:
            # List current secret engines
            engines = self.client.sys.list_mounted_secrets_engines()
            engine_path = f"{self.secrets_path}/"
            
            if engine_path in engines['data']:
                logger.info(f"Secrets engine already enabled at {self.secrets_path}")
                return
                
            # Enable KV v2 secrets engine
            self.client.sys.enable_secrets_engine(
                backend_type='kv',
                path=self.secrets_path,
                options={
                    'version': '2'
                },
                description=f"Secrets engine for tenant {self.tenant_id}"
            )
            
            logger.info(f"Enabled KV v2 secrets engine at {self.secrets_path}")
            
            # Configure engine
            self._configure_secrets_engine()
            
            # Report usage
            self._report_usage('secrets_engine_enabled', {
                'path': self.secrets_path,
                'type': 'kv-v2'
            })
            
        except Exception as e:
            raise NonRecoverableError(f"Failed to enable secrets engine: {str(e)}")
            
    def _configure_secrets_engine(self):
        """Configure the secrets engine with tenant-specific settings."""
        try:
            # Configure KV engine settings
            config_data = {
                'cas_required': self.config.get('cas_required', False),
                'delete_version_after': self.config.get('delete_version_after', '0s'),
                'max_versions': self.config.get('max_versions', 10)
            }
            
            self.client.write(
                f"{self.secrets_path}/config",
                **config_data
            )
            
            logger.info(f"Configured secrets engine at {self.secrets_path}")
            
        except Exception as e:
            logger.error(f"Failed to configure secrets engine: {str(e)}")
            
    def create_tenant_policy(self):
        """Create Vault policy for tenant access."""
        try:
            policy_name = f"tenant-{self.tenant_id}-policy"
            
            # Check if policy exists
            existing_policies = self.client.sys.list_policies()['data']['policies']
            if policy_name in existing_policies:
                logger.info(f"Policy {policy_name} already exists")
                return
                
            # Define policy rules
            policy_rules = f"""
# Tenant {self.tenant_id} policy

# Allow full access to tenant's own secrets
path "{self.secrets_path}/*" {{
  capabilities = ["create", "read", "update", "delete", "list"]
}}

# Allow managing metadata/versions
path "{self.secrets_path}/metadata/*" {{
  capabilities = ["list", "read", "delete"]
}}

# Allow deleting versions
path "{self.secrets_path}/delete/*" {{
  capabilities = ["update"]
}}

# Allow destroying versions
path "{self.secrets_path}/destroy/*" {{
  capabilities = ["update"]
}}

# Allow undeleting versions
path "{self.secrets_path}/undelete/*" {{
  capabilities = ["update"]
}}

# Allow listing secret engine configuration
path "sys/mounts/{self.secrets_path}" {{
  capabilities = ["read"]
}}

# Allow access to tenant's PKI if enabled
path "pki-{self.tenant_id}/*" {{
  capabilities = ["create", "read", "update", "delete", "list"]
}}

# Allow access to tenant's transit encryption
path "transit-{self.tenant_id}/*" {{
  capabilities = ["create", "read", "update", "delete", "list"]
}}

# Deny access to other tenants' secrets
path "tenant/+" {{
  capabilities = ["deny"]
}}

# Allow reading own entity info
path "identity/entity/id/{{identity.entity.id}}" {{
  capabilities = ["read"]
}}

# Allow managing own tokens
path "auth/token/create" {{
  capabilities = ["create", "update"]
  allowed_parameters = {{
    "policies" = ["{policy_name}"]
    "ttl" = []
    "num_uses" = []
  }}
}}

path "auth/token/renew-self" {{
  capabilities = ["update"]
}}

path "auth/token/revoke-self" {{
  capabilities = ["update"]
}}

path "auth/token/lookup-self" {{
  capabilities = ["read"]
}}
"""
            
            # Create policy
            self.client.sys.create_or_update_policy(
                name=policy_name,
                policy=policy_rules
            )
            
            logger.info(f"Created policy: {policy_name}")
            
        except Exception as e:
            raise NonRecoverableError(f"Failed to create policy: {str(e)}")
            
    def create_app_role(self):
        """Create AppRole for tenant applications."""
        try:
            role_name = f"tenant-{self.tenant_id}-approle"
            
            # Enable AppRole auth if not already enabled
            auth_methods = self.client.sys.list_auth_methods()
            if 'approle/' not in auth_methods['data']:
                self.client.sys.enable_auth_method(
                    method_type='approle',
                    description='AppRole authentication for applications'
                )
                logger.info("Enabled AppRole auth method")
                
            # Create AppRole
            self.client.write(
                f"auth/approle/role/{role_name}",
                token_policies=[f"tenant-{self.tenant_id}-policy"],
                token_ttl=self.config.get('token_ttl', '1h'),
                token_max_ttl=self.config.get('token_max_ttl', '24h'),
                secret_id_ttl=self.config.get('secret_id_ttl', '6h'),
                token_num_uses=self.config.get('token_num_uses', 0),
                secret_id_num_uses=self.config.get('secret_id_num_uses', 0),
                bind_secret_id=self.config.get('bind_secret_id', True),
                token_bound_cidrs=self.config.get('token_bound_cidrs', []),
                secret_id_bound_cidrs=self.config.get('secret_id_bound_cidrs', [])
            )
            
            logger.info(f"Created AppRole: {role_name}")
            
            # Get role ID
            role_id_response = self.client.read(f"auth/approle/role/{role_name}/role-id")
            role_id = role_id_response['data']['role_id']
            
            # Generate initial secret ID
            secret_id_response = self.client.write(
                f"auth/approle/role/{role_name}/secret-id",
                metadata={
                    'tenant_id': self.tenant_id,
                    'created_by': 'cloudify',
                    'created_at': str(int(time.time()))
                }
            )
            secret_id = secret_id_response['data']['secret_id']
            
            # Store credentials in runtime properties
            ctx.instance.runtime_properties['approle_role_id'] = role_id
            ctx.instance.runtime_properties['approle_secret_id'] = secret_id
            
            logger.info(f"Generated AppRole credentials for {role_name}")
            
        except Exception as e:
            logger.error(f"Failed to create AppRole: {str(e)}")
            
    def create_initial_secrets(self):
        """Create initial secrets structure for the tenant."""
        try:
            # Create service credentials structure
            services = ['cassandra', 'ignite', 'pulsar', 'minio', 'elasticsearch', 'janusgraph']
            
            for service in services:
                secret_path = f"{self.secrets_path}/data/services/{service}"
                
                # Check if secret exists
                try:
                    existing = self.client.read(secret_path)
                    logger.info(f"Secret already exists at {secret_path}")
                    continue
                except (InvalidPath, InvalidRequest):
                    pass
                    
                # Create placeholder credentials
                self.client.write(
                    secret_path,
                    data={
                        'username': f"{self.tenant_id}-{service}",
                        'password': self._generate_password(),
                        'endpoint': f"{service}.platform-q.local",
                        'tenant_id': self.tenant_id,
                        'created_at': int(time.time())
                    }
                )
                
                logger.info(f"Created initial secret for {service}")
                
            # Create API keys structure
            self.client.write(
                f"{self.secrets_path}/data/api-keys/default",
                data={
                    'api_key': self._generate_api_key(),
                    'tenant_id': self.tenant_id,
                    'created_at': int(time.time()),
                    'expires_at': int(time.time()) + (365 * 24 * 60 * 60)  # 1 year
                }
            )
            
            # Create encryption keys structure
            self.client.write(
                f"{self.secrets_path}/data/encryption/master",
                data={
                    'key': self._generate_encryption_key(),
                    'algorithm': 'AES-256-GCM',
                    'created_at': int(time.time()),
                    'rotated_at': int(time.time())
                }
            )
            
            logger.info(f"Created initial secrets structure for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to create initial secrets: {str(e)}")
            
    def enable_transit_engine(self):
        """Enable transit encryption engine for the tenant."""
        try:
            transit_path = f"transit-{self.tenant_id}"
            
            # Check if already enabled
            engines = self.client.sys.list_mounted_secrets_engines()
            if f"{transit_path}/" in engines['data']:
                logger.info(f"Transit engine already enabled at {transit_path}")
                return
                
            # Enable transit engine
            self.client.sys.enable_secrets_engine(
                backend_type='transit',
                path=transit_path,
                description=f"Transit encryption for tenant {self.tenant_id}"
            )
            
            logger.info(f"Enabled transit engine at {transit_path}")
            
            # Create default encryption key
            self.client.write(
                f"{transit_path}/keys/default",
                type='aes256-gcm96',
                derived=False,
                exportable=False,
                allow_plaintext_backup=False
            )
            
            logger.info(f"Created default encryption key for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to enable transit engine: {str(e)}")
            
    def enable_pki_engine(self):
        """Enable PKI engine for the tenant if requested."""
        if not self.config.get('enable_pki', False):
            return
            
        try:
            pki_path = f"pki-{self.tenant_id}"
            
            # Check if already enabled
            engines = self.client.sys.list_mounted_secrets_engines()
            if f"{pki_path}/" in engines['data']:
                logger.info(f"PKI engine already enabled at {pki_path}")
                return
                
            # Enable PKI engine
            self.client.sys.enable_secrets_engine(
                backend_type='pki',
                path=pki_path,
                config={
                    'max_lease_ttl': self.config.get('pki_max_ttl', '87600h')  # 10 years
                },
                description=f"PKI for tenant {self.tenant_id}"
            )
            
            logger.info(f"Enabled PKI engine at {pki_path}")
            
            # Generate root CA
            self.client.write(
                f"{pki_path}/root/generate/internal",
                common_name=f"Tenant {self.tenant_id} Root CA",
                ttl=self.config.get('pki_root_ttl', '87600h'),
                key_type='rsa',
                key_bits=4096
            )
            
            # Configure URLs
            self.client.write(
                f"{pki_path}/config/urls",
                issuing_certificates=f"{self.vault_addr}/v1/{pki_path}/ca",
                crl_distribution_points=f"{self.vault_addr}/v1/{pki_path}/crl"
            )
            
            # Create a role for issuing certificates
            self.client.write(
                f"{pki_path}/roles/tenant-services",
                allowed_domains=[
                    f"*.tenant-{self.tenant_id}.platform-q.local",
                    f"tenant-{self.tenant_id}.platform-q.local"
                ],
                allow_subdomains=True,
                max_ttl='8760h'  # 1 year
            )
            
            logger.info(f"Configured PKI for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to enable PKI engine: {str(e)}")
            
    def configure_audit_logging(self):
        """Configure audit logging for tenant operations."""
        try:
            audit_path = f"file-tenant-{self.tenant_id}"
            
            # Check if audit device exists
            audit_devices = self.client.sys.list_enabled_audit_devices()
            if audit_path in audit_devices['data']:
                logger.info(f"Audit device {audit_path} already enabled")
                return
                
            # Enable file audit device
            options = {
                'file_path': f"/vault/logs/audit-tenant-{self.tenant_id}.log",
                'log_raw': False,
                'hmac_accessor': True,
                'mode': '0600',
                'format': 'json',
                'prefix': f"tenant-{self.tenant_id}"
            }
            
            self.client.sys.enable_audit_device(
                device_type='file',
                path=audit_path,
                description=f"Audit log for tenant {self.tenant_id}",
                options=options
            )
            
            logger.info(f"Enabled audit logging for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to configure audit logging: {str(e)}")
            
    def _generate_password(self) -> str:
        """Generate a secure password."""
        import secrets
        import string
        alphabet = string.ascii_letters + string.digits + string.punctuation
        return ''.join(secrets.choice(alphabet) for _ in range(32))
        
    def _generate_api_key(self) -> str:
        """Generate an API key."""
        import secrets
        return secrets.token_urlsafe(32)
        
    def _generate_encryption_key(self) -> str:
        """Generate an encryption key."""
        import secrets
        return secrets.token_hex(32)  # 256-bit key
        
    def _report_usage(self, event_type: str, details: Dict[str, Any]):
        """Report usage event to metering service."""
        try:
            # In production, this would send to OpenMeter/CloudKitty
            usage_event = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id,
                'customer_id': self.customer_id,
                'service': 'vault',
                'event_type': event_type,
                'timestamp': int(time.time()),
                'details': details
            }
            logger.info(f"Usage event: {usage_event}")
            
        except Exception as e:
            logger.error(f"Failed to report usage: {str(e)}")


def main():
    """Main execution function for Cloudify."""
    try:
        # Get configuration from Cloudify inputs
        config = {
            'vault_addr': inputs.get('vault_addr', 'http://localhost:8200'),
            'vault_token': inputs['vault_token'],
            'verify_ssl': inputs.get('verify_ssl', True),
            'tenant_id': inputs['tenant_id'],
            'reseller_id': inputs.get('reseller_id'),
            'customer_id': inputs.get('customer_id'),
            'secrets_path': inputs.get('secrets_path', f"tenant/{inputs['tenant_id']}"),
            
            # KV engine settings
            'cas_required': inputs.get('cas_required', False),
            'delete_version_after': inputs.get('delete_version_after', '0s'),
            'max_versions': inputs.get('max_versions', 10),
            
            # AppRole settings
            'token_ttl': inputs.get('token_ttl', '1h'),
            'token_max_ttl': inputs.get('token_max_ttl', '24h'),
            'secret_id_ttl': inputs.get('secret_id_ttl', '6h'),
            'token_num_uses': inputs.get('token_num_uses', 0),
            'secret_id_num_uses': inputs.get('secret_id_num_uses', 0),
            'bind_secret_id': inputs.get('bind_secret_id', True),
            'token_bound_cidrs': inputs.get('token_bound_cidrs', []),
            'secret_id_bound_cidrs': inputs.get('secret_id_bound_cidrs', []),
            
            # PKI settings
            'enable_pki': inputs.get('enable_pki', False),
            'pki_max_ttl': inputs.get('pki_max_ttl', '87600h'),
            'pki_root_ttl': inputs.get('pki_root_ttl', '87600h'),
            
            # Environment
            'region': inputs.get('region', 'default')
        }
        
        # Store config in runtime properties for other operations
        ctx.instance.runtime_properties['vault_config'] = config
        
        provisioner = VaultProvisioner(config)
        
        # Enable secrets engine
        provisioner.enable_secrets_engine()
        
        # Create tenant policy
        provisioner.create_tenant_policy()
        
        # Create AppRole
        provisioner.create_app_role()
        
        # Create initial secrets
        provisioner.create_initial_secrets()
        
        # Enable transit engine
        provisioner.enable_transit_engine()
        
        # Enable PKI if requested
        provisioner.enable_pki_engine()
        
        # Configure audit logging
        provisioner.configure_audit_logging()
        
        # Store secrets path in runtime properties
        ctx.instance.runtime_properties['secrets_path'] = config['secrets_path']
        ctx.instance.runtime_properties['vault_configured'] = True
        
        logger.info(f"Successfully provisioned Vault for tenant {config['tenant_id']}")
        
    except Exception as e:
        logger.error(f"Failed to provision Vault: {str(e)}")
        raise NonRecoverableError(str(e))


if __name__ == '__main__':
    main() 