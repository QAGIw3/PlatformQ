# Vault & Consul Integration Guides Index

This directory contains comprehensive integration guides for implementing HashiCorp Vault and Consul across all PlatformQ services.

## 📚 Available Guides

### 1. [Authentication & Identity Services](./auth-service-vault-consul.md) 🔑
Complete guide for integrating authentication services with Vault and Consul, including:
- JWT key management and rotation
- OAuth provider credentials
- Session encryption
- PII data protection
- Distributed rate limiting
- Feature flag management

### 2. [Blockchain & Crypto Services](./blockchain-service-vault-consul.md) ⛓️
Comprehensive guide for blockchain services with maximum security:
- **Transit engine for key management** (private keys NEVER leave Vault)
- Transaction signing workflows
- Multi-signature coordination
- Gas wallet management
- RPC endpoint failover
- Circuit breaker patterns

### 3. [Data & Analytics Services](./data-service-vault-consul.md) 📊
Guide for data platforms and analytics services:
- Dynamic database credentials
- Column-level encryption for PII
- Cloud storage authentication
- Data quality monitoring
- ETL pipeline coordination
- Retention policy management

### 4. [Machine Learning Services](./ml-service-vault-consul.md) 🤖
Specialized guide for ML platforms:
- Model signing and encryption
- Experiment tracking integration
- Distributed training coordination
- Feature store encryption
- Model registry with governance
- Deployment orchestration

### 5. [General Purpose Services](./general-service-vault-consul.md) 🛠️
Template and patterns for any service type:
- Quick start template
- Common Vault operations
- Consul configuration patterns
- Health check implementation
- Circuit breaker patterns
- Migration checklist

## 🚀 Quick Start

### For New Services
1. Start with the [General Purpose Guide](./general-service-vault-consul.md)
2. Copy the template code
3. Customize for your service needs
4. Follow the security best practices

### For Existing Services
1. Review the specific guide for your service type
2. Use the migration checklist
3. Implement incrementally:
   - Move secrets to Vault
   - Move config to Consul
   - Add health checks
   - Enable dynamic updates

## 🔐 Key Principles

### Vault Integration
- **No hardcoded secrets** - Everything sensitive goes in Vault
- **Dynamic credentials** - Use temporary database credentials
- **Encryption at rest** - Use Transit engine for data encryption
- **Audit everything** - Enable audit logging for compliance

### Consul Integration
- **Service discovery** - Automatic service registration
- **Dynamic configuration** - Real-time config updates
- **Health monitoring** - Comprehensive health checks
- **Distributed coordination** - Leader election, distributed locks

## 📊 Common Patterns

### 1. Secret Rotation
```python
# Automatic key rotation with zero downtime
await vault_integration.rotate_api_keys()
```

### 2. Dynamic Configuration
```python
# Real-time configuration updates
config = await consul_integration.get_service_config()
if await consul_integration.get_feature_flag("new-feature"):
    enable_new_feature()
```

### 3. Circuit Breakers
```python
# Prevent cascade failures
if await consul_integration.check_circuit_breaker("external-api"):
    result = await call_external_api()
else:
    return cached_result
```

### 4. Secure Database Access
```python
# Temporary credentials, auto-revoked
async with vault_integration.get_database_connection() as conn:
    results = await conn.fetch(query)
```

## 🛡️ Security Best Practices

1. **Least Privilege**: Grant minimal required permissions
2. **Short-lived Credentials**: Use TTLs for all dynamic secrets
3. **Audit Trail**: Log all secret access and config changes
4. **Encryption**: Use Transit engine, keys never leave Vault
5. **Zero Trust**: Authenticate and authorize every request

## 📈 Monitoring & Observability

Each guide includes:
- Prometheus metrics for monitoring
- Structured logging patterns
- Health check implementations
- Alert configurations
- Troubleshooting guides

## 🔧 Tooling

### CLI Tools
```bash
# Vault CLI
vault kv get platformq/service-name/api-keys

# Consul CLI
consul kv get services/service-name/config
```

### Development Tools
- Vault UI: http://vault.local:8200
- Consul UI: http://consul.local:8500
- Local development scripts in each guide

## 📝 Contributing

When adding new integration patterns:
1. Follow the existing guide structure
2. Include working code examples
3. Add security considerations
4. Provide monitoring guidance
5. Include troubleshooting tips

## 🆘 Support

- **Slack**: #platform-security
- **Wiki**: Internal security documentation
- **Office Hours**: Wednesdays 2-3pm

## 🔄 Version Compatibility

These guides are tested with:
- Vault: 1.15.0+
- Consul: 1.16.0+
- Kubernetes: 1.26+
- Docker: 24.0+

---

*Last Updated: January 2024*
*Maintained by: Platform Security Team* 