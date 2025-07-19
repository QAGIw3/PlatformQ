#!/bin/sh

# Wait for Vault to be ready
echo "Waiting for Vault to be ready..."
while ! vault status 2>/dev/null; do
    sleep 2
done

# Check if Vault is initialized
if vault status | grep -q "Initialized.*false"; then
    echo "Vault not initialized. Please run vault operator init first."
    exit 1
fi

# Check if Vault is sealed
if vault status | grep -q "Sealed.*true"; then
    echo "Vault is sealed. Please unseal it first."
    exit 1
fi

echo "Configuring Vault for PlatformQ..."

# Enable required secret engines
echo "Enabling secret engines..."

# KV v2 for general secrets
vault secrets enable -path=secret kv-v2 || echo "KV v2 already enabled"

# Transit for encryption/signing
vault secrets enable -path=transit transit || echo "Transit already enabled"

# PKI for certificate management
vault secrets enable -path=pki pki || echo "PKI already enabled"

# Database for dynamic credentials
vault secrets enable -path=database database || echo "Database already enabled"

# Create policies
echo "Creating policies..."

# Blockchain signing policy
vault policy write blockchain-signing - <<EOF
# Read blockchain keys metadata
path "secret/data/blockchain/keys/*" {
  capabilities = ["read", "list"]
}

# Use transit keys for signing
path "transit/sign/*" {
  capabilities = ["create", "update"]
}

# Verify signatures
path "transit/verify/*" {
  capabilities = ["create", "update"]
}

# Audit log access
path "secret/data/audit/signing/*" {
  capabilities = ["create", "read"]
}
EOF

# Service authentication policy
vault policy write service-auth - <<EOF
# Read service credentials
path "secret/data/services/*" {
  capabilities = ["read"]
}

# Renew tokens
path "auth/token/renew-self" {
  capabilities = ["update"]
}

# Lookup token info
path "auth/token/lookup-self" {
  capabilities = ["read"]
}
EOF

# Database credentials policy
vault policy write database-creds - <<EOF
# Generate database credentials
path "database/creds/*" {
  capabilities = ["read"]
}

# Revoke database credentials
path "sys/leases/revoke" {
  capabilities = ["update"]
}
EOF

# Admin policy
vault policy write platformq-admin - <<EOF
# Full access to all PlatformQ paths
path "secret/data/platformq/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "transit/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "pki/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

path "database/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

# Manage policies
path "sys/policies/*" {
  capabilities = ["create", "read", "update", "delete", "list"]
}

# Manage auth methods
path "sys/auth/*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}

path "auth/*" {
  capabilities = ["create", "read", "update", "delete", "list", "sudo"]
}
EOF

# Enable AppRole auth method
echo "Enabling AppRole auth..."
vault auth enable approle || echo "AppRole already enabled"

# Create roles for services
echo "Creating service roles..."

# Blockchain Gateway Service role
vault write auth/approle/role/blockchain-gateway \
    token_policies="blockchain-signing,service-auth" \
    token_ttl=1h \
    token_max_ttl=4h \
    secret_id_ttl=0 \
    bind_secret_id=true

# Data Platform Service role
vault write auth/approle/role/data-platform \
    token_policies="service-auth,database-creds" \
    token_ttl=1h \
    token_max_ttl=4h \
    secret_id_ttl=0 \
    bind_secret_id=true

# Workflow Service role
vault write auth/approle/role/workflow-service \
    token_policies="service-auth" \
    token_ttl=1h \
    token_max_ttl=4h \
    secret_id_ttl=0 \
    bind_secret_id=true

# Configure database connections
echo "Configuring database connections..."

# PostgreSQL configuration
vault write database/config/postgresql \
    plugin_name=postgresql-database-plugin \
    allowed_roles="readonly,readwrite" \
    connection_url="postgresql://{{username}}:{{password}}@postgres:5432/platformq?sslmode=disable" \
    username="vault_admin" \
    password="vault_admin_password"

# Create database roles
vault write database/roles/readonly \
    db_name=postgresql \
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; \
                        GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"{{name}}\";" \
    default_ttl="1h" \
    max_ttl="24h"

vault write database/roles/readwrite \
    db_name=postgresql \
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; \
                        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"{{name}}\";" \
    default_ttl="1h" \
    max_ttl="24h"

# Configure PKI
echo "Configuring PKI..."

# Set PKI URLs
vault write pki/config/urls \
    issuing_certificates="http://vault:8200/v1/pki/ca" \
    crl_distribution_points="http://vault:8200/v1/pki/crl"

# Generate root CA
vault write -field=certificate pki/root/generate/internal \
    common_name="PlatformQ Root CA" \
    ttl=87600h > /tmp/root_ca.crt

# Create PKI roles
vault write pki/roles/platformq-services \
    allowed_domains="platformq.local,svc.cluster.local" \
    allow_subdomains=true \
    max_ttl=72h

# Store initial service credentials
echo "Storing initial service credentials..."

# Store service API keys
vault kv put secret/services/api-keys \
    blockchain-gateway="$(openssl rand -hex 32)" \
    data-platform="$(openssl rand -hex 32)" \
    workflow-service="$(openssl rand -hex 32)"

# Store encryption keys
vault kv put secret/platformq/encryption \
    master-key="$(openssl rand -hex 32)" \
    data-encryption-key="$(openssl rand -hex 32)"

# Store Consul encryption key
CONSUL_ENCRYPT_KEY=$(consul keygen)
vault kv put secret/consul/encryption \
    gossip-key="$CONSUL_ENCRYPT_KEY"

echo "Vault initialization complete!"
echo ""
echo "Next steps:"
echo "1. Save the root token securely"
echo "2. Generate AppRole credentials for each service:"
echo "   vault read auth/approle/role/blockchain-gateway/role-id"
echo "   vault write -f auth/approle/role/blockchain-gateway/secret-id"
echo "3. Update service configurations with AppRole credentials"
echo "4. Update Consul configuration with encryption key from Vault" 