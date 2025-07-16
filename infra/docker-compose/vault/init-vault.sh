#!/bin/sh

# This script is used to initialize and configure the Vault server.

# Give Vault time to start
sleep 5

# Set Vault address
export VAULT_ADDR='http://127.0.0.1:8200'

# Unseal Vault (only necessary if not using dev mode, but good practice)
# In a real setup, these keys would be stored securely.
vault operator unseal $(grep 'Unseal Key 1' /vault/logs/vault.log | awk '{print $NF}')
vault operator unseal $(grep 'Unseal Key 2' /vault/logs/vault.log | awk '{print $NF}')
vault operator unseal $(grep 'Unseal Key 3' /vault/logs/vault.log | awk '{print $NF}')

# Login with the root token
vault login dev-root-token

# Enable AppRole auth method
vault auth enable approle

# Create a generic policy for all services
vault policy write service-policy - <<EOF
path "secret/data/platformq/*" {
  capabilities = ["read"]
}
EOF

# Write all static secrets to Vault
vault kv put secret/platformq/minio \
    access_key="minioadmin" \
    secret_key="minioadmin"

vault kv put secret/platformq/postgres \
    password="password"

vault kv put secret/platformq/cassandra \
    password="cassandra" \
    config='{"hosts": "cassandra", "port": "9042"}'

vault kv put secret/platformq/jwt \
    s2s_secret_key="a-very-secret-key"

vault kv put secret/platformq/ethereum \
    private_key="0x0000000000000000000000000000000000000000000000000000000000000000"

vault kv put secret/platformq/polygon \
    private_key="0x0000000000000000000000000000000000000000000000000000000000000000"

vault kv put secret/platformq/credentials \
    encryption_key="a-very-secret-key"

vault kv put secret/platformq/bridge \
    private_key="0x0000000000000000000000000000000000000000000000000000000000000000"

vault kv put secret/platformq/reputation \
    operator_private_key="0x0000000000000000000000000000000000000000000000000000000000000000"

vault kv put secret/platformq/zulip \
    api_key="a-zulip-api-key"

vault kv put secret/platformq/openproject \
    api_key="an-openproject-api-key"

# Create AppRoles for each service
vault write auth/approle/role/auth-service token_policies="service-policy"
vault write auth/approle/role/digital-asset-service token_policies="service-policy"
vault write auth/approle/role/storage-proxy-service token_policies="service-policy"
# ... add other services here as needed

# Enable and configure Database Secrets Engine for PostgreSQL
vault secrets enable database

vault write database/config/postgresql \
    plugin_name=postgresql-database-plugin \
    allowed_roles="digital-asset-service" \
    connection_url="postgresql://{{username}}:{{password}}@postgres:5432/platformq?sslmode=disable" \
    username="postgres" \
    password="password"

vault write database/roles/digital-asset-service \
    db_name=postgresql \
    creation_statements="CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"{{name}}\";" \
    default_ttl="1h" \
    max_ttl="24h"

# Enable and configure Database Secrets Engine for Cassandra
vault write database/config/cassandra \
    plugin_name=cassandra-database-plugin \
    allowed_roles="auth-service" \
    hosts="cassandra" \
    username="cassandra" \
    password="cassandra"

vault write database/roles/auth-service \
    db_name=cassandra \
    creation_statements="CREATE USER '{{name}}' WITH PASSWORD '{{password}}'; GRANT ALL PERMISSIONS ON KEYSPACE auth_keyspace TO '{{name}}';" \
    default_ttl="1h" \
    max_ttl="24h"

echo "Vault initialized and configured."

# Keep the script running to prevent the container from exiting if needed
# tail -f /dev/null 