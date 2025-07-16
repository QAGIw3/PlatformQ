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

# Create AppRoles for each service
vault write auth/approle/role/auth-service token_policies="service-policy"
vault write auth/approle/role/digital-asset-service token_policies="service-policy"
vault write auth/approle/role/storage-proxy-service token_policies="service-policy"
# ... add other services here as needed

echo "Vault initialized and configured."

# Keep the script running to prevent the container from exiting if needed
# tail -f /dev/null 