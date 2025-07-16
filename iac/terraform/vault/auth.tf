resource "vault_auth_backend" "approle" {
  type = "approle"
}

resource "vault_approle_auth_backend_role" "auth_service" {
  backend        = vault_auth_backend.approle.path
  role_name      = "auth-service"
  token_policies = [vault_policy.service_policy.name]
}

resource "vault_approle_auth_backend_role" "digital_asset_service" {
  backend        = vault_auth_backend.approle.path
  role_name      = "digital-asset-service"
  token_policies = [vault_policy.service_policy.name]
}

resource "vault_approle_auth_backend_role" "storage_proxy_service" {
  backend        = vault_auth_backend.approle.path
  role_name      = "storage-proxy-service"
  token_policies = [vault_policy.service_policy.name]
}

# ... add other services here as needed 