resource "vault_auth_backend" "kubernetes" {
  type = "kubernetes"
}

resource "vault_kubernetes_auth_backend_config" "kubernetes" {
  backend                = vault_auth_backend.kubernetes.path
  kubernetes_host        = "https://<KUBERNETES_HOST>" # This will be configured via variables
  kubernetes_ca_cert     = file("<PATH_TO_CA_CERT>")    # This will be configured via variables
  service_account_jwt    = file("<PATH_TO_SA_TOKEN>")  # This will be configured via variables
}

resource "vault_kubernetes_auth_backend_role" "auth_service_k8s" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "auth-service"
  bound_service_account_names      = ["auth-service-sa"]
  bound_service_account_namespaces = ["default"] # This should be configured via variables
  token_policies                   = [vault_policy.service_policy.name]
  ttl                              = "1h"
}

resource "vault_kubernetes_auth_backend_role" "digital_asset_service_k8s" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "digital-asset-service"
  bound_service_account_names      = ["digital-asset-service-sa"]
  bound_service_account_namespaces = ["default"] # This should be configured via variables
  token_policies                   = [vault_policy.service_policy.name]
  ttl                              = "1h"
}

resource "vault_kubernetes_auth_backend_role" "storage_proxy_service_k8s" {
  backend                          = vault_auth_backend.kubernetes.path
  role_name                        = "storage-proxy-service"
  bound_service_account_names      = ["storage-proxy-service-sa"]
  bound_service_account_namespaces = ["default"] # This should be configured via variables
  token_policies                   = [vault_policy.service_policy.name]
  ttl                              = "1h"
}

resource "vault_gcp_auth_backend_role" "developer" {
  backend   = vault_auth_backend.gcp.path
  role      = "developer"
  type      = "iam"
  policies  = [vault_policy.service_policy.name]
  bound_service_accounts = [
    "developer-sa@${var.gcp_project_id}.iam.gserviceaccount.com"
  ]
}

resource "vault_auth_backend" "gcp" {
  type = "gcp"
}

resource "vault_auth_backend" "oidc" {
  type = "oidc"
}

resource "vault_oidc_config" "oidc" {
  backend          = vault_auth_backend.oidc.path
  default_role     = "default"
  oidc_discovery_url = "http://auth-service:8000/api/v1/.well-known/openid-configuration"
  oidc_client_id     = "vault"
  oidc_client_secret = "a-very-secret-vault-client-secret"
}

resource "vault_oidc_role" "admin" {
  backend        = vault_auth_backend.oidc.path
  name           = "admin"
  user_claim     = "sub"
  groups_claim   = "groups"
  allowed_redirect_uris = [
    "http://127.0.0.1:8200/ui/vault/auth/oidc/oidc/callback",
    "http://localhost:8200/ui/vault/auth/oidc/oidc/callback",
  ]
  token_policies = [vault_policy.service_policy.name, "default"] # Add admin policy here
}

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